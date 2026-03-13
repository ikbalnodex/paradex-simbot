"""
paradex_executor.py — Paradex Live Trading Executor
Uses paradex-py library for proper StarkNet L2 authentication.
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from paradex_py import Paradex, ParadexSubkey
    from paradex_py.environment import Environment
    PARADEX_PY_AVAILABLE = True
except ImportError as e:
    PARADEX_PY_AVAILABLE = False
    logger.error(f"paradex-py not installed: {e}")


def _to_int(key: str) -> int:
    """Convert hex string ke integer."""
    key = key.strip()
    if key.startswith(("0x", "0X")):
        return int(key, 16)
    try:
        return int(key, 16)
    except ValueError:
        return int(key)


class ParadexExecutor:
    """
    Mode L2 (Recommended — pakai Paradex Private Key dari UI):
        ParadexExecutor(l2_private_key="0x...", l2_address="0x...")

    Mode L1 (pakai Ethereum private key):
        ParadexExecutor(l1_private_key="0x...", l1_address="0x...")
    """

    def __init__(
        self,
        l1_private_key: str = None,
        l1_address:     str = None,
        l2_private_key: str = None,
        l2_address:     str = None,
    ):
        self.account_address = l1_address or l2_address or ""
        self._ready          = False
        self._positions      = {}
        self._pdx            = None

        if not PARADEX_PY_AVAILABLE:
            logger.error("paradex-py tidak tersedia")
            return

        try:
            if l2_private_key is not None:
                # ── Mode L2 ──────────────────────────────────────────────
                # ParadexSubkey sudah auto-init di __init__, JANGAN panggil
                # init_account() lagi — itu yang menyebabkan error "PROD"
                self._pdx = ParadexSubkey(
                    env=Environment.PROD,
                    l2_private_key=l2_private_key.strip(),
                    l2_address=l2_address.strip(),
                )
                self.account_address = l2_address
            else:
                # ── Mode L1 ──────────────────────────────────────────────
                # l1_private_key HARUS integer, bukan string
                self._pdx = Paradex(
                    env=Environment.PROD,
                    l1_address=l1_address.strip(),
                    l1_private_key=_to_int(l1_private_key),
                )
                self.account_address = l1_address

            # Test koneksi
            bal = self.get_balance()
            if bal:
                self._ready = True
                logger.info(f"✅ Paradex connected: {self.account_address[:12]}...")
            else:
                logger.warning("Paradex connected tapi get_balance gagal")
                self._ready = True  # tetap ready, mungkin balance 0

        except Exception as e:
            logger.warning(f"Paradex init failed: {e}")
            self._ready = False

    # ─────────────────────────────────────────────────────────────
    # Async runner
    # ─────────────────────────────────────────────────────────────

    def _run(self, coro):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor() as pool:
                    future = pool.submit(asyncio.run, coro)
                    return future.result()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        except RuntimeError:
            return asyncio.run(coro)

    # ─────────────────────────────────────────────────────────────
    # Public
    # ─────────────────────────────────────────────────────────────

    def is_ready(self) -> bool:
        return self._ready and self._pdx is not None

    # ─────────────────────────────────────────────────────────────
    # Balance
    # ─────────────────────────────────────────────────────────────

    def get_balance(self) -> dict:
        if self._pdx is None:
            return {}
        try:
            data = self._run(self._pdx.api_client.fetch_account_summary())
            if not data:
                return {}
            if hasattr(data, "__dict__"):
                data = vars(data)
            if isinstance(data, list) and data:
                data = data[0]
            return {
                "free_collateral":  float(data.get("free_collateral",  data.get("available_margin",  0))),
                "total_collateral": float(data.get("total_collateral", data.get("initial_margin",    0))),
                "equity":           float(data.get("equity",           data.get("account_value",     0))),
                "unrealized_pnl":   float(data.get("unrealized_pnl",  0)),
            }
        except Exception as e:
            logger.warning(f"get_balance error: {e}")
            return {}

    # ─────────────────────────────────────────────────────────────
    # Positions
    # ─────────────────────────────────────────────────────────────

    def sync_all(self):
        if not self.is_ready():
            return
        try:
            data = self._run(self._pdx.api_client.fetch_positions())
            if not data:
                self._positions = {}
                return
            results = data if isinstance(data, list) else data.get("results", [])
            self._positions = {}
            for pos in results:
                if hasattr(pos, "__dict__"):
                    pos = vars(pos)
                market = pos.get("market", "")
                size   = float(pos.get("size", 0))
                if size == 0:
                    continue
                self._positions[market] = {
                    "market":         market,
                    "side":           "LONG" if size > 0 else "SHORT",
                    "size":           size,
                    "avg_entry":      float(pos.get("average_entry_price", pos.get("avg_entry_price", 0))),
                    "unrealized_pnl": float(pos.get("unrealized_pnl", 0)),
                    "leverage":       float(pos.get("leverage", 1)),
                    "liq_price":      float(pos.get("liquidation_price", 0)) or None,
                }
            logger.info(f"Positions synced: {list(self._positions.keys())}")
        except Exception as e:
            logger.warning(f"sync_all error: {e}")

    def get_live_position(self, market: str) -> Optional[dict]:
        return self._positions.get(market)

    # ─────────────────────────────────────────────────────────────
    # Orders
    # ─────────────────────────────────────────────────────────────

    def place_order(
        self,
        market:      str,
        side:        str,
        size:        float,
        order_type:  str   = "MARKET",
        price:       float = None,
        reduce_only: bool  = False,
    ) -> Optional[dict]:
        if not self.is_ready():
            logger.warning("Paradex not ready — cannot place order")
            return None
        try:
            logger.info(f"Placing order: {side} {size} {market} @ {order_type}")
            result = self._run(self._pdx.api_client.submit_order(
                market=market,
                order_side=side.upper(),
                order_type=order_type.upper(),
                size=str(size),
                limit_price=str(price) if price else None,
                reduce_only=reduce_only,
            ))
            if result:
                if hasattr(result, "__dict__"):
                    result = vars(result)
                order_id = result.get("id", result.get("order_id", "?"))
                logger.info(f"Order placed: {order_id}")
                return result
            return None
        except Exception as e:
            logger.warning(f"place_order error: {e}")
            return None

    def cancel_all_orders(self, market: str = None) -> bool:
        if not self.is_ready():
            return False
        try:
            self._run(self._pdx.api_client.cancel_all_orders(market=market or ""))
            return True
        except Exception as e:
            logger.warning(f"cancel_all_orders error: {e}")
            return False

    def get_fills(self, market: str = None, limit: int = 10) -> list:
        if not self.is_ready():
            return []
        try:
            data = self._run(self._pdx.api_client.fetch_fills(
                market=market or "", page_size=limit
            ))
            if not data:
                return []
            results = data if isinstance(data, list) else data.get("results", [])
            out = []
            for f in results:
                if hasattr(f, "__dict__"):
                    f = vars(f)
                out.append(f)
            return out
        except Exception as e:
            logger.warning(f"get_fills error: {e}")
            return []

    def close_position(
        self,
        market:     str,
        order_type: str   = "MARKET",
        price:      float = None,
    ) -> Optional[dict]:
        pos = self.get_live_position(market)
        if not pos:
            self.sync_all()
            pos = self.get_live_position(market)
        if not pos:
            logger.info(f"No position to close for {market}")
            return None
        size       = abs(pos["size"])
        close_side = "SELL" if pos["side"] == "LONG" else "BUY"
        return self.place_order(
            market=market,
            side=close_side,
            size=size,
            order_type=order_type,
            price=price,
            reduce_only=True,
        )

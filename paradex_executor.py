"""
paradex_executor.py — Paradex Live Trading Executor
Uses paradex-py with L1 Ethereum key (most stable/documented approach).
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    import paradex_py
    from paradex_py import Paradex
    from paradex_py.environment import Environment
    logger.info(f"paradex-py loaded, version: {getattr(paradex_py, '__version__', 'unknown')}")
    PARADEX_PY_AVAILABLE = True
except ImportError as e:
    PARADEX_PY_AVAILABLE = False
    logger.error(f"paradex-py not installed: {e}")


class ParadexExecutor:
    """
    Executor untuk koneksi dan order ke Paradex.

    Gunakan L1 Ethereum private key:
        ParadexExecutor(l1_private_key="0x...", l1_address="0x...")

    Cara dapat key:
        MetaMask → Account Details → Export Private Key
    """

    def __init__(
        self,
        l1_private_key: str = None,
        l1_address:     str = None,
        # L2 params diterima tapi diabaikan (fallback ke L1)
        l2_private_key: str = None,
        l2_address:     str = None,
    ):
        # Gunakan L1 kalau ada, fallback ke L2 params sebagai L1
        self._l1_key     = (l1_private_key or l2_private_key or "").strip()
        self._l1_address = (l1_address or l2_address or "").strip()
        self.account_address = self._l1_address

        self._ready     = False
        self._positions = {}
        self._pdx       = None

        if not PARADEX_PY_AVAILABLE:
            logger.error("paradex-py tidak tersedia — cek requirements.txt")
            return

        if not self._l1_key or not self._l1_address:
            logger.error("L1 private key dan address wajib diisi")
            return

        self._init_client()

    # ─────────────────────────────────────────────────────────────
    # Async runner — thread-safe
    # ─────────────────────────────────────────────────────────────

    def _run(self, coro):
        """Jalankan coroutine dari sync context."""
        try:
            loop = asyncio.get_event_loop()
            if loop.is_running():
                # Kita di dalam thread yang sudah ada event loop
                import concurrent.futures
                with concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
                    return pool.submit(asyncio.run, coro).result(timeout=30)
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop.run_until_complete(asyncio.wait_for(coro, timeout=30))
        except asyncio.TimeoutError:
            logger.warning("Paradex API call timeout (30s)")
            return None
        except Exception as e:
            logger.warning(f"_run error: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Init
    # ─────────────────────────────────────────────────────────────

    def _init_client(self):
        try:
            logger.info(f"Connecting to Paradex (L1 mode): {self._l1_address[:12]}...")
            self._pdx = Paradex(
                env=Environment.MAINNET,
                l1_address=self._l1_address,
                l1_private_key=self._l1_key,
            )
            # init_account() derive L2 key dari L1 dan generate JWT
            self._run(self._pdx.init_account())
            self._ready = True
            logger.info(f"✅ Paradex connected: {self._l1_address[:12]}...")
        except Exception as e:
            logger.warning(f"Paradex init failed: {e}")
            self._ready = False

    def is_ready(self) -> bool:
        return self._ready and self._pdx is not None

    def reconnect(self) -> bool:
        """Re-init kalau JWT expired."""
        logger.info("Reconnecting to Paradex...")
        self._init_client()
        return self._ready

    # ─────────────────────────────────────────────────────────────
    # Account & Balance
    # ─────────────────────────────────────────────────────────────

    def get_balance(self) -> dict:
        """Return free_collateral, total_collateral, equity, unrealized_pnl."""
        if not self.is_ready():
            return {}
        try:
            data = self._run(self._pdx.api_client.fetch_account_summary())
            if data is None:
                return {}
            # Handle berbagai format response
            if hasattr(data, "__dict__"):
                data = vars(data)
            if isinstance(data, list) and data:
                data = data[0]
                if hasattr(data, "__dict__"):
                    data = vars(data)
            if not isinstance(data, dict):
                logger.warning(f"get_balance unexpected type: {type(data)}")
                return {}
            return {
                "free_collateral":  float(data.get("free_collateral",  data.get("available_margin",  0)) or 0),
                "total_collateral": float(data.get("total_collateral", data.get("initial_margin",    0)) or 0),
                "equity":           float(data.get("equity",           data.get("account_value",     0)) or 0),
                "unrealized_pnl":   float(data.get("unrealized_pnl",  0) or 0),
            }
        except Exception as e:
            logger.warning(f"get_balance error: {e}")
            return {}

    # ─────────────────────────────────────────────────────────────
    # Positions
    # ─────────────────────────────────────────────────────────────

    def sync_all(self):
        """Refresh semua open positions dari Paradex."""
        if not self.is_ready():
            return
        try:
            data = self._run(self._pdx.api_client.fetch_positions())
            if data is None:
                return
            if isinstance(data, list):
                results = data
            elif isinstance(data, dict):
                results = data.get("results", data.get("positions", []))
            else:
                results = []

            self._positions = {}
            for pos in results:
                if hasattr(pos, "__dict__"):
                    pos = vars(pos)
                if not isinstance(pos, dict):
                    continue
                market = pos.get("market", "")
                size   = float(pos.get("size", 0) or 0)
                if size == 0:
                    continue
                self._positions[market] = {
                    "market":         market,
                    "side":           "LONG" if size > 0 else "SHORT",
                    "size":           size,
                    "avg_entry":      float(pos.get("average_entry_price", pos.get("avg_entry_price", 0)) or 0),
                    "unrealized_pnl": float(pos.get("unrealized_pnl", 0) or 0),
                    "leverage":       float(pos.get("leverage", 1) or 1),
                    "liq_price":      float(pos.get("liquidation_price", 0) or 0) or None,
                }
            logger.info(f"Positions synced: {list(self._positions.keys()) or 'none'}")
        except Exception as e:
            logger.warning(f"sync_all error: {e}")

    def get_live_position(self, market: str) -> Optional[dict]:
        """Return posisi untuk market tertentu, None kalau tidak ada."""
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
        """
        Kirim order ke Paradex.
        side: "BUY" atau "SELL"
        Returns order dict atau None kalau gagal.
        """
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
                limit_price=str(price) if price is not None else None,
                reduce_only=reduce_only,
            ))
            if result is None:
                logger.warning(f"place_order returned None for {market}")
                return None
            if hasattr(result, "__dict__"):
                result = vars(result)
            order_id = result.get("id", result.get("order_id", "?")) if isinstance(result, dict) else "?"
            logger.info(f"✅ Order placed: {order_id}")
            return result if isinstance(result, dict) else {"id": str(result)}
        except Exception as e:
            logger.warning(f"place_order error: {e}")
            # Coba reconnect kalau JWT expired
            if "jwt" in str(e).lower() or "token" in str(e).lower() or "unauthorized" in str(e).lower():
                logger.info("JWT might be expired, reconnecting...")
                if self.reconnect():
                    return self.place_order(market, side, size, order_type, price, reduce_only)
            return None

    def cancel_all_orders(self, market: str = None) -> bool:
        """Cancel semua open orders."""
        if not self.is_ready():
            return False
        try:
            if market:
                self._run(self._pdx.api_client.cancel_all_orders(market=market))
            else:
                self._run(self._pdx.api_client.cancel_all_orders())
            logger.info("All orders cancelled")
            return True
        except Exception as e:
            logger.warning(f"cancel_all_orders error: {e}")
            return False

    def get_open_orders(self, market: str = None) -> list:
        """Ambil daftar open orders."""
        if not self.is_ready():
            return []
        try:
            params = {"market": market} if market else {}
            data   = self._run(self._pdx.api_client.fetch_orders(**params))
            if not data:
                return []
            results = data if isinstance(data, list) else data.get("results", [])
            return [vars(o) if hasattr(o, "__dict__") else o for o in results]
        except Exception as e:
            logger.warning(f"get_open_orders error: {e}")
            return []

    def get_fills(self, market: str = None, limit: int = 10) -> list:
        """Ambil history fills/trades terakhir."""
        if not self.is_ready():
            return []
        try:
            kwargs = {"page_size": limit}
            if market:
                kwargs["market"] = market
            data = self._run(self._pdx.api_client.fetch_fills(**kwargs))
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

    # ─────────────────────────────────────────────────────────────
    # Close position helper
    # ─────────────────────────────────────────────────────────────

    def close_position(
        self,
        market:     str,
        order_type: str   = "MARKET",
        price:      float = None,
    ) -> Optional[dict]:
        """
        Tutup posisi existing dengan reduce-only order.
        Auto-sync kalau posisi belum ada di cache.
        """
        pos = self.get_live_position(market)
        if not pos:
            self.sync_all()
            pos = self.get_live_position(market)
        if not pos:
            logger.info(f"No open position found for {market} — skipping close")
            return None

        size       = abs(pos["size"])
        close_side = "SELL" if pos["side"] == "LONG" else "BUY"
        logger.info(f"Closing {market}: {close_side} {size} (was {pos['side']})")

        return self.place_order(
            market=market,
            side=close_side,
            size=size,
            order_type=order_type,
            price=price,
            reduce_only=True,
        )

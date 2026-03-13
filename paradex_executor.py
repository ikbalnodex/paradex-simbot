"""
paradex_executor.py — Paradex Live Trading Executor
Uses paradex-py with L1 Ethereum key.
l1_private_key MUST be passed as integer (int_from_hex).
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Import paradex-py ─────────────────────────────────────────────────────────
PARADEX_PY_AVAILABLE = False
_PROD_ENV = None

try:
    import paradex_py
    from paradex_py import Paradex
    from paradex_py.environment import Environment
    import paradex_py.environment as _pe

    # Log semua konstanta yang tersedia di module environment
    _env_exports = [x for x in dir(_pe) if not x.startswith("_")]
    logger.info(f"paradex-py v{getattr(paradex_py, '__version__', '?')} | environment exports: {_env_exports}")

    # Coba semua nama yang mungkin untuk production environment
    for _prod_name in ("PRODNET", "MAINNET", "PROD", "PRODUCTION"):
        if hasattr(_pe, _prod_name):
            _PROD_ENV = getattr(_pe, _prod_name)
            logger.info(f"Using production env constant: {_prod_name} = {_PROD_ENV}")
            break

    if _PROD_ENV is None:
        # Fallback: gunakan Environment enum jika ada value yg cocok
        for _name in ("PRODNET", "MAINNET", "PROD"):
            try:
                _PROD_ENV = Environment[_name]
                logger.info(f"Using Environment['{_name}'] = {_PROD_ENV}")
                break
            except (KeyError, AttributeError):
                pass

    if _PROD_ENV is None:
        logger.warning("Cannot find production environment constant! Falling back to TESTNET.")
        _PROD_ENV = getattr(_pe, "TESTNET", None) or Environment["TESTNET"]

    PARADEX_PY_AVAILABLE = True

except ImportError as e:
    logger.error(f"paradex-py not installed: {e}")


def _int_from_hex(key: str) -> int:
    """Convert hex private key string ke integer. Paradex-py butuh int."""
    key = key.strip()
    if not key.startswith("0x") and not key.startswith("0X"):
        key = "0x" + key
    return int(key, 16)


class ParadexExecutor:
    """
    Executor untuk koneksi dan order ke Paradex.

    ParadexExecutor(l1_private_key="0x...", l1_address="0x...")

    l1_private_key = Ethereum private key dari MetaMask
    l1_address     = Ethereum wallet address
    """

    def __init__(
        self,
        l1_private_key: str = None,
        l1_address:     str = None,
        l2_private_key: str = None,
        l2_address:     str = None,
    ):
        # Terima L1 atau L2 params (treat L2 params as L1 if no L1 given)
        self._l1_key     = (l1_private_key or l2_private_key or "").strip()
        self._l1_address = (l1_address     or l2_address     or "").strip()
        self.account_address = self._l1_address
        self._ready          = False
        self._positions      = {}
        self._pdx            = None

        if not PARADEX_PY_AVAILABLE:
            logger.error("paradex-py tidak tersedia")
            return
        if not self._l1_key or not self._l1_address:
            logger.error("Private key dan address wajib diisi")
            return

        self._init_client()

    # ─────────────────────────────────────────────────────────────
    # Async runner
    # ─────────────────────────────────────────────────────────────

    def _run(self, coro):
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(asyncio.wait_for(coro, timeout=30))
            finally:
                loop.close()
        except asyncio.TimeoutError:
            logger.warning("Paradex API timeout (30s)")
            return None
        except Exception as e:
            logger.warning(f"_run error: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Init
    # ─────────────────────────────────────────────────────────────

    def _init_client(self):
        try:
            logger.info(f"Connecting to Paradex: {self._l1_address[:12]}... env={_PROD_ENV}")
            self._pdx = Paradex(
                env=_PROD_ENV,
                l1_address=self._l1_address,
                l1_private_key=_int_from_hex(self._l1_key),  # HARUS integer
            )
            try:
                self._run(self._pdx.init_account(l1_address=self._l1_address))
            except Exception as init_err:
                # "already initialized" = account sudah ada, bukan error
                if "already initialized" in str(init_err).lower():
                    logger.info("Account already initialized — OK, continuing")
                else:
                    raise init_err
            self._ready = True
            logger.info(f"✅ Paradex connected: {self._l1_address[:12]}...")
        except Exception as e:
            logger.warning(f"Paradex init failed: {e}")
            self._ready = False

    def is_ready(self) -> bool:
        return self._ready and self._pdx is not None

    def reconnect(self) -> bool:
        logger.info("Reconnecting to Paradex...")
        self._init_client()
        return self._ready

    # ─────────────────────────────────────────────────────────────
    # Balance
    # ─────────────────────────────────────────────────────────────

    def get_balance(self) -> dict:
        if not self.is_ready():
            return {}
        try:
            data = self._pdx.api_client.fetch_account_summary()
            if data is None:
                return {}
            if hasattr(data, "__dict__"):
                data = vars(data)
            if isinstance(data, list) and data:
                data = data[0]
                if hasattr(data, "__dict__"):
                    data = vars(data)
            if not isinstance(data, dict):
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
        if not self.is_ready():
            return
        try:
            data = self._pdx.api_client.fetch_positions()
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
            from paradex_py.common.order import Order, OrderSide, OrderType
            order_obj = Order(
                market=market,
                side=OrderSide(side.upper()),
                type=OrderType(order_type.upper()),
                size=str(size),
                reduce_only=reduce_only,
            )
            if price is not None:
                order_obj.price = str(price)
            result = self._pdx.api_client.submit_order(order=order_obj, signer=self._pdx.account)
            if result is None:
                return None
            if hasattr(result, "__dict__"):
                result = vars(result)
            order_id = result.get("id", "?") if isinstance(result, dict) else "?"
            logger.info(f"✅ Order placed: {order_id}")
            return result if isinstance(result, dict) else {"id": str(result)}
        except Exception as e:
            logger.warning(f"place_order error: {e}")
            if any(k in str(e).lower() for k in ("jwt", "token", "unauthorized", "401")):
                logger.info("JWT expired, reconnecting...")
                if self.reconnect():
                    return self.place_order(market, side, size, order_type, price, reduce_only)
            return None

    def cancel_all_orders(self, market: str = None) -> bool:
        if not self.is_ready():
            return False
        try:
            if market:
                self._pdx.api_client.cancel_all_orders(market=market)
            else:
                self._pdx.api_client.cancel_all_orders()
            return True
        except Exception as e:
            logger.warning(f"cancel_all_orders error: {e}")
            return False

    def get_fills(self, market: str = None, limit: int = 10) -> list:
        if not self.is_ready():
            return []
        try:
            kwargs = {"page_size": limit}
            if market:
                kwargs["market"] = market
            data = self._pdx.api_client.fetch_fills(**kwargs)
            if not data:
                return []
            results = data if isinstance(data, list) else data.get("results", [])
            return [vars(f) if hasattr(f, "__dict__") else f for f in results]
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
            logger.info(f"No open position for {market}")
            return None
        close_side = "SELL" if pos["side"] == "LONG" else "BUY"
        return self.place_order(
            market=market,
            side=close_side,
            size=abs(pos["size"]),
            order_type=order_type,
            price=price,
            reduce_only=True,
        )

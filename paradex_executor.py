"""
paradex_executor.py — Paradex Live Trading Executor
Uses paradex-py library for proper StarkNet L2 authentication.
"""
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from paradex_py import Paradex
    from paradex_py.environment import Environment
    PARADEX_PY_AVAILABLE = True
except ImportError as e:
    PARADEX_PY_AVAILABLE = False
    logger.error(f"paradex-py not installed: {e}")


class ParadexExecutor:
    """
    Executor untuk koneksi dan order ke Paradex.
    Menggunakan paradex-py dengan L1 Ethereum private key.
    """

    def __init__(self, l1_private_key: str, l1_address: str):
        self.l1_private_key  = l1_private_key
        self.account_address = l1_address
        self._ready          = False
        self._positions      = {}
        self._pdx            = None

        if not PARADEX_PY_AVAILABLE:
            logger.error("paradex-py tidak tersedia")
            return

        self._ready = self._init_client()

    def _run(self, coro):
        try:
            loop = asyncio.get_event_loop()
            if loop.is_closed():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
            return loop.run_until_complete(coro)
        except RuntimeError:
            loop = asyncio.new_event_loop()
            return loop.run_until_complete(coro)

    def _init_client(self) -> bool:
        try:
            self._pdx = Paradex(
                env=Environment.PROD,
                l1_address=self.account_address,
                l1_private_key=self.l1_private_key,
            )
            account = self._run(self._pdx.account.get())
            if account:
                logger.info(f"✅ Paradex connected: {self.account_address[:10]}...")
                return True
            return False
        except Exception as e:
            logger.warning(f"Paradex init failed: {e}")
            return False

    def is_ready(self) -> bool:
        return self._ready and self._pdx is not None

    def reconnect(self) -> bool:
        self._ready = self._init_client()
        return self._ready

    def get_balance(self) -> dict:
        if not self.is_ready():
            return {}
        try:
            data = self._run(self._pdx.account.get())
            if not data:
                return {}
            if hasattr(data, "__dict__"):
                data = data.__dict__
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

    def sync_all(self):
        if not self.is_ready():
            return
        try:
            data = self._run(self._pdx.positions.list())
            if not data:
                return
            results = data if isinstance(data, list) else getattr(data, "results", [])
            self._positions = {}
            for pos in results:
                if hasattr(pos, "__dict__"):
                    pos = pos.__dict__
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
            order_params = {
                "market":      market,
                "side":        side.upper(),
                "type":        order_type.upper(),
                "size":        str(size),
                "reduce_only": reduce_only,
            }
            if order_type.upper() == "LIMIT" and price is not None:
                order_params["price"] = str(price)

            logger.info(f"Placing order: {side} {size} {market} @ {order_type}")
            result = self._run(self._pdx.orders.create(**order_params))
            if result:
                if hasattr(result, "__dict__"):
                    result = result.__dict__
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
            if market:
                self._run(self._pdx.orders.cancel_all(market=market))
            else:
                self._run(self._pdx.orders.cancel_all())
            return True
        except Exception as e:
            logger.warning(f"cancel_all_orders error: {e}")
            return False

    def get_open_orders(self, market: str = None) -> list:
        if not self.is_ready():
            return []
        try:
            params = {"market": market} if market else {}
            data   = self._run(self._pdx.orders.list(**params))
            if not data:
                return []
            return data if isinstance(data, list) else getattr(data, "results", [])
        except Exception as e:
            logger.warning(f"get_open_orders error: {e}")
            return []

    def get_fills(self, market: str = None, limit: int = 10) -> list:
        if not self.is_ready():
            return []
        try:
            params = {"page_size": limit}
            if market:
                params["market"] = market
            data = self._run(self._pdx.fills.list(**params))
            if not data:
                return []
            results = data if isinstance(data, list) else getattr(data, "results", [])
            out = []
            for f in results:
                if hasattr(f, "__dict__"):
                    f = f.__dict__
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

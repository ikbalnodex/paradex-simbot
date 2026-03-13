"""
paradex_executor.py — Paradex Live Trading Executor
Handles direct API communication with Paradex exchange.
"""
import time
import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)

PARADEX_API_BASE = "https://api.prod.paradex.trade/v1"


class ParadexExecutor:
    """
    Executor untuk koneksi dan order ke Paradex via subkey JWT.
    """

    def __init__(self, jwt_token: str, account_address: str):
        self.jwt_token       = jwt_token
        self.account_address = account_address
        self._ready          = False
        self._positions      = {}   # market → position dict
        self._account        = {}   # balance info

        # Validasi koneksi saat init
        self._ready = self._check_connection()

    # ─────────────────────────────────────────────────────────────
    # Internal helpers
    # ─────────────────────────────────────────────────────────────

    def _headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.jwt_token}",
            "Content-Type":  "application/json",
            "PARADEX-ETHEREUM-ACCOUNT": self.account_address,
        }

    def _get(self, path: str, params: dict = None) -> Optional[dict]:
        try:
            resp = requests.get(
                f"{PARADEX_API_BASE}{path}",
                headers=self._headers(),
                params=params or {},
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.warning(f"Paradex GET {path} error {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as e:
            logger.warning(f"Paradex GET {path} failed: {e}")
            return None

    def _post(self, path: str, body: dict) -> Optional[dict]:
        try:
            resp = requests.post(
                f"{PARADEX_API_BASE}{path}",
                headers=self._headers(),
                json=body,
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.HTTPError as e:
            logger.warning(f"Paradex POST {path} error {resp.status_code}: {resp.text[:200]}")
            return None
        except Exception as e:
            logger.warning(f"Paradex POST {path} failed: {e}")
            return None

    def _delete(self, path: str) -> Optional[dict]:
        try:
            resp = requests.delete(
                f"{PARADEX_API_BASE}{path}",
                headers=self._headers(),
                timeout=15,
            )
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Paradex DELETE {path} failed: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Connection & Status
    # ─────────────────────────────────────────────────────────────

    def _check_connection(self) -> bool:
        """Cek koneksi dengan ambil summary akun."""
        data = self._get("/account")
        if data and "account" in data:
            self._account = data["account"]
            logger.info(f"✅ Paradex connected: {self.account_address[:10]}...")
            return True
        # Coba endpoint alternatif
        data = self._get("/account/summary")
        if data:
            self._account = data
            logger.info(f"✅ Paradex connected: {self.account_address[:10]}...")
            return True
        logger.warning("❌ Paradex connection failed")
        return False

    def is_ready(self) -> bool:
        return self._ready

    def reconnect(self) -> bool:
        self._ready = self._check_connection()
        return self._ready

    # ─────────────────────────────────────────────────────────────
    # Account & Balance
    # ─────────────────────────────────────────────────────────────

    def get_account_summary(self) -> dict:
        """Ambil balance dan equity dari akun."""
        # Coba beberapa endpoint
        data = self._get("/account/summary")
        if data:
            return data
        data = self._get("/account")
        if data and "account" in data:
            return data["account"]
        return {}

    def get_balance(self) -> dict:
        """Return dict: free_collateral, total_collateral, equity."""
        summary = self.get_account_summary()
        if not summary:
            return {}
        try:
            return {
                "free_collateral":  float(summary.get("free_collateral",  summary.get("available_margin", 0))),
                "total_collateral": float(summary.get("total_collateral", summary.get("initial_margin",   0))),
                "equity":           float(summary.get("equity",           summary.get("account_value",    0))),
                "unrealized_pnl":   float(summary.get("unrealized_pnl",  0)),
            }
        except Exception as e:
            logger.warning(f"get_balance parse error: {e}")
            return {}

    # ─────────────────────────────────────────────────────────────
    # Positions
    # ─────────────────────────────────────────────────────────────

    def sync_all(self):
        """Refresh semua posisi dari Paradex."""
        data = self._get("/positions")
        if not data:
            return
        results = data if isinstance(data, list) else data.get("results", [])
        self._positions = {}
        for pos in results:
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

    def get_live_position(self, market: str) -> Optional[dict]:
        """Return posisi untuk market tertentu, atau None kalau tidak ada."""
        return self._positions.get(market)

    # ─────────────────────────────────────────────────────────────
    # Orders
    # ─────────────────────────────────────────────────────────────

    def place_order(
        self,
        market:     str,
        side:       str,        # "BUY" atau "SELL"
        size:       float,
        order_type: str = "MARKET",
        price:      float = None,
        reduce_only: bool = False,
    ) -> Optional[dict]:
        """
        Kirim order ke Paradex.
        Returns order dict atau None jika gagal.
        """
        body = {
            "market":      market,
            "side":        side.upper(),
            "type":        order_type.upper(),
            "size":        str(size),
            "reduce_only": reduce_only,
        }
        if order_type.upper() == "LIMIT" and price is not None:
            body["price"] = str(price)

        logger.info(f"Placing order: {side} {size} {market} @ {order_type}")
        result = self._post("/orders", body)
        if result:
            order_id = result.get("id", result.get("order_id", "?"))
            logger.info(f"Order placed: {order_id}")
        return result

    def cancel_all_orders(self, market: str = None) -> bool:
        """Cancel semua open orders, opsional filter per market."""
        path   = f"/orders?market={market}" if market else "/orders"
        result = self._delete(path)
        return result is not None

    def get_open_orders(self, market: str = None) -> list:
        params = {"market": market} if market else {}
        data   = self._get("/orders", params=params)
        if not data:
            return []
        return data if isinstance(data, list) else data.get("results", [])

    def get_fills(self, market: str = None, limit: int = 10) -> list:
        """Ambil history fills/trades terakhir."""
        params = {"page_size": limit}
        if market:
            params["market"] = market
        data = self._get("/fills", params=params)
        if not data:
            return []
        return data if isinstance(data, list) else data.get("results", [])

    # ─────────────────────────────────────────────────────────────
    # Helper: close position
    # ─────────────────────────────────────────────────────────────

    def close_position(
        self,
        market:     str,
        order_type: str   = "MARKET",
        price:      float = None,
    ) -> Optional[dict]:
        """Tutup posisi existing dengan reduce-only order."""
        pos = self.get_live_position(market)
        if not pos:
            # Coba sync dulu
            self.sync_all()
            pos = self.get_live_position(market)
        if not pos:
            logger.info(f"No position to close for {market}")
            return None

        size = abs(pos["size"])
        # Kalau LONG → tutup dengan SELL, kalau SHORT → tutup dengan BUY
        close_side = "SELL" if pos["side"] == "LONG" else "BUY"

        return self.place_order(
            market=market,
            side=close_side,
            size=size,
            order_type=order_type,
            price=price,
            reduce_only=True,
        )

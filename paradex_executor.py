"""
paradex_executor.py — Paradex Live Trading Executor

FIXES:
1. set_leverage → POST /account/leverage (endpoint benar Paradex)
2. Size rounding per market step
3. Signature [r,s] → hex + signature_timestamp ms
4. close_all_positions() untuk close via command
"""
import asyncio
import logging
import time
import traceback
from decimal import Decimal, ROUND_DOWN
from typing import Optional

logger = logging.getLogger(__name__)

# ── Market size steps ─────────────────────────────────────────────────────────
# Minimum size increment per market di Paradex
_SIZE_STEP = {
    "ETH-USD-PERP": Decimal("0.0001"),
    "BTC-USD-PERP": Decimal("0.00001"),
}
_DEFAULT_STEP = Decimal("0.0001")


def _round_size(size: float, market: str) -> Decimal:
    step = _SIZE_STEP.get(market, _DEFAULT_STEP)
    d    = Decimal(str(size))
    return (d / step).to_integral_value(rounding=ROUND_DOWN) * step


# ── Import paradex-py ─────────────────────────────────────────────────────────
PARADEX_PY_AVAILABLE = False
_PROD_ENV = None

try:
    import paradex_py
    from paradex_py import Paradex
    from paradex_py.environment import Environment
    import paradex_py.environment as _pe

    _env_exports = [x for x in dir(_pe) if not x.startswith("_")]
    logger.info(f"paradex-py v{getattr(paradex_py, '__version__', '?')} | env exports: {_env_exports}")

    for _prod_name in ("PRODNET", "MAINNET", "PROD", "PRODUCTION"):
        if hasattr(_pe, _prod_name):
            _PROD_ENV = getattr(_pe, _prod_name)
            logger.info(f"Using production env: {_prod_name} = {_PROD_ENV}")
            break

    if _PROD_ENV is None:
        for _name in ("PRODNET", "MAINNET", "PROD"):
            try:
                _PROD_ENV = Environment[_name]
                logger.info(f"Using Environment['{_name}'] = {_PROD_ENV}")
                break
            except (KeyError, AttributeError):
                pass

    if _PROD_ENV is None:
        logger.warning("Cannot find production env! Falling back to TESTNET.")
        _PROD_ENV = getattr(_pe, "TESTNET", None) or Environment["TESTNET"]

    PARADEX_PY_AVAILABLE = True

except ImportError as e:
    logger.error(f"paradex-py not installed: {e}")


def _int_from_hex(key: str) -> int:
    key = key.strip()
    if not key.startswith("0x") and not key.startswith("0X"):
        key = "0x" + key
    return int(key, 16)


def _to_dict(obj):
    if obj is None:
        return None
    if isinstance(obj, list) and obj and isinstance(obj[0], dict):
        obj = obj[0]
    if hasattr(obj, "__dict__"):
        obj = vars(obj)
    if not isinstance(obj, dict):
        obj = {"id": str(obj)}
    return obj


class ParadexExecutor:
    def __init__(
        self,
        l1_private_key: str = None,
        l1_address:     str = None,
        l2_private_key: str = None,
        l2_address:     str = None,
    ):
        self._l1_key     = (l1_private_key or l2_private_key or "").strip()
        self._l1_address = (l1_address     or l2_address     or "").strip()
        self.account_address = self._l1_address
        self._ready          = False
        self._positions      = {}
        self._pdx            = None
        self._base_url       = "https://api.prod.paradex.trade/v1"

        if not PARADEX_PY_AVAILABLE:
            logger.error("paradex-py tidak tersedia")
            return
        if not self._l1_key or not self._l1_address:
            logger.error("Private key dan address wajib diisi")
            return

        self._init_client()

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

    def _init_client(self):
        try:
            logger.info(f"Connecting to Paradex: {self._l1_address[:12]}... env={_PROD_ENV}")
            self._pdx = Paradex(
                env=_PROD_ENV,
                l1_address=self._l1_address,
                l1_private_key=_int_from_hex(self._l1_key),
            )
            try:
                self._run(self._pdx.init_account(l1_address=self._l1_address))
            except Exception as init_err:
                if "already initialized" in str(init_err).lower():
                    logger.info("Account already initialized — OK")
                else:
                    raise init_err

            try:
                url = str(self._pdx.api_client.base_url).rstrip("/")
                if url.startswith("http"):
                    self._base_url = url
            except Exception:
                pass
            logger.info(f"Base URL: {self._base_url}")

            self._ready = True
            logger.info(f"✅ Paradex connected: {self._l1_address[:12]}...")
        except Exception as e:
            logger.warning(f"Paradex init failed: {e}\n{traceback.format_exc()}")
            self._ready = False

    def is_ready(self) -> bool:
        return self._ready and self._pdx is not None

    def reconnect(self) -> bool:
        logger.info("Reconnecting to Paradex...")
        self._init_client()
        return self._ready

    def _get_jwt(self) -> Optional[str]:
        try:
            acc = self._pdx.account
            for attr in ("jwt_token", "jwt", "token", "access_token"):
                val = getattr(acc, attr, None)
                if val:
                    return str(val)
            hdrs = getattr(self._pdx.api_client, "headers", {})
            if isinstance(hdrs, dict):
                for k in ("Authorization", "authorization"):
                    v = hdrs.get(k, "")
                    if v:
                        return v.replace("Bearer ", "").strip()
            return None
        except Exception as e:
            logger.warning(f"_get_jwt error: {e}")
            return None

    def _request(self, method: str, path: str, body: dict = None) -> Optional[dict]:
        """Authenticated REST request ke Paradex."""
        import requests
        jwt = self._get_jwt()
        if not jwt:
            logger.warning("_request: tidak bisa ambil JWT")
            return None
        url     = f"{self._base_url}{path}"
        headers = {
            "Content-Type":  "application/json",
            "Authorization": f"Bearer {jwt}",
        }
        try:
            if method.upper() == "POST":
                resp = requests.post(url, json=body, headers=headers, timeout=15)
            elif method.upper() == "PUT":
                resp = requests.put(url, json=body, headers=headers, timeout=15)
            elif method.upper() == "DELETE":
                resp = requests.delete(url, headers=headers, timeout=15)
            else:
                resp = requests.get(url, headers=headers, timeout=15)

            logger.info(f"{method.upper()} {path} → {resp.status_code}: {resp.text[:300]}")

            if resp.status_code in (200, 201):
                return resp.json() if resp.text.strip() else {"ok": True}
            elif resp.status_code == 204:
                return {"ok": True}
            elif resp.status_code == 401:
                logger.info("JWT expired, reconnecting...")
                if self.reconnect():
                    return self._request(method, path, body)
            else:
                logger.warning(f"HTTP {resp.status_code}: {resp.text}")
            return None
        except Exception as e:
            logger.warning(f"_request error: {e}")
            return None

    # ─────────────────────────────────────────────────────────────
    # Leverage  ← FIX: endpoint /account/leverage
    # ─────────────────────────────────────────────────────────────

    def set_leverage(self, market: str, leverage: float) -> bool:
        """
        Set leverage untuk market tertentu.

        NOTE: Paradex tidak menyediakan REST endpoint khusus untuk set leverage
        secara programatik seperti Binance/Bybit. Leverage di Paradex dikontrol
        via isolated margin account yang harus dibuat dulu dari UI.

        Workaround: Set leverage MANUAL di Paradex UI sekali saja,
        lalu bot akan pakai leverage yang sudah di-set tersebut.

        Method ini tetap mencoba paradex-py jika ada method-nya,
        tapi tidak akan spam REST calls yang 404.
        """
        if not self.is_ready():
            return False

        lev_int = int(leverage)
        logger.info(f"set_leverage {market} {lev_int}x — mencoba via paradex-py")

        # Coba via paradex-py method jika tersedia
        try:
            for fn_name in ("update_leverage", "set_leverage", "change_leverage",
                            "set_margin_config", "update_margin"):
                fn = getattr(self._pdx.api_client, fn_name, None)
                if fn:
                    fn(market=market, leverage=lev_int)
                    logger.info(f"✅ Leverage set via api_client.{fn_name}(): {market} {lev_int}x")
                    return True
        except Exception as e:
            logger.debug(f"api_client leverage method tidak tersedia: {e}")

        # Paradex tidak punya endpoint REST untuk set leverage — skip REST calls
        # Leverage harus di-set manual di UI Paradex
        logger.info(
            f"ℹ️ set_leverage {market} {lev_int}x: Paradex tidak punya REST endpoint leverage. "
            f"Set leverage {lev_int}x manual di UI Paradex untuk market ini."
        )
        return False

    def set_leverage_all(self, leverage: float, markets: list = None) -> None:
        if markets is None:
            markets = list(_SIZE_STEP.keys())
        for m in markets:
            self.set_leverage(m, leverage)

    # ─────────────────────────────────────────────────────────────
    # Balance
    # ─────────────────────────────────────────────────────────────

    def get_balance(self) -> dict:
        if not self.is_ready():
            return {}
        try:
            data = _to_dict(self._pdx.api_client.fetch_account_summary())
            if not data:
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
            results = data if isinstance(data, list) else data.get("results", data.get("positions", []))
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

    def get_all_positions(self) -> dict:
        return dict(self._positions)

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

        size_dec = _round_size(size, market)
        logger.info(f"Placing order: {side} {size_dec} {market} @ {order_type} (raw={size})")

        if size_dec <= 0:
            logger.warning(f"place_order: size terlalu kecil setelah rounding ({size} → {size_dec})")
            return None

        # ── Step 1: Build Order object ────────────────────────────
        try:
            from paradex_py.common.order import Order, OrderSide, OrderType

            order_obj = Order(
                market=market,
                order_type=OrderType(order_type.upper()),
                order_side=OrderSide(side.upper()),
                size=size_dec,
                limit_price=Decimal(str(price)) if price is not None else Decimal("0"),
                reduce_only=reduce_only,
            )
            logger.info(f"Order object OK: {order_obj}")
        except Exception as e:
            logger.warning(f"place_order [build] error: {e}\n{traceback.format_exc()}")
            return None

        # ── Step 2: Sign Order object langsung ───────────────────
        try:
            sig_timestamp_ms = int(time.time() * 1000)
            if hasattr(order_obj, "signature_timestamp"):
                order_obj.signature_timestamp = sig_timestamp_ms

            signature = self._pdx.account.sign_order(order_obj)
            logger.info(f"sign_order OK: {signature}")

            if isinstance(signature, (list, tuple)) and len(signature) >= 2:
                sig_str = f"0x{int(signature[0]):064x}{int(signature[1]):064x}"
            elif isinstance(signature, str):
                sig_str = signature
            else:
                sig_str = str(signature)

            logger.info(f"Signature: {sig_str[:34]}...")
        except Exception as e:
            logger.warning(f"place_order [sign] error: {e}\n{traceback.format_exc()}")
            return None

        # ── Step 3: POST ke /orders ───────────────────────────────
        payload = {
            "market":              market,
            "side":                side.upper(),
            "type":                order_type.upper(),
            "size":                str(size_dec),
            "signature":           sig_str,
            "signature_timestamp": sig_timestamp_ms,
            "reduce_only":         reduce_only,
            "instruction":         "GTC",
        }
        if price is not None:
            payload["price"] = str(price)

        client_id = getattr(order_obj, "client_id", None)
        if client_id:
            payload["client_id"] = str(client_id)

        result = self._request("POST", "/orders", payload)
        if result is None:
            return None

        result   = _to_dict(result)
        order_id = result.get("id", result.get("order_id", result.get("client_id", "?")))
        logger.info(f"✅ Order placed: {order_id}")
        return result

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
        """Close satu posisi spesifik."""
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

    def close_all_positions(self, order_type: str = "MARKET") -> dict:
        """
        Close semua posisi yang terbuka.
        Return: {"ETH-USD-PERP": result_or_none, "BTC-USD-PERP": result_or_none, ...}
        """
        self.sync_all()
        results = {}
        for market, pos in list(self._positions.items()):
            logger.info(f"Closing position: {market} {pos['side']} {pos['size']}")
            result = self.close_position(market, order_type=order_type)
            results[market] = result
            logger.info(f"Close {market}: {'✅' if result else '❌'}")
        return results

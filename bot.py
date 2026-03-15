#!/usr/bin/env python3
"""
Monk Bot B — BTC/ETH Divergence Bot (Swing / Day Trade Edition)
+ Paradex Live Trading Integration
"""
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from enum import Enum
from typing import Optional, Tuple, List, NamedTuple

import requests

import os
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    API_BASE_URL,
    API_ENDPOINT,
    SCAN_INTERVAL_SECONDS,
    TRACK_INTERVAL_SECONDS,
    FRESHNESS_THRESHOLD_MINUTES,
    ENTRY_THRESHOLD,
    EXIT_THRESHOLD,
    INVALIDATION_THRESHOLD,
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)

# =============================================================================
# Paradex Live Trading Integration
# =============================================================================
try:
    from paradex_live import (
        handle_pdx_command,
        handle_live_command,
        live_open_or_sim,
        live_close_or_sim,
        is_live_active,
        live_settings,
        get_executor,
    )
    PARADEX_LIVE_AVAILABLE = True
    logger.info("Paradex live trading module berhasil dimuat.")
except ImportError as _pdx_err:
    PARADEX_LIVE_AVAILABLE = False
    logger.warning(f"Modul Paradex live tidak tersedia: {_pdx_err}")

    def handle_pdx_command(args, chat_id, send_reply_fn=None):
        if send_reply_fn:
            send_reply_fn(
                "⚠️ File `paradex_live.py` tidak ditemukan.\n"
                "Pastikan file tersebut berada di folder yang sama dengan bot.",
                chat_id,
            )

    def handle_live_command(args, chat_id, send_reply_fn=None):
        if send_reply_fn:
            send_reply_fn("⚠️ File `paradex_live.py` tidak ditemukan.", chat_id)

    def is_live_active(): return False
    def live_open_or_sim(*a, **kw): return ""
    def live_close_or_sim(*a, **kw): return ""
    live_settings = {}
    def get_executor(): return None

# =============================================================================
# Konstanta
# =============================================================================
DEFAULT_LOOKBACK_HOURS  = 24
HISTORY_BUFFER_MINUTES  = 30
REDIS_REFRESH_MINUTES   = 1
RATIO_WINDOW_DAYS       = 30

# =============================================================================
# Struktur Data
# =============================================================================
class Mode(Enum):
    SCAN       = "SCAN"
    PEAK_WATCH = "PEAK_WATCH"
    TRACK      = "TRACK"

class Strategy(Enum):
    S1 = "S1"  # Long BTC / Short ETH
    S2 = "S2"  # Long ETH / Short BTC

class PricePoint(NamedTuple):
    timestamp: datetime
    btc:       Decimal
    eth:       Decimal

class PriceData(NamedTuple):
    btc_price:      Decimal
    eth_price:      Decimal
    btc_updated_at: datetime
    eth_updated_at: datetime

# =============================================================================
# State Global
# =============================================================================
price_history:   List[PricePoint]    = []
current_mode:    Mode                = Mode.SCAN
active_strategy: Optional[Strategy] = None

peak_gap:      Optional[float]    = None
peak_strategy: Optional[Strategy] = None

entry_gap_value:   Optional[float]   = None
trailing_gap_best: Optional[float]   = None

entry_btc_price: Optional[Decimal] = None
entry_eth_price: Optional[Decimal] = None
entry_btc_lb:    Optional[Decimal] = None
entry_eth_lb:    Optional[Decimal] = None
entry_btc_ret:   Optional[float]   = None
entry_eth_ret:   Optional[float]   = None
entry_driver:    Optional[str]     = None

# Baca threshold dari environment variable Railway jika tersedia
# Set di Railway: ENTRY_THRESHOLD=0.5, EXIT_THRESHOLD=0.2, dst.
_env_entry     = float(os.environ.get("BOT_ENTRY_THRESHOLD",  os.environ.get("ENTRY_THRESHOLD",  1.5)))
_env_exit      = float(os.environ.get("BOT_EXIT_THRESHOLD",   os.environ.get("EXIT_THRESHOLD",   0.2)))
_env_invalid   = float(os.environ.get("BOT_INVALID_THRESHOLD", INVALIDATION_THRESHOLD))
_env_sl        = float(os.environ.get("BOT_SL_PCT",           1.0))
_env_lookback  = int(os.environ.get("BOT_LOOKBACK_HOURS",     DEFAULT_LOOKBACK_HOURS))
_env_heartbeat = int(os.environ.get("BOT_HEARTBEAT_MINUTES",  30))
_env_capital   = float(os.environ.get("BOT_CAPITAL",          0.0))

settings = {
    "scan_interval":          SCAN_INTERVAL_SECONDS,
    "entry_threshold":        _env_entry,
    "exit_threshold":         _env_exit,
    "invalidation_threshold": _env_invalid,
    "peak_reversal":          0.3,
    "peak_enabled":           False,
    "lookback_hours":         _env_lookback,
    "heartbeat_minutes":      _env_heartbeat,
    "sl_pct":                 _env_sl,
    "redis_refresh_minutes":  REDIS_REFRESH_MINUTES,
    "capital":                _env_capital,
    "ratio_window_days":      RATIO_WINDOW_DAYS,
    "exit_confirm_scans":     2,
    "exit_confirm_buffer":    0.0,
    "exit_pnl_gate":          0.0,
    "eth_size_ratio":         50.0,
    "sim_enabled":            False,
    "sim_margin_usd":         100.0,
    "sim_leverage":           10.0,
    "sim_fee_pct":            0.06,
    "sim_regime_filter":      True,
}

sim_trade: dict = {
    "active":        False,
    "strategy":      None,
    "eth_entry":     None,
    "btc_entry":     None,
    "eth_qty":       None,
    "btc_qty":       None,
    "eth_notional":  None,
    "btc_notional":  None,
    "eth_margin":    None,
    "btc_margin":    None,
    "fee_open":      None,
    "opened_at":     None,
    "history":       [],
}

last_update_id:      int                = 0
last_heartbeat_time: Optional[datetime] = None
last_redis_refresh:  Optional[datetime] = None
exit_confirm_count:  int                = 0

scan_stats = {
    "count":          0,
    "last_btc_price": None,
    "last_eth_price": None,
    "last_btc_ret":   None,
    "last_eth_ret":   None,
    "last_gap":       None,
    "signals_sent":   0,
}

gap_history: List[Tuple[datetime, float]] = []
MAX_GAP_HISTORY = 120

pos_data: dict = {
    "eth_entry_price":  None,
    "eth_qty":          None,
    "eth_notional_usd": None,
    "eth_leverage":     None,
    "eth_liq_price":    None,
    "eth_funding_rate": None,
    "btc_entry_price":  None,
    "btc_qty":          None,
    "btc_notional_usd": None,
    "btc_leverage":     None,
    "btc_liq_price":    None,
    "btc_funding_rate": None,
    "strategy":         None,
    "set_at":           None,
}

# =============================================================================
# Redis
# =============================================================================
REDIS_KEY          = "monk_bot:price_history"
REDIS_KEY_POS      = "monk_bot:pos_data"
REDIS_KEY_SETTINGS = "monk_bot:settings"

def _redis_request(method: str, path: str, body=None):
    if not UPSTASH_REDIS_URL or not UPSTASH_REDIS_TOKEN:
        return None
    try:
        headers = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        url     = f"{UPSTASH_REDIS_URL}{path}"
        resp    = (
            requests.get(url, headers=headers, timeout=10)
            if method == "GET"
            else requests.post(url, headers=headers, json=body, timeout=10)
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.warning(f"Redis request gagal: {e}")
        return None

def load_history() -> None:
    global price_history
    if not UPSTASH_REDIS_URL:
        logger.info("Redis belum dikonfigurasi")
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY}")
        if not result or result.get("result") is None:
            logger.info("Belum ada data history di Redis")
            return
        data = json.loads(result["result"])
        price_history = [
            PricePoint(
                timestamp=datetime.fromisoformat(p["timestamp"]),
                btc=Decimal(p["btc"]),
                eth=Decimal(p["eth"]),
            )
            for p in data
        ]
        logger.info(f"Berhasil memuat {len(price_history)} data dari Redis")
    except Exception as e:
        logger.warning(f"Gagal memuat history: {e}")
        price_history = []

def refresh_history_from_redis(now: datetime) -> None:
    global last_redis_refresh
    interval = settings["redis_refresh_minutes"]
    if interval <= 0:
        return
    if last_redis_refresh is not None:
        if (now - last_redis_refresh).total_seconds() / 60 < interval:
            return
    load_history()
    prune_history(now)
    last_redis_refresh = now
    logger.debug(f"Redis diperbarui. {len(price_history)} data setelah prune")

def save_pos_data() -> bool:
    if not UPSTASH_REDIS_URL:
        return False
    try:
        payload = json.dumps(pos_data, default=str)
        result  = _redis_request("POST", f"/set/{REDIS_KEY_POS}", body=payload)
        if result and result.get("result") == "OK":
            logger.info("Data posisi berhasil disimpan ke Redis")
            return True
        logger.warning(f"save_pos_data hasil tidak terduga: {result}")
        return False
    except Exception as e:
        logger.warning(f"save_pos_data gagal: {e}")
        return False

def load_pos_data() -> None:
    global pos_data
    if not UPSTASH_REDIS_URL:
        return
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY_POS}")
        if not result or result.get("result") is None:
            logger.info("Belum ada data posisi di Redis")
            return
        data = json.loads(result["result"])
        for k in pos_data:
            if k in data and data[k] is not None:
                if k in ("eth_entry_price", "eth_qty", "eth_leverage", "eth_liq_price",
                         "btc_entry_price", "btc_qty", "btc_leverage", "btc_liq_price"):
                    pos_data[k] = float(data[k])
                else:
                    pos_data[k] = data[k]
        logger.info(f"Data posisi dimuat dari Redis: {pos_data.get('strategy')} "
                    f"ETH@{pos_data.get('eth_entry_price')} BTC@{pos_data.get('btc_entry_price')}")
    except Exception as e:
        logger.warning(f"load_pos_data gagal: {e}")

def clear_pos_data_redis() -> bool:
    if not UPSTASH_REDIS_URL:
        return False
    try:
        _redis_request("POST", f"/del/{REDIS_KEY_POS}")
        return True
    except Exception as e:
        logger.warning(f"clear_pos_data_redis gagal: {e}")
        return False

# =============================================================================
# Simpan & Muat Settings ke Redis (Persistent)
# =============================================================================
_PERSISTENT_KEYS = [
    "entry_threshold", "exit_threshold", "invalidation_threshold",
    "sl_pct", "lookback_hours", "heartbeat_minutes", "capital",
    "eth_size_ratio", "peak_reversal", "peak_enabled",
    "exit_confirm_scans", "exit_confirm_buffer", "exit_pnl_gate",
    "sim_enabled", "sim_margin_usd", "sim_leverage", "sim_fee_pct",
]

def save_settings() -> bool:
    """Simpan settings yang bisa diubah user ke Redis."""
    if not UPSTASH_REDIS_URL:
        return False
    try:
        data    = {k: settings[k] for k in _PERSISTENT_KEYS if k in settings}
        payload = json.dumps(data)
        result  = _redis_request("POST", f"/set/{REDIS_KEY_SETTINGS}", body=payload)
        if result and result.get("result") == "OK":
            logger.debug("Settings disimpan ke Redis")
            return True
        return False
    except Exception as e:
        logger.warning(f"save_settings gagal: {e}")
        return False

def load_settings() -> bool:
    """Muat settings dari Redis saat bot start."""
    if not UPSTASH_REDIS_URL:
        return False
    try:
        result = _redis_request("GET", f"/get/{REDIS_KEY_SETTINGS}")
        if not result or result.get("result") is None:
            logger.info("Belum ada settings di Redis — pakai default/env var")
            return False
        data = json.loads(result["result"])
        restored = []
        for k in _PERSISTENT_KEYS:
            if k in data and data[k] is not None:
                try:
                    if k in ("peak_enabled", "sim_enabled", "sim_regime_filter"):
                        settings[k] = bool(data[k])
                    elif k in ("exit_confirm_scans",):
                        settings[k] = int(data[k])
                    else:
                        settings[k] = float(data[k])
                    restored.append(k)
                except Exception:
                    pass
        logger.info(f"Settings dipulihkan dari Redis: {restored}")
        return True
    except Exception as e:
        logger.warning(f"load_settings gagal: {e}")
        return False

# =============================================================================
# Telegram
# =============================================================================
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

def _post_telegram(chat_id: str, message: str) -> bool:
    payload_md = {
        "chat_id":                  chat_id,
        "text":                     message,
        "parse_mode":               "Markdown",
        "disable_web_page_preview": True,
    }
    try:
        resp = requests.post(TELEGRAM_API_URL, json=payload_md, timeout=30)
        resp.raise_for_status()
        return True
    except requests.HTTPError as e:
        if resp.status_code == 400:
            logger.warning("Markdown parse error (400), mencoba kirim plain text.")
            import re as _re
            plain = _re.sub(r"[*_`\[\]]", "", message)
            try:
                resp2 = requests.post(
                    TELEGRAM_API_URL,
                    json={"chat_id": chat_id, "text": plain, "disable_web_page_preview": True},
                    timeout=30,
                )
                resp2.raise_for_status()
                return True
            except requests.RequestException as e2:
                logger.error(f"Fallback plain text gagal: {e2}")
                return False
        logger.error(f"Gagal mengirim pesan Telegram: {e}")
        return False
    except requests.RequestException as e:
        logger.error(f"Gagal mengirim pesan Telegram: {e}")
        return False

def send_alert(message: str) -> bool:
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False
    ok = _post_telegram(TELEGRAM_CHAT_ID, message)
    if ok:
        logger.info("Notifikasi terkirim")
    return ok

def send_reply(message: str, chat_id: str) -> bool:
    if not TELEGRAM_BOT_TOKEN:
        return False
    return _post_telegram(chat_id, message)

# =============================================================================
# Command Polling
# =============================================================================
LONG_POLL_TIMEOUT = 30

def get_telegram_updates() -> list:
    global last_update_id
    if not TELEGRAM_BOT_TOKEN:
        return []
    try:
        url    = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
        params = {"offset": last_update_id + 1, "timeout": LONG_POLL_TIMEOUT}
        resp   = requests.get(url, params=params, timeout=LONG_POLL_TIMEOUT + 5)
        resp.raise_for_status()
        data = resp.json()
        if data.get("ok") and data.get("result"):
            updates = data["result"]
            if updates:
                last_update_id = updates[-1]["update_id"]
            return updates
    except requests.RequestException as e:
        logger.debug(f"Gagal mengambil update: {e}")
    return []

def process_commands() -> None:
    for update in get_telegram_updates():
        message       = update.get("message", {})
        text          = message.get("text", "")
        chat_id       = str(message.get("chat", {}).get("id", ""))
        user_id       = str(message.get("from", {}).get("id", ""))
        is_authorized = (chat_id == TELEGRAM_CHAT_ID) or (chat_id == user_id)
        if not is_authorized or not text.startswith("/"):
            continue
        parts   = text.split()
        command = parts[0].lower().split("@")[0]
        args    = parts[1:] if len(parts) > 1 else []
        logger.info(f"Command diterima: {command} dari {chat_id}")

        dispatch = {
            "/settings":  lambda: handle_settings_command(chat_id),
            "/status":    lambda: handle_status_command(chat_id),
            "/help":      lambda: handle_help_command(chat_id),
            "/start":     lambda: handle_help_command(chat_id),
            "/interval":  lambda: handle_interval_command(args, chat_id),
            "/threshold": lambda: handle_threshold_command(args, chat_id),
            "/lookback":  lambda: handle_lookback_command(args, chat_id),
            "/heartbeat": lambda: handle_heartbeat_command(args, chat_id),
            "/peak":      lambda: handle_peak_command(args, chat_id),
            "/sltp":      lambda: handle_sltp_command(args, chat_id),
            "/redis":     lambda: handle_redis_command(chat_id),
            "/capital":   lambda: handle_capital_command(args, chat_id),
            "/sizeratio": lambda: handle_sizeratio_command(args, chat_id),
            "/sim":       lambda: handle_sim_command(args, chat_id),
            "/simstats":  lambda: handle_simstats_command(chat_id),
            "/ratio":     lambda: handle_ratio_command(chat_id),
            "/pnl":       lambda: handle_pnl_command(chat_id),
            "/analysis":  lambda: handle_analysis_command(chat_id),
            "/setpos":    lambda: handle_setpos_command(args, chat_id),
            "/health":    lambda: handle_health_command(chat_id),
            "/clearpos":  lambda: handle_clearpos_command(chat_id),
            "/setfunding":lambda: handle_setfunding_command(args, chat_id),
            "/velocity":  lambda: handle_velocity_command(chat_id),
            "/exitconf":  lambda: handle_exitconf_command(args, chat_id),
            # Paradex Live Trading
            # /pdx close dihandle di sini supaya bisa reset state bot sekaligus
            "/pdx":       lambda: handle_pdx_close_with_reset(args, chat_id) if (args and args[0].lower() == "close") else handle_pdx_command(args, chat_id, send_reply),
            "/live":      lambda: handle_live_command(args, chat_id, send_reply),
            # Command darurat reset state bot
            "/forceclose": lambda: handle_forceclose_command(chat_id),
            "/resetstate": lambda: handle_forceclose_command(chat_id),
        }
        if command in dispatch:
            dispatch[command]()

# =============================================================================
# Analisis Engine
# =============================================================================

def analyze_gap_driver(btc_ret, eth_ret, gap):
    abs_btc = abs(btc_ret)
    abs_eth = abs(eth_ret)
    total   = abs_btc + abs_eth
    if total == 0:
        return "Mixed", "⚪", "Keduanya tidak bergerak"
    eth_contrib = abs_eth / total * 100
    btc_contrib = abs_btc / total * 100
    if eth_contrib >= 65:
        driver  = "ETH-led"
        emoji   = "🟡"
        explain = f"ETH {'pumping' if eth_ret > 0 else 'dumping'} dominan ({eth_contrib:.0f}% kontribusi)"
    elif btc_contrib >= 65:
        driver  = "BTC-led"
        emoji   = "🟠"
        explain = f"BTC {'naik' if btc_ret > 0 else 'turun'} dominan ({btc_contrib:.0f}% kontribusi)"
    else:
        driver  = "Mixed"
        emoji   = "⚪"
        explain = f"ETH {eth_contrib:.0f}% / BTC {btc_contrib:.0f}% — keduanya berkontribusi"
    return driver, emoji, explain

def detect_market_regime() -> dict:
    if len(price_history) < 3:
        return {"regime": "N/A", "emoji": "⚪", "strength": "—",
                "description": "Data belum cukup", "implications": "—",
                "btc_1h": None, "btc_4h": None, "btc_24h": None,
                "eth_1h": None, "eth_4h": None, "eth_24h": None,
                "volatility": "—", "vol_pct": 0.0}
    now      = price_history[-1]
    btc_now  = float(now.btc)
    eth_now  = float(now.eth)
    interval = settings["scan_interval"]

    def _pct_change(minutes: int):
        scans_back = max(1, int(minutes * 60 / interval))
        if len(price_history) <= scans_back:
            return None, None
        old = price_history[-scans_back - 1]
        return (btc_now - float(old.btc)) / float(old.btc) * 100, (eth_now - float(old.eth)) / float(old.eth) * 100

    btc_1h,  eth_1h  = _pct_change(60)
    btc_4h,  eth_4h  = _pct_change(240)
    btc_24h, eth_24h = _pct_change(1440)

    scans_1h   = max(2, int(3600 / interval))
    window_pts = price_history[-scans_1h:] if len(price_history) >= scans_1h else price_history
    vol_samples = []
    for i in range(1, len(window_pts)):
        prev_b = float(window_pts[i-1].btc)
        curr_b = float(window_pts[i].btc)
        if prev_b > 0:
            vol_samples.append(abs(curr_b - prev_b) / prev_b * 100)
    avg_vol = sum(vol_samples) / len(vol_samples) if vol_samples else 0.0

    if avg_vol >= 0.15:   vol_label = "Tinggi 🔥"
    elif avg_vol >= 0.05: vol_label = "Normal 📊"
    else:                 vol_label = "Rendah 😴"

    def _vote(ret, threshold=0.5):
        if ret is None: return 0
        if ret > threshold:  return 1
        if ret < -threshold: return -1
        return 0

    votes = _vote(btc_1h, 0.3)*1 + _vote(btc_4h, 0.8)*2 + _vote(btc_24h, 1.5)*3

    if votes >= 4:      regime, emoji = "BULLISH",     "🟢"
    elif votes >= 1:    regime, emoji = "BULLISH",     "🟡"
    elif votes <= -4:   regime, emoji = "BEARISH",     "🔴"
    elif votes <= -1:   regime, emoji = "BEARISH",     "🟠"
    else:               regime, emoji = "KONSOLIDASI", "⚪"

    if abs(votes) >= 4:   strength = "Kuat"
    elif abs(votes) >= 2: strength = "Moderat"
    else:                 strength = "Lemah"

    desc = f"BTC dalam tren {'naik' if regime == 'BULLISH' else 'turun' if regime == 'BEARISH' else 'sideways'}"

    if regime == "BULLISH" and strength == "Kuat":
        impl = "✅ *S1 setup* — pump kuat, ETH cenderung outperform BTC."
    elif regime == "BULLISH":
        impl = "S1 moderat — pump ada tapi belum kuat."
    elif regime == "BEARISH" and strength == "Kuat":
        impl = "✅ *S2 setup* — dump kuat, ETH cenderung underperform BTC."
    elif regime == "BEARISH":
        impl = "S2 moderat — dump ada tapi belum kuat."
    else:
        impl = "⚠️ Sideways — kedua strategi sulit, tunggu arah jelas."

    return {
        "regime": regime, "emoji": emoji, "strength": strength, "votes": votes,
        "btc_1h": btc_1h, "eth_1h": eth_1h, "btc_4h": btc_4h, "eth_4h": eth_4h,
        "btc_24h": btc_24h, "eth_24h": eth_24h,
        "volatility": vol_label, "vol_pct": avg_vol,
        "description": desc, "implications": impl,
    }

def get_convergence_hint(strategy: Strategy, driver: str) -> str:
    if strategy == Strategy.S1:
        hints = {
            "ETH-led": "ETH pump yang dominan → kemungkinan *ETH pullback* dulu.",
            "BTC-led": "BTC yang ketinggalan naik → tunggu *BTC catch up*.",
            "Mixed":   "Bisa revert dari ETH pullback atau BTC catch up.",
        }
    else:
        hints = {
            "ETH-led": "ETH dump yang dominan → kemungkinan *ETH bounce* dulu.",
            "BTC-led": "BTC yang terlalu kuat → tunggu *BTC koreksi*.",
            "Mixed":   "Bisa revert dari ETH bounce atau BTC koreksi.",
        }
    return hints.get(driver, "")

def calc_ratio_percentile():
    if not price_history or len(price_history) < 10:
        return None, None, None, None, None
    now    = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=settings["ratio_window_days"])
    window = [p for p in price_history if p.timestamp >= cutoff]
    if len(window) < 5:
        window = price_history
    ratios    = [float(p.eth / p.btc) for p in window]
    if not ratios:
        return None, None, None, None, None
    current   = ratios[-1]
    avg       = sum(ratios) / len(ratios)
    high      = max(ratios)
    low       = min(ratios)
    below     = sum(1 for r in ratios if r <= current)
    percentile= int(below / len(ratios) * 100)
    return current, avg, high, low, percentile

def get_ratio_conviction(strategy: Strategy, pct: Optional[int]):
    if pct is None:
        return "⭐⭐⭐", "Data terbatas"
    if strategy == Strategy.S2:
        if pct <= 10:   return "⭐⭐⭐⭐⭐", "ETH *sangat murah* vs BTC — conviction tertinggi"
        elif pct <= 25: return "⭐⭐⭐⭐",   "ETH *murah* vs BTC — setup bagus"
        elif pct <= 40: return "⭐⭐⭐",     "ETH cukup murah — setup moderat"
        elif pct <= 60: return "⭐⭐",       "ETH di area tengah"
        else:           return "⭐",         "ETH *mahal* vs BTC — S2 berisiko"
    else:
        if pct >= 90:   return "⭐⭐⭐⭐⭐", "ETH *sangat mahal* vs BTC — conviction tertinggi"
        elif pct >= 75: return "⭐⭐⭐⭐",   "ETH *mahal* vs BTC — setup bagus"
        elif pct >= 60: return "⭐⭐⭐",     "ETH cukup mahal — setup moderat"
        elif pct >= 40: return "⭐⭐",       "ETH di area tengah"
        else:           return "⭐",         "ETH *murah* vs BTC — S1 berisiko"

def _calc_ratio_extended_stats(curr_r, avg_r, hi_r, lo_r, pct_r):
    pct_from_high = (curr_r - hi_r) / hi_r * 100 if hi_r else 0
    pct_from_low  = (curr_r - lo_r) / lo_r * 100  if lo_r else 0
    range_total   = hi_r - lo_r if hi_r and lo_r else 0
    pos_in_range  = (curr_r - lo_r) / range_total * 100 if range_total > 0 else 50
    revert_to_avg = (avg_r - curr_r) / curr_r * 100 if avg_r else 0
    ratios  = [float(p.eth / p.btc) for p in price_history]
    z_score = None
    if len(ratios) >= 10:
        mean = sum(ratios) / len(ratios)
        std  = (sum((r - mean) ** 2 for r in ratios) / len(ratios)) ** 0.5
        if std > 0:
            z_score = (curr_r - mean) / std
    return {
        "pct_from_high": pct_from_high, "pct_from_low": pct_from_low,
        "pos_in_range": pos_in_range, "z_score": z_score, "revert_to_avg": revert_to_avg,
    }

def _build_conviction_detail(strategy, stars, pct_r, curr_r, avg_r, hi_r, lo_r, ext):
    window = settings["ratio_window_days"]
    z      = ext["z_score"]
    z_str  = f"{z:+.2f}σ dari avg" if z is not None else "N/A"
    if strategy == Strategy.S1:
        label = "S1 — Long BTC / Short ETH"
        reasons = []
        if pct_r >= 75:
            reasons.append(f"Ratio *{pct_r}th percentile* — ETH mahal secara historis ({window}d)")
        if ext["pct_from_high"] >= -3.0:
            reasons.append(f"Ratio *{abs(ext['pct_from_high']):.2f}%* di bawah {window}d high")
        if z is not None and z >= 1.0:
            reasons.append(f"Z-score *{z:+.2f}σ* — ETH secara statistik mahal vs BTC")
        if pct_r >= 90:   timing = "🟢 Timing sangat baik"
        elif pct_r >= 75: timing = "🟡 Timing baik"
        elif pct_r >= 60: timing = "🟠 Timing cukup"
        else:             timing = "🔴 Timing kurang ideal"
        risk = "⚠️ *Risiko:* Ratio bisa terus naik sebelum revert"
        entry_note = f"_💡 Mentor rule: ratio ≥75th pct = konfirmasi S1. Sekarang {pct_r}th → {'✅' if pct_r >= 75 else '❌'}_"
        reason_block = "\n".join(f"│ ✅ {r}" for r in reasons) if reasons else "│ Belum ada sinyal kuat"
        return (
            f"*{stars} {label}*\n"
            f"┌─────────────────────\n"
            f"│ Percentile:  *{pct_r}th* dari {window}d history\n"
            f"│ Dari high:   {ext['pct_from_high']:+.2f}%\n"
            f"│ Dari avg:    revert *{ext['revert_to_avg']:+.2f}%* ke {avg_r:.5f}\n"
            f"│ Z-score:     {z_str}\n"
            f"├─────────────────────\n"
            f"{reason_block}\n"
            f"├─────────────────────\n"
            f"│ {timing}\n"
            f"└─────────────────────\n"
            f"{risk}\n{entry_note}"
        )
    else:
        label = "S2 — Long ETH / Short BTC"
        reasons = []
        if pct_r <= 25:
            reasons.append(f"Ratio *{pct_r}th percentile* — ETH murah secara historis ({window}d)")
        if ext["pct_from_low"] <= 3.0:
            reasons.append(f"Ratio *{ext['pct_from_low']:.2f}%* dari {window}d low")
        if z is not None and z <= -1.0:
            reasons.append(f"Z-score *{z:+.2f}σ* — ETH secara statistik murah vs BTC")
        if pct_r <= 10:   timing = "🟢 Timing sangat baik"
        elif pct_r <= 25: timing = "🟡 Timing baik"
        elif pct_r <= 40: timing = "🟠 Timing cukup"
        else:             timing = "🔴 Timing kurang ideal"
        risk = "⚠️ *Risiko:* Ratio bisa terus turun"
        entry_note = f"_💡 Mentor rule: ratio ≤25th pct = konfirmasi S2. Sekarang {pct_r}th → {'✅' if pct_r <= 25 else '❌'}_"
        reason_block = "\n".join(f"│ ✅ {r}" for r in reasons) if reasons else "│ Belum ada sinyal kuat untuk S2"
        return (
            f"*{stars} {label}*\n"
            f"┌─────────────────────\n"
            f"│ Percentile:  *{pct_r}th* dari {window}d history\n"
            f"│ Dari low:    +{ext['pct_from_low']:.2f}%\n"
            f"│ Dari avg:    revert *{ext['revert_to_avg']:+.2f}%* ke {avg_r:.5f}\n"
            f"│ Z-score:     {z_str}\n"
            f"├─────────────────────\n"
            f"{reason_block}\n"
            f"├─────────────────────\n"
            f"│ {timing}\n"
            f"└─────────────────────\n"
            f"{risk}\n{entry_note}"
        )

def calc_sizing(btc_price, eth_price):
    capital   = settings["capital"]
    eth_ratio = max(1.0, min(99.0, float(settings["eth_size_ratio"]))) / 100.0
    btc_ratio = 1.0 - eth_ratio
    if capital <= 0 or float(btc_price) <= 0 or float(eth_price) <= 0:
        return 0.0, 0.0, 0.0, 0.0
    eth_alloc = capital * eth_ratio
    btc_alloc = capital * btc_ratio
    eth_qty   = eth_alloc / float(eth_price)
    btc_qty   = btc_alloc / float(btc_price)
    return eth_alloc, btc_alloc, eth_qty, btc_qty

def calc_convergence_scenarios(strategy, btc_now, eth_now, btc_lb, eth_lb):
    et = settings["exit_threshold"]
    try:
        btc_ret_now = float((btc_now - btc_lb) / btc_lb * Decimal("100"))
        eth_ret_now = float((eth_now - eth_lb) / eth_lb * Decimal("100"))
        if strategy == Strategy.S1:
            eth_a = float(eth_lb) * (1 + (btc_ret_now - et) / 100)
            btc_b = float(btc_lb) * (1 + (eth_ret_now - et) / 100)
        else:
            eth_a = float(eth_lb) * (1 + (btc_ret_now + et) / 100)
            btc_b = float(btc_lb) * (1 + (eth_ret_now + et) / 100)
        return eth_a, btc_b
    except Exception:
        return None, None

def calc_net_pnl(strategy, current_gap):
    if entry_gap_value is None:
        return None, None, None
    if entry_btc_price is None or entry_eth_price is None:
        net = entry_gap_value - current_gap if strategy == Strategy.S1 else current_gap - entry_gap_value
        return None, None, net
    try:
        btc_now = scan_stats.get("last_btc_price")
        eth_now = scan_stats.get("last_eth_price")
        if btc_now is None or eth_now is None:
            return None, None, None
        if strategy == Strategy.S1:
            leg_btc = float((btc_now - entry_btc_price) / entry_btc_price * 100)
            leg_eth = float((entry_eth_price - eth_now)  / entry_eth_price * 100)
        else:
            leg_eth = float((eth_now - entry_eth_price) / entry_eth_price * 100)
            leg_btc = float((entry_btc_price - btc_now) / entry_btc_price * 100)
        return leg_eth, leg_btc, (leg_eth + leg_btc) / 2
    except Exception:
        return None, None, None

def get_pairs_health(strategy, current_gap):
    if entry_gap_value is None:
        return "❓", "Tidak ada posisi aktif"
    et = settings["exit_threshold"]
    if strategy == Strategy.S1:
        progress = (entry_gap_value - current_gap) / (entry_gap_value - et) * 100 if entry_gap_value != et else 100
    else:
        progress = (current_gap - entry_gap_value) / (-et - entry_gap_value) * 100 if entry_gap_value != -et else 100
    progress = max(0.0, min(100.0, progress))
    if progress >= 80:   return "🟢", f"Hampir TP! Progress {progress:.0f}%"
    elif progress >= 50: return "🟡", f"Setengah jalan. Progress {progress:.0f}%"
    elif progress >= 20: return "🔵", f"Mulai bergerak. Progress {progress:.0f}%"
    elif progress >= 0:  return "⚪", f"Belum banyak bergerak. Progress {progress:.0f}%"
    else:                return "🔴", f"Berlawanan arah. Progress {progress:.0f}%"

# =============================================================================
# Position Health Engine
# =============================================================================

def sim_open_position(strategy: Strategy, btc_price: float, eth_price: float) -> str:
    if not settings["sim_enabled"]:
        return ""
    if sim_trade["active"]:
        return "_⚠️ Sim: posisi sudah aktif, lewati open._\n"

    if settings["sim_regime_filter"]:
        btc_r = entry_btc_ret
        eth_r = entry_eth_ret
        skip_reason = None
        if strategy == Strategy.S1 and btc_r < 0:
            skip_reason = (
                f"Gap+ tapi market sedang *DUMP* (BTC {btc_r:+.2f}% / ETH {eth_r:+.2f}%)\n"
                f"_S1 hanya valid saat market sedang pump._"
            )
        elif strategy == Strategy.S2 and btc_r > 0:
            skip_reason = (
                f"Gap- tapi market sedang *PUMP* (BTC {btc_r:+.2f}% / ETH {eth_r:+.2f}%)\n"
                f"_S2 hanya valid saat market sedang dump._"
            )
        if skip_reason:
            return (
                f"\n🤖 *[SIM] Entry Dilewati — Regime Mismatch* ⚠️\n"
                f"┌─────────────────────\n│ {skip_reason}\n└─────────────────────\n"
                f"_Filter dapat dinonaktifkan: `/sim regime off`_\n"
            )

    margin  = float(settings["sim_margin_usd"])
    lev     = float(settings["sim_leverage"])
    fee_pct = float(settings["sim_fee_pct"]) / 100.0
    notional   = margin * lev
    eth_ratio  = float(settings["eth_size_ratio"]) / 100.0
    btc_ratio  = 1.0 - eth_ratio
    eth_notional = notional * eth_ratio
    btc_notional = notional * btc_ratio
    eth_margin_  = eth_notional / lev
    btc_margin_  = btc_notional / lev
    eth_qty_abs  = eth_notional / eth_price
    btc_qty_abs  = btc_notional / btc_price

    if strategy == Strategy.S1:
        eth_qty = -eth_qty_abs
        btc_qty = +btc_qty_abs
    else:
        eth_qty = +eth_qty_abs
        btc_qty = -btc_qty_abs

    fee_open = (eth_notional + btc_notional) * fee_pct
    sim_trade.update({
        "active": True, "strategy": strategy.value,
        "eth_entry": eth_price, "btc_entry": btc_price,
        "eth_qty": eth_qty, "btc_qty": btc_qty,
        "eth_notional": eth_notional, "btc_notional": btc_notional,
        "eth_margin": eth_margin_, "btc_margin": btc_margin_,
        "fee_open": fee_open, "opened_at": datetime.now(timezone.utc).isoformat(),
    })
    pos_data.update({
        "eth_entry_price": eth_price, "eth_qty": eth_qty,
        "eth_notional_usd": eth_notional, "eth_leverage": lev,
        "eth_liq_price": None, "eth_funding_rate": None,
        "btc_entry_price": btc_price, "btc_qty": btc_qty,
        "btc_notional_usd": btc_notional, "btc_leverage": lev,
        "btc_liq_price": None, "btc_funding_rate": None,
        "strategy": strategy.value, "set_at": sim_trade["opened_at"],
    })
    save_pos_data()

    eth_dir = "Long 📈" if eth_qty > 0 else "Short 📉"
    btc_dir = "Long 📈" if btc_qty > 0 else "Short 📉"
    er = settings["eth_size_ratio"]; br = 100 - er
    return (
        f"\n🤖 *[SIM] Posisi Dibuka Otomatis*\n"
        f"┌─────────────────────\n"
        f"│ ETH: *{eth_dir}* {abs(eth_qty):.4f} @ ${eth_price:,.2f}\n"
        f"│ BTC: *{btc_dir}* {abs(btc_qty):.6f} @ ${btc_price:,.2f}\n"
        f"│ Leverage: {lev:.0f}x | Rasio: {er:.0f}/{br:.0f}\n"
        f"│ Fee buka: ${fee_open:.3f}\n"
        f"└─────────────────────\n"
        f"_Ketik `/health` untuk memantau P&L secara live._\n"
    )

def sim_close_position(btc_price: float, eth_price: float, reason: str = "EXIT") -> str:
    if not settings["sim_enabled"] or not sim_trade["active"]:
        return ""
    eth_qty      = sim_trade["eth_qty"]
    btc_qty      = sim_trade["btc_qty"]
    eth_entry    = sim_trade["eth_entry"]
    btc_entry    = sim_trade["btc_entry"]
    eth_notional = sim_trade["eth_notional"]
    btc_notional = sim_trade["btc_notional"]
    eth_margin   = sim_trade["eth_margin"]
    btc_margin   = sim_trade["btc_margin"]
    fee_pct      = float(settings["sim_fee_pct"]) / 100.0
    fee_open     = sim_trade["fee_open"]
    fee_close    = (eth_notional + btc_notional) * fee_pct
    total_fee    = fee_open + fee_close
    total_margin = eth_margin + btc_margin
    eth_pnl      = eth_qty * (eth_price - eth_entry)
    btc_pnl      = btc_qty * (btc_price - btc_entry)
    gross_pnl    = eth_pnl + btc_pnl
    net_pnl      = gross_pnl - total_fee
    net_pct      = net_pnl / total_margin * 100

    opened_at = sim_trade.get("opened_at")
    dur_str   = "N/A"
    if opened_at:
        try:
            sa = datetime.fromisoformat(opened_at)
            dur_min = int((datetime.now(timezone.utc) - sa).total_seconds() / 60)
            h, m = divmod(dur_min, 60)
            dur_str = f"{h}h {m}m" if h > 0 else f"{m}m"
        except Exception:
            pass

    result_e = "🟢" if net_pnl >= 0 else "🔴"
    result_s = "PROFIT" if net_pnl >= 0 else "LOSS"
    sign     = "+" if net_pnl >= 0 else ""

    sim_trade["history"].append({
        "strategy": sim_trade["strategy"], "reason": reason,
        "eth_entry": eth_entry, "eth_exit": eth_price,
        "btc_entry": btc_entry, "btc_exit": btc_price,
        "gross_pnl": gross_pnl, "fee": total_fee,
        "net_pnl": net_pnl, "net_pct": net_pct,
        "duration": dur_str, "closed_at": datetime.now(timezone.utc).isoformat(),
    })
    sim_trade.update({
        "active": False, "strategy": None, "eth_entry": None, "btc_entry": None,
        "eth_qty": None, "btc_qty": None, "eth_notional": None, "btc_notional": None,
        "eth_margin": None, "btc_margin": None, "fee_open": None, "opened_at": None,
    })
    for k in ["eth_entry_price","eth_qty","eth_notional_usd","eth_leverage","eth_liq_price",
              "btc_entry_price","btc_qty","btc_notional_usd","btc_leverage","btc_liq_price","strategy","set_at"]:
        pos_data[k] = None
    save_pos_data()

    return (
        f"\n🤖 *[SIM] Posisi Ditutup — {result_e} {result_s}*\n"
        f"┌─────────────────────\n"
        f"│ ETH: {eth_entry:,.2f} → {eth_price:,.2f}\n"
        f"│ BTC: {btc_entry:,.2f} → {btc_price:,.2f}\n"
        f"├─────────────────────\n"
        f"│ *Net P&L: {sign}${net_pnl:.2f} ({sign}{net_pct:.2f}%)*\n"
        f"│ Fee: -${total_fee:.3f} | Durasi: {dur_str}\n"
        f"└─────────────────────\n"
        f"Total trade: {len(sim_trade['history'])} | Ketik `/simstats` untuk rekap\n"
    )

def calc_gap_velocity() -> dict:
    if len(gap_history) < 3:
        return {}
    try:
        now = datetime.now(timezone.utc)
        def _slice(minutes):
            cutoff = now - timedelta(minutes=minutes)
            return [(ts, g) for ts, g in gap_history if ts >= cutoff]
        def _delta(pts):
            return pts[-1][1] - pts[0][1] if len(pts) >= 2 else None
        def _velocity(pts):
            if len(pts) < 2: return None
            dt = (pts[-1][0] - pts[0][0]).total_seconds() / 60
            return (pts[-1][1] - pts[0][1]) / dt if dt > 0 else None

        pts_15 = _slice(15); pts_30 = _slice(30); pts_60 = _slice(60)
        v15 = _velocity(pts_15); v60 = _velocity(pts_60)
        curr_gap = gap_history[-1][1]
        accel    = v15 / v60 if v15 is not None and v60 is not None and v60 != 0 else None
        et       = settings["exit_threshold"]
        eta_minutes = None
        if v15 is not None and v15 != 0:
            gap_to_tp  = abs(curr_gap) - et
            conv_rate  = -abs(v15) if v15 * curr_gap > 0 else abs(v15)
            if gap_to_tp > 0 and conv_rate != 0:
                eta_minutes = gap_to_tp / abs(conv_rate)
        return {
            "curr_gap": curr_gap, "delta_15m": _delta(pts_15),
            "delta_30m": _delta(pts_30), "delta_60m": _delta(pts_60),
            "vel_15m": v15, "vel_60m": v60, "accel": accel,
            "eta_min": eta_minutes, "n_pts": len(gap_history),
        }
    except Exception as e:
        logger.warning(f"calc_gap_velocity error: {e}")
        return {}

def calc_position_pnl() -> dict:
    if pos_data["eth_entry_price"] is None or pos_data["btc_entry_price"] is None:
        return {}
    btc_now = scan_stats.get("last_btc_price")
    eth_now = scan_stats.get("last_eth_price")
    if btc_now is None or eth_now is None:
        return {}
    try:
        eth_entry = pos_data["eth_entry_price"]; eth_qty = pos_data["eth_qty"]
        eth_lev   = pos_data["eth_leverage"] or 1.0; eth_fr = pos_data.get("eth_funding_rate") or 0.0
        btc_entry = pos_data["btc_entry_price"]; btc_qty = pos_data["btc_qty"]
        btc_lev   = pos_data["btc_leverage"] or 1.0; btc_fr = pos_data.get("btc_funding_rate") or 0.0
        eth_p = float(eth_now); btc_p = float(btc_now)

        eth_notional   = pos_data.get("eth_notional_usd") or (abs(eth_qty) * eth_entry)
        btc_notional   = pos_data.get("btc_notional_usd") or (abs(btc_qty) * btc_entry)
        eth_margin     = eth_notional / eth_lev
        btc_margin     = btc_notional / btc_lev
        total_margin   = eth_margin + btc_margin
        total_notional = eth_notional + btc_notional

        eth_pnl  = eth_qty * (eth_p - eth_entry)
        btc_pnl  = btc_qty * (btc_p - btc_entry)
        net_pnl  = eth_pnl + btc_pnl
        eth_pnl_pct = eth_pnl / eth_margin * 100 if eth_margin > 0 else 0
        btc_pnl_pct = btc_pnl / btc_margin * 100 if btc_margin > 0 else 0
        net_pnl_pct = net_pnl / total_margin * 100 if total_margin > 0 else 0

        set_at_str = pos_data.get("set_at"); time_in_min = None; time_label = "N/A"
        if set_at_str:
            try:
                sa = datetime.fromisoformat(set_at_str)
                time_in_min = (datetime.now(timezone.utc) - sa).total_seconds() / 60
                h_part = int(time_in_min // 60); m_part = int(time_in_min % 60)
                time_label = f"{h_part}h {m_part}m" if h_part > 0 else f"{m_part}m"
            except Exception:
                pass

        def _funding_flow(qty, notional, fr):
            return -(1.0 if qty > 0 else -1.0) * notional * (fr / 100)

        eth_f8h = _funding_flow(eth_qty, eth_notional, eth_fr)
        btc_f8h = _funding_flow(btc_qty, btc_notional, btc_fr)
        net_f8h = eth_f8h + btc_f8h
        total_fp = net_f8h * (time_in_min / 480) if time_in_min else 0
        net_pnl_af = net_pnl + total_fp
        net_pnl_af_pct = net_pnl_af / total_margin * 100 if total_margin > 0 else 0

        be_h = None
        if net_pnl > 0 and net_f8h < 0:
            be_h = (net_pnl / abs(net_f8h)) * 8
        elif net_pnl < 0 and net_f8h > 0:
            be_h = abs(net_pnl) / abs(net_f8h) * 8

        total_equity = total_margin + net_pnl
        margin_ratio = total_equity / total_notional * 100 if total_notional > 0 else 0
        maint_margin = total_notional * 0.005
        liq_buffer_usd = total_equity - maint_margin
        liq_buffer_pct = liq_buffer_usd / total_equity * 100 if total_equity > 0 else 0

        eth_liq = pos_data["eth_liq_price"]
        btc_liq = pos_data["btc_liq_price"]
        if eth_liq is None and eth_qty != 0:
            eth_liq = eth_entry - (eth_margin / eth_qty)
        if btc_liq is None and btc_qty != 0:
            btc_liq = btc_entry - (btc_margin / btc_qty)

        eth_dist_liq = abs(eth_p - eth_liq) / eth_p * 100 if eth_liq else None
        btc_dist_liq = abs(btc_p - btc_liq) / btc_p * 100 if btc_liq else None

        if margin_ratio >= 10:   health_e, health_label = "🟢", "SEHAT"
        elif margin_ratio >= 5:  health_e, health_label = "🟡", "PERHATIKAN"
        elif margin_ratio >= 3:  health_e, health_label = "🟠", "WASPADA"
        else:                    health_e, health_label = "🔴", "BAHAYA — Mendekati Likuidasi!"

        return {
            "eth_pnl": eth_pnl, "eth_pnl_pct": eth_pnl_pct,
            "eth_notional": eth_notional, "eth_margin": eth_margin,
            "eth_value_now": abs(eth_qty) * eth_p,
            "eth_lev": eth_lev, "eth_liq_est": eth_liq,
            "eth_dist_liq": eth_dist_liq, "eth_danger": eth_dist_liq is not None and eth_dist_liq < 10,
            "eth_funding_per_8h": eth_f8h,
            "btc_pnl": btc_pnl, "btc_pnl_pct": btc_pnl_pct,
            "btc_notional": btc_notional, "btc_margin": btc_margin,
            "btc_value_now": abs(btc_qty) * btc_p,
            "btc_lev": btc_lev, "btc_liq_est": btc_liq,
            "btc_dist_liq": btc_dist_liq, "btc_danger": btc_dist_liq is not None and btc_dist_liq < 10,
            "btc_funding_per_8h": btc_f8h,
            "net_pnl": net_pnl, "net_pnl_pct": net_pnl_pct,
            "net_pnl_after_funding": net_pnl_af, "net_pnl_af_pct": net_pnl_af_pct,
            "total_margin": total_margin, "total_notional": total_notional,
            "total_equity": total_equity, "margin_ratio": margin_ratio,
            "liq_buffer_usd": liq_buffer_usd, "liq_buffer_pct": liq_buffer_pct,
            "net_funding_per_8h": net_f8h, "net_funding_per_day": net_f8h * 3,
            "total_funding_paid": total_fp, "breakeven_hours": be_h,
            "time_in_min": time_in_min, "time_label": time_label,
            "health_emoji": health_e, "health_label": health_label,
        }
    except Exception as e:
        logger.warning(f"calc_position_pnl error: {e}")
        return {}

def build_position_health_message(h: dict) -> str:
    strat   = pos_data.get("strategy") or "?"
    eth_p   = float(scan_stats["last_eth_price"]) if scan_stats.get("last_eth_price") else 0
    btc_p   = float(scan_stats["last_btc_price"]) if scan_stats.get("last_btc_price") else 0
    eth_qty = pos_data["eth_qty"]
    btc_qty = pos_data["btc_qty"]
    eth_dir = "Long 📈" if eth_qty and eth_qty > 0 else "Short 📉"
    btc_dir = "Long 📈" if btc_qty and btc_qty > 0 else "Short 📉"

    def _s(v): return "+" if v >= 0 else ""
    def _e(v): return "🟢" if v >= 0 else "🔴"

    # Sinkronisasi dari Paradex jika live aktif
    if PARADEX_LIVE_AVAILABLE and is_live_active():
        try:
            executor = get_executor()
            if executor and executor.is_ready():
                executor.sync_all()
                pdx_eth = executor.get_live_position("ETH-USD-PERP")
                pdx_btc = executor.get_live_position("BTC-USD-PERP")
                if pdx_eth:
                    pos_data["eth_entry_price"] = pdx_eth["avg_entry"]
                    pos_data["eth_qty"]         = abs(pdx_eth["size"]) if pdx_eth["side"] == "LONG" else -abs(pdx_eth["size"])
                    pos_data["eth_leverage"]    = pdx_eth.get("leverage", live_settings.get("leverage", 10))
                    pos_data["eth_liq_price"]   = pdx_eth.get("liq_price")
                if pdx_btc:
                    pos_data["btc_entry_price"] = pdx_btc["avg_entry"]
                    pos_data["btc_qty"]         = abs(pdx_btc["size"]) if pdx_btc["side"] == "LONG" else -abs(pdx_btc["size"])
                    pos_data["btc_leverage"]    = pdx_btc.get("leverage", live_settings.get("leverage", 10))
                    pos_data["btc_liq_price"]   = pdx_btc.get("liq_price")
                h = calc_position_pnl()
                if not h:
                    h = {}
                eth_qty = pos_data["eth_qty"]
                btc_qty = pos_data["btc_qty"]
                eth_dir = "Long 📈" if eth_qty and eth_qty > 0 else "Short 📉"
                btc_dir = "Long 📈" if btc_qty and btc_qty > 0 else "Short 📉"
        except Exception as e:
            logger.warning(f"Sinkronisasi health dari Paradex gagal: {e}")

    if not h:
        return "⚠️ Kalkulasi health gagal. Silakan coba `/setpos` ulang."

    eth_liq_s  = f"${h['eth_liq_est']:,.2f}" if h.get("eth_liq_est") else "N/A"
    btc_liq_s  = f"${h['btc_liq_est']:,.2f}" if h.get("btc_liq_est") else "N/A"
    eth_dist_s = f"{h['eth_dist_liq']:.1f}% jauh" if h.get("eth_dist_liq") else "N/A"
    btc_dist_s = f"{h['btc_dist_liq']:.1f}% jauh" if h.get("btc_dist_liq") else "N/A"
    eth_liq_e  = "⚠️" if h.get("eth_danger") else "✅"
    btc_liq_e  = "⚠️" if h.get("btc_danger") else "✅"

    eth_fr = pos_data.get("eth_funding_rate") or 0.0
    btc_fr = pos_data.get("btc_funding_rate") or 0.0
    has_funding = eth_fr != 0.0 or btc_fr != 0.0
    eth_f8h  = h.get("eth_funding_per_8h", 0)
    btc_f8h  = h.get("btc_funding_per_8h", 0)
    net_f8h  = h.get("net_funding_per_8h", 0)
    net_fday = h.get("net_funding_per_day", 0)
    total_fp = h.get("total_funding_paid", 0)
    be_h     = h.get("breakeven_hours")

    if has_funding:
        eth_f_dir = "terima 🟢" if eth_f8h >= 0 else "bayar 🔴"
        btc_f_dir = "terima 🟢" if btc_f8h >= 0 else "bayar 🔴"
        net_f_dir = "terima 🟢" if net_f8h >= 0 else "bayar 🔴"
        be_str    = f"{be_h:.1f}h" if be_h is not None else "N/A"
        funding_block = (
            f"\n*💸 Biaya Funding:*\n"
            f"┌─────────────────────\n"
            f"│ ETH: {eth_fr:+.4f}%/8h → {_s(eth_f8h)}${abs(eth_f8h):.3f} ({eth_f_dir})\n"
            f"│ BTC: {btc_fr:+.4f}%/8h → {_s(btc_f8h)}${abs(btc_f8h):.3f} ({btc_f_dir})\n"
            f"│ Net: {_s(net_f8h)}${abs(net_f8h):.3f}/8h | {_s(net_fday)}${abs(net_fday):.2f}/hari ({net_f_dir})\n"
            f"│ Total dibayar: {_s(total_fp)}${abs(total_fp):.2f}\n"
            f"│ Net P&L setelah funding: {_e(h['net_pnl_after_funding'])} "
            f"{_s(h['net_pnl_after_funding'])}${h['net_pnl_after_funding']:,.2f} "
            f"({_s(h['net_pnl_af_pct'])}{h['net_pnl_af_pct']:.2f}%)\n"
            + (f"│ ⏱️ Break-even: *{be_str}* lagi\n" if be_h is not None else "")
            + f"└─────────────────────\n"
        )
    else:
        funding_block = "\n_💸 Funding belum diset — gunakan `/setfunding`._\n"

    vel = calc_gap_velocity()
    vel_block = ""
    if vel:
        d15      = vel.get("delta_15m")
        d60      = vel.get("delta_60m")
        eta      = vel.get("eta_min")
        curr_gap = vel.get("curr_gap", 0)
        accel    = vel.get("accel")
        if d15 is not None:
            conv  = abs(curr_gap + d15) < abs(curr_gap)
            d15_e = "⬇️ konvergen" if conv else "⬆️ melebar"
            d15_s = f"{d15:+.3f}%"
        else:
            d15_e, d15_s = "—", "N/A"
        d60_s    = f"{d60:+.3f}%" if d60 is not None else "N/A"
        accel_s  = (
            "📉 melambat" if accel and accel < 0.8
            else "📈 mempercepat" if accel and accel > 1.2
            else "➡️ stabil"
        ) if accel else "N/A"
        eta_s    = f"~{int(eta)}m" if eta is not None and eta < 10000 else "tidak dapat dihitung"
        vel_block = (
            f"\n*📡 Kecepatan Gap:*\n"
            f"┌─────────────────────\n"
            f"│ Gap sekarang: {curr_gap:+.3f}%\n"
            f"│ Δ 15m: {d15_s} {d15_e}\n"
            f"│ Δ 60m: {d60_s}\n"
            f"│ Tren: {accel_s}\n"
            f"│ Estimasi ke TP: ~{eta_s}\n"
            f"│ Data: {vel['n_pts']} titik\n"
            f"└─────────────────────\n"
        )

    mr       = h.get("margin_ratio", 0)
    mr_fill  = min(10, int(mr / 2))
    mr_bar   = "█" * mr_fill + "░" * (10 - mr_fill)
    lb_pct   = max(0.0, h.get("liq_buffer_pct", 0))
    lb_fill  = min(10, int(lb_pct / 10))
    lb_bar   = "█" * lb_fill + "░" * (10 - lb_fill)

    danger_note = ""
    if h.get("eth_danger") or h.get("btc_danger"):
        legs = []
        if h.get("eth_danger"): legs.append("ETH")
        if h.get("btc_danger"): legs.append("BTC")
        danger_note = f"\n🚨 *PERINGATAN: {'/'.join(legs)} mendekati harga likuidasi!*\n"

    live_tag = ""
    if PARADEX_LIVE_AVAILABLE and is_live_active():
        dr = " 🧪 DRYRUN" if live_settings.get("dryrun") else ""
        live_tag = f"\n_🔴 Data disinkronkan langsung dari Paradex{dr}_\n"

    eth_notional  = h.get("eth_notional", 0)
    btc_notional  = h.get("btc_notional", 0)
    eth_value_now = abs(eth_qty or 0) * eth_p
    btc_value_now = abs(btc_qty or 0) * btc_p
    eth_margin    = h.get("eth_margin", 0)
    btc_margin    = h.get("btc_margin", 0)

    eth_entry = pos_data.get("eth_entry_price", 0) or 0
    btc_entry = pos_data.get("btc_entry_price", 0) or 0
    net_note  = ""
    if eth_entry > 0 and btc_entry > 0:
        net_note = (
            f"_💡 NET P&L adalah yang terpenting dalam pairs trade._"
        )

    return (
        f"🏥 *Position Health — {strat}*\n"
        f"⏱️ Waktu dalam trade: *{h.get('time_label', 'N/A')}*\n"
        f"💰 ETH: ${eth_p:,.2f} | BTC: ${btc_p:,.2f}\n"
        f"{live_tag}"
        f"\n*📊 ETH Leg ({eth_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:    ${pos_data.get('eth_entry_price', 0):,.2f}\n"
        f"│ Qty:      {abs(eth_qty or 0):.4f} ETH\n"
        f"│ Leverage: {h.get('eth_lev', 1):.0f}x\n"
        f"│ Notional: ${eth_notional:,.2f} → nilai kini: ${eth_value_now:,.2f}\n"
        f"│ Margin:   ${eth_margin:,.2f}\n"
        f"│ UPnL: {_e(h['eth_pnl'])} {_s(h['eth_pnl'])}${h['eth_pnl']:,.2f} ({_s(h['eth_pnl_pct'])}{h['eth_pnl_pct']:.2f}%)\n"
        f"│ Liq: {eth_liq_s} {eth_liq_e} | {eth_dist_s}\n"
        f"└─────────────────────\n"
        f"\n*📊 BTC Leg ({btc_dir}):*\n"
        f"┌─────────────────────\n"
        f"│ Entry:    ${pos_data.get('btc_entry_price', 0):,.2f}\n"
        f"│ Qty:      {abs(btc_qty or 0):.6f} BTC\n"
        f"│ Leverage: {h.get('btc_lev', 1):.0f}x\n"
        f"│ Notional: ${btc_notional:,.2f} → nilai kini: ${btc_value_now:,.2f}\n"
        f"│ Margin:   ${btc_margin:,.2f}\n"
        f"│ UPnL: {_e(h['btc_pnl'])} {_s(h['btc_pnl'])}${h['btc_pnl']:,.2f} ({_s(h['btc_pnl_pct'])}{h['btc_pnl_pct']:.2f}%)\n"
        f"│ Liq: {btc_liq_s} {btc_liq_e} | {btc_dist_s}\n"
        f"└─────────────────────\n"
        f"\n*⚖️ Ringkasan Pairs:*\n"
        f"┌─────────────────────\n"
        f"│ Notional: ${h.get('total_notional', 0):,.2f}\n"
        f"│ Margin:   ${h.get('total_margin', 0):,.2f}\n"
        f"│ Ekuitas:  ${h.get('total_equity', 0):,.2f}\n"
        f"│ Net UPnL: {_e(h['net_pnl'])} {_s(h['net_pnl'])}${h['net_pnl']:,.2f} ({_s(h['net_pnl_pct'])}{h['net_pnl_pct']:.2f}%)\n"
        f"└─────────────────────\n"
        f"{funding_block}"
        f"{vel_block}"
        f"*🛡️ Kesehatan Margin:*\n"
        f"┌─────────────────────\n"
        f"│ Margin Ratio: {mr:.2f}%\n"
        f"│ {mr_bar} {h.get('health_emoji', '')} *{h.get('health_label', '')}*\n"
        f"│ Buffer Liq: ${h.get('liq_buffer_usd', 0):,.2f} ({lb_pct:.1f}%)\n"
        f"│ {lb_bar} sebelum likuidasi\n"
        f"└─────────────────────\n"
        f"{danger_note}"
        f"\n{net_note}"
    )

# =============================================================================
# Entry Readiness Engine
# =============================================================================

def build_entry_readiness(strategy, pct_r, curr_r, avg_r, ext):
    gap_now = scan_stats.get("last_gap")
    btc_r   = scan_stats.get("last_btc_ret")
    eth_r   = scan_stats.get("last_eth_ret")
    et      = settings["entry_threshold"]
    it      = settings["invalidation_threshold"]
    gap_f   = float(gap_now) if gap_now is not None else 0.0
    gap_abs = abs(gap_f)

    checks = []
    correct_side = (gap_f >= et) if strategy == Strategy.S1 else (gap_f <= -et)
    if correct_side:
        checks.append((True,  "Gap di zona entry", f"Gap {gap_f:+.2f}% melewati ±{et}%"))
    else:
        checks.append((False, "Gap belum di threshold", f"Gap {gap_f:+.2f}% | perlu {et - gap_abs:.2f}% lagi"))

    if pct_r is not None:
        ratio_ok = pct_r >= 60 if strategy == Strategy.S1 else pct_r <= 40
        checks.append((ratio_ok, f"Ratio {pct_r}th percentile", f"{'Baik' if ratio_ok else 'Belum ideal'}"))
    else:
        checks.append((None, "Ratio N/A", "Butuh lebih banyak history"))

    if btc_r is not None and eth_r is not None:
        driver, _, driver_ex = analyze_gap_driver(float(btc_r), float(eth_r), gap_f)
        driver_ok = driver in ("ETH-led", "Mixed")
        checks.append((driver_ok, f"Driver: {driver}", driver_ex))
    else:
        checks.append((None, "Driver belum tersedia", "Tunggu data scan"))

    dist_invalid = (it - gap_f) if strategy == Strategy.S1 else (gap_f - (-it))
    buffer_ok    = dist_invalid >= 0.5
    checks.append((buffer_ok, "Buffer invalidasi", f"{dist_invalid:.2f}% sebelum invalid ±{it}%"))

    true_count  = sum(1 for c in checks if c[0] is True)
    false_count = sum(1 for c in checks if c[0] is False)
    total_valid = sum(1 for c in checks if c[0] is not None)
    score_pct   = true_count / total_valid * 100 if total_valid > 0 else 0

    if false_count == 0 and true_count >= 3:
        v_e, verdict = "🟢", "SIAP ENTRY"
    elif not correct_side or false_count >= 2:
        v_e, verdict = "🔴", "JANGAN ENTRY DULU"
    elif false_count <= 1 and true_count >= 2:
        v_e, verdict = "🟡", "BISA ENTRY, TAPI BERHATI-HATI"
    else:
        v_e, verdict = "🟠", "TUNGGU KONFIRMASI"

    checklist = "".join(
        f"{'✅' if ok is True else '❌' if ok is False else '⚪'} *{label}*: _{detail}_\n"
        for ok, label, detail in checks
    )

    return (
        f"*── Entry Readiness: {strategy.value} ──*\n"
        f"{v_e} *{verdict}*\n"
        f"Skor: {true_count}/{total_valid} ✅ ({score_pct:.0f}%)\n"
        f"\n{checklist}"
    )

# =============================================================================
# Helpers & Formatter
# =============================================================================
def format_value(value) -> str:
    fv = float(value)
    if abs(fv) < 0.05:
        return "+0.0"
    return f"+{fv:.1f}" if fv >= 0 else f"{fv:.1f}"

def get_lookback_label() -> str:
    return f"{settings['lookback_hours']}h"

def calc_eth_price_at_gap(gap_target):
    if None in (entry_btc_lb, entry_eth_lb, entry_btc_price):
        return None, None
    try:
        btc_ret_entry  = float((entry_btc_price - entry_btc_lb) / entry_btc_lb * Decimal("100"))
        target_eth_ret = btc_ret_entry + gap_target
        eth_target     = float(entry_eth_lb) * (1 + target_eth_ret / 100)
        return eth_target, float(entry_btc_price)
    except Exception:
        return None, None

def calc_tp_target_price(strategy):
    et         = settings["exit_threshold"]
    gap_target = et if strategy == Strategy.S1 else -et
    return calc_eth_price_at_gap(gap_target)

def _build_pnl_section(leg_e, leg_b, net, emoji=""):
    if net is None:
        return ""
    capital = settings["capital"]
    if leg_e is not None and leg_b is not None and capital > 0:
        half = capital / 2.0
        usd_e = leg_e / 100 * half; usd_b = leg_b / 100 * half; usd_net = usd_e + usd_b
        _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
        if _live_mode == "x":
            if active_strategy == Strategy.S1:
                return (
                    f"\n*Estimasi P&L — Mode X 🟣:*\n"
                    f"┌─────────────────────\n"
                    f"│ Short ETH: {leg_e:+.2f}% (${usd_e:+.2f})\n"
                    f"│ Short BTC: {leg_b:+.2f}% (${usd_b:+.2f})\n"
                    f"│ *Net: {net:+.2f}% (${usd_net:+.2f})* {emoji}\n"
                    f"└─────────────────────\n\n"
                )
            else:
                return (
                    f"\n*Estimasi P&L — Mode X 🟣:*\n"
                    f"┌─────────────────────\n"
                    f"│ Long ETH:  {leg_e:+.2f}% (${usd_e:+.2f})\n"
                    f"│ Long BTC:  {leg_b:+.2f}% (${usd_b:+.2f})\n"
                    f"│ *Net: {net:+.2f}% (${usd_net:+.2f})* {emoji}\n"
                    f"└─────────────────────\n\n"
                )
        else:
            if active_strategy == Strategy.S1:
                return (
                    f"\n*Estimasi P&L Pairs:*\n"
                    f"┌─────────────────────\n"
                    f"│ Long BTC:  {leg_b:+.2f}% (${usd_b:+.2f})\n"
                    f"│ Short ETH: {leg_e:+.2f}% (${usd_e:+.2f})\n"
                    f"│ *Net: {net:+.2f}% (${usd_net:+.2f})* {emoji}\n"
                    f"└─────────────────────\n\n"
                )
            else:
                return (
                    f"\n*Estimasi P&L Pairs:*\n"
                    f"┌─────────────────────\n"
                    f"│ Long ETH:  {leg_e:+.2f}% (${usd_e:+.2f})\n"
                    f"│ Short BTC: {leg_b:+.2f}% (${usd_b:+.2f})\n"
                    f"│ *Net: {net:+.2f}% (${usd_net:+.2f})* {emoji}\n"
                    f"└─────────────────────\n\n"
                )
    elif net is not None:
        return f"\n_Pergerakan gap net: {net:+.2f}%_\n\n"
    return ""

# =============================================================================
# Message Builders
# =============================================================================

def build_peak_watch_message(strategy, gap):
    lb = get_lookback_label()
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    if strategy == Strategy.S1:
        reason = f"ETH pumping lebih kencang dari BTC ({lb})"
        direction = "Short ETH + Short BTC (Mode X)" if _live_mode == "x" else "Long BTC / Short ETH"
    else:
        reason = f"ETH dumping lebih dalam dari BTC ({lb})"
        direction = "Long ETH + Long BTC (Mode X)" if _live_mode == "x" else "Long ETH / Short BTC"
    mode_tag = " 🟣" if _live_mode == "x" else ""
    return (
        f"………\nSinyal menarik terdeteksi.\n\n"
        f"_{reason}_\nRencana: *{direction}*{mode_tag}\nGap sekarang: *{format_value(gap)}%*\n\n"
        f"Bot akan menunggu konfirmasi puncak sebelum masuk. ⚡"
    )

def build_entry_message(strategy, btc_ret, eth_ret, gap, peak, btc_now, eth_now, btc_lb, eth_lb, is_direct=False):
    lb        = get_lookback_label()
    gap_float = float(gap)
    sl_pct    = settings["sl_pct"]
    et        = settings["exit_threshold"]

    # Tentukan direction label sesuai mode aktif
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    if strategy == Strategy.S1:
        tp_gap      = et
        tsl_initial = gap_float + sl_pct
        if _live_mode == "x":
            direction = "Short ETH + Short BTC (Mode X)"
        else:
            direction = "Long BTC / Short ETH"
    else:
        tp_gap      = -et
        tsl_initial = gap_float - sl_pct
        if _live_mode == "x":
            direction = "Long ETH + Long BTC (Mode X)"
        else:
            direction = "Long ETH / Short BTC"

    driver, driver_emoji, driver_explain = analyze_gap_driver(float(btc_ret), float(eth_ret), gap_float)
    conv_hint = get_convergence_hint(strategy, driver)

    curr_r, avg_r, hi_r, lo_r, pct_r = calc_ratio_percentile()
    stars, conviction = get_ratio_conviction(strategy, pct_r)
    ratio_str = f"{curr_r:.5f}" if curr_r else "N/A"
    avg_str   = f"{avg_r:.5f}"  if avg_r  else "N/A"
    pct_str   = f"{pct_r}th"    if pct_r is not None else "N/A"

    eth_tp, btc_ref  = calc_tp_target_price(strategy)
    eth_tp_str       = f"${eth_tp:,.2f}"  if eth_tp  else "N/A"
    btc_ref_str      = f"${btc_ref:,.2f}" if btc_ref else "N/A"
    eth_tsl, _       = calc_eth_price_at_gap(tsl_initial)
    eth_tsl_str      = f"${eth_tsl:,.2f}" if eth_tsl else "N/A"

    eth_a, btc_b = calc_convergence_scenarios(strategy, btc_now, eth_now, btc_lb, eth_lb)
    if strategy == Strategy.S1:
        scen_a_str = f"ETH turun ke *${eth_a:,.2f}*" if eth_a else "N/A"
        scen_b_str = f"BTC naik ke *${btc_b:,.2f}*"  if btc_b else "N/A"
        scen_a_label, scen_b_label = "ETH pullback", "BTC catch-up"
    else:
        scen_a_str = f"ETH naik ke *${eth_a:,.2f}*"  if eth_a else "N/A"
        scen_b_str = f"BTC turun ke *${btc_b:,.2f}*" if btc_b else "N/A"
        scen_a_label, scen_b_label = "ETH bounce", "BTC koreksi"

    reg = detect_market_regime()
    if strategy == Strategy.S1:
        regime_fit = "✅ Ideal — market pump kuat" if reg["regime"] == "BULLISH" and reg["strength"] == "Kuat" else (
            "✅ OK — market pump moderat" if reg["regime"] == "BULLISH" else
            "⚠️ Sideways — S1 bisa tapi lebih lambat" if reg["regime"] == "KONSOLIDASI" else
            "⚠️ Market dump — S1 berisiko"
        )
    else:
        regime_fit = "✅ Ideal — market dump kuat" if reg["regime"] == "BEARISH" and reg["strength"] == "Kuat" else (
            "✅ OK — market dump moderat" if reg["regime"] == "BEARISH" else
            "⚠️ Sideways — S2 bisa tapi lebih lambat" if reg["regime"] == "KONSOLIDASI" else
            "⚠️ Market pump — S2 berisiko"
        )

    eth_ratio = settings["eth_size_ratio"]; btc_ratio = 100.0 - eth_ratio
    ratio_tag = f"{eth_ratio:.0f}/{btc_ratio:.0f}" if eth_ratio != 50.0 else "50/50"
    eth_alloc, btc_alloc, eth_qty, btc_qty = calc_sizing(btc_now, eth_now)
    if eth_alloc > 0:
        if _live_mode == "x":
            # Mode X: keduanya searah
            if strategy == Strategy.S1:
                sizing_section = (
                    f"\n💰 *Sizing ({ratio_tag}, ${settings['capital']:,.0f}) — Mode X:*\n"
                    f"┌─────────────────────\n│ Short ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                    f"│ Short BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n└─────────────────────\n"
                )
            else:
                sizing_section = (
                    f"\n💰 *Sizing ({ratio_tag}, ${settings['capital']:,.0f}) — Mode X:*\n"
                    f"┌─────────────────────\n│ Long ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                    f"│ Long BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n└─────────────────────\n"
                )
        else:
            # Mode Normal: pairs trade
            if strategy == Strategy.S1:
                sizing_section = (
                    f"\n💰 *Sizing ({ratio_tag}, ${settings['capital']:,.0f}):*\n"
                    f"┌─────────────────────\n│ Long BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n"
                    f"│ Short ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n└─────────────────────\n"
                )
            else:
                sizing_section = (
                    f"\n💰 *Sizing ({ratio_tag}, ${settings['capital']:,.0f}):*\n"
                    f"┌─────────────────────\n│ Long ETH: ${eth_alloc:,.0f} → {eth_qty:.4f} ETH\n"
                    f"│ Short BTC: ${btc_alloc:,.0f} → {btc_qty:.6f} BTC\n└─────────────────────\n"
                )
    else:
        sizing_section = "\n_💡 Gunakan `/capital <modal>` untuk panduan sizing._\n"

    peak_line     = f"│ Peak:     {peak:+.2f}%\n" if not is_direct and peak else ""
    direct_tag    = " _(Peak OFF)_\n" if is_direct else "\n"
    reversal_note = (
        f"_Gap berbalik {settings['peak_reversal']}% dari puncak → entry terkonfirmasi._\n"
        if not is_direct else ""
    )

    live_line = ""
    if PARADEX_LIVE_AVAILABLE and is_live_active():
        lev        = float(live_settings.get("leverage", 1))
        otype      = live_settings.get("order_type", "MARKET")
        _mode_lbl  = "🟣 Mode X" if live_settings.get("mode") == "x" else "🔵 Normal"
        live_line  = f"\n🔴 *Live AKTIF — {otype} {lev:.0f}x | {_mode_lbl}*\n"

    return (
        f"Sinyal yang ditunggu sudah muncul! ⚡\n"
        f"🚨 *SINYAL ENTRY: {strategy.value}*{direct_tag}"
        f"📈 *{direction}*\n"
        f"{live_line}\n"
        f"*── 1. Analisis Gap ({lb}) ──*\n"
        f"┌─────────────────────\n"
        f"│ BTC:    {format_value(btc_ret)}%\n│ ETH:    {format_value(eth_ret)}%\n"
        f"│ Gap:    *{format_value(gap)}%*\n{peak_line}"
        f"│ Driver: {driver_emoji} *{driver}* — {driver_explain}\n"
        f"└─────────────────────\n"
        f"_💡 {conv_hint}_\n\n"
        f"*── 2. Rasio ETH/BTC ──*\n"
        f"┌─────────────────────\n"
        f"│ Sekarang: {ratio_str} | {lb} rata-rata: {avg_str}\n"
        f"│ Persentil: *{pct_str}* | {stars}\n"
        f"│ _{conviction}_\n└─────────────────────\n\n"
        f"*── 3. Regime Pasar ──*\n"
        f"┌─────────────────────\n"
        f"│ {reg['emoji']} *{reg['regime']}* {reg['strength']}\n│ {regime_fit}\n"
        f"└─────────────────────\n\n"
        f"*── 4. Target & Proteksi ──*\n"
        f"┌─────────────────────\n"
        f"│ TP gap: {tp_gap:+.2f}% | ETH TP: {eth_tp_str}\n"
        f"│ Trail SL: {tsl_initial:+.2f}% → ETH {eth_tsl_str}\n"
        f"└─────────────────────\n\n"
        f"*── 5. Skenario Konvergensi ──*\n"
        f"• *A — {scen_a_label}:* {scen_a_str}\n"
        f"• *B — {scen_b_label}:* {scen_b_str}\n\n"
        f"{sizing_section}{reversal_note}\n"
        f"Keputusan ada di tangan Anda. Semangat! ⚡"
    )

def build_exit_message(btc_ret, eth_ret, gap, confirm_note=""):
    lb          = get_lookback_label()
    gap_f       = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section = _build_pnl_section(leg_e, leg_b, net)
    conf_line   = f"_{confirm_note}_\n" if confirm_note else ""
    return (
        f"Sinyal exit terdeteksi.\n✅ *EXIT — Gap Konvergen!*\n{conf_line}\n"
        f"*Perubahan {lb}:*\n┌─────────────────────\n"
        f"│ BTC: {format_value(btc_ret)}% | ETH: {format_value(eth_ret)}% | Gap: *{format_value(gap)}%*\n"
        f"└─────────────────────\n{net_section}"
        f"Pertimbangkan untuk menutup posisi. Bot terus memantau. ⚡"
    )

def build_tp_message(btc_ret, eth_ret, gap, entry_gap, tp_level, eth_target):
    lb          = get_lookback_label()
    eth_str     = f"${eth_target:,.2f}" if eth_target else "N/A"
    gap_f       = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section = _build_pnl_section(leg_e, leg_b, net, emoji="🎉")
    return (
        f"Target profit tercapai! ✨\n🎯 *TAKE PROFIT*\n\n"
        f"*Perubahan {lb}:*\n┌─────────────────────\n"
        f"│ BTC: {format_value(btc_ret)}% | ETH: {format_value(eth_ret)}%\n"
        f"│ Gap: *{format_value(gap)}%* | TP: {tp_level:+.2f}% | ETH: {eth_str}\n"
        f"└─────────────────────\n{net_section}Posisi ditutup dengan hasil positif. ⚡"
    )

def build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap, best_gap, sl_level):
    lb          = get_lookback_label()
    profit_locked = abs(entry_gap - best_gap)
    eth_sl, _   = calc_eth_price_at_gap(sl_level)
    eth_sl_str  = f"${eth_sl:,.2f}" if eth_sl else "N/A"
    gap_f       = float(gap)
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_f) if active_strategy else (None, None, None)
    net_section = _build_pnl_section(leg_e, leg_b, net)
    return (
        f"………\n⛔ *TRAILING STOP LOSS*\n\n"
        f"*Perubahan {lb}:*\n┌─────────────────────\n"
        f"│ Gap: {format_value(gap)}% | Best: {best_gap:+.2f}% | TSL: {sl_level:+.2f}% → ETH {eth_sl_str}\n"
        f"│ Profit terkunci: ~{profit_locked:.2f}%\n└─────────────────────\n"
        f"{net_section}Bot kembali ke mode SCAN. ⚡"
    )

def build_invalidation_message(strategy, btc_ret, eth_ret, gap):
    lb          = get_lookback_label()
    gap_f       = float(gap)
    leg_e, leg_b, net = calc_net_pnl(strategy, gap_f)
    net_section = _build_pnl_section(leg_e, leg_b, net)
    return (
        f"………\n⚠️ *INVALIDASI: {strategy.value}*\n\n"
        f"Gap melebar melewati batas. Disarankan untuk cut posisi.\n\n"
        f"*Perubahan {lb}:*\n┌─────────────────────\n"
        f"│ BTC: {format_value(btc_ret)}% | ETH: {format_value(eth_ret)}% | Gap: {format_value(gap)}%\n"
        f"└─────────────────────\n{net_section}Bot memulai scan ulang. ⚡"
    )

def build_peak_cancelled_message(strategy, gap):
    return (
        f"………\n❌ *Peak Watch Dibatalkan: {strategy.value}*\n\n"
        f"Gap mundur sebelum terkonfirmasi.\nGap sekarang: *{format_value(gap)}%*\n\nBot tetap memantau."
    )

def build_heartbeat_message() -> str:
    lb          = get_lookback_label()
    btc_str     = f"${float(scan_stats['last_btc_price']):,.2f} ({format_value(scan_stats['last_btc_ret'])}%)" if scan_stats.get("last_btc_price") else "N/A"
    eth_str     = f"${float(scan_stats['last_eth_price']):,.2f} ({format_value(scan_stats['last_eth_ret'])}%)" if scan_stats.get("last_eth_price") else "N/A"
    gap_str     = f"{format_value(scan_stats['last_gap'])}%" if scan_stats.get("last_gap") is not None else "N/A"
    hours_data  = len(price_history) * settings["scan_interval"] / 3600
    data_status = f"✅ {hours_data:.1f}h" if hours_data >= settings["lookback_hours"] else f"⏳ {hours_data:.1f}h"
    peak_str    = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    peak_line   = f"│ Peak: {peak_gap:+.2f}%\n" if current_mode == Mode.PEAK_WATCH and peak_gap is not None else ""

    curr_r, _, _, _, pct_r = calc_ratio_percentile()
    ratio_line = f"ETH/BTC: {curr_r:.5f} | Persentil: {pct_r}th\n" if curr_r and pct_r is not None else ""

    track_section = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et  = settings["exit_threshold"]; sl = settings["sl_pct"]
        tpl = et if active_strategy == Strategy.S1 else -et
        tsl = trailing_gap_best + sl if active_strategy == Strategy.S1 else trailing_gap_best - sl
        gap_now   = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
        _, _, net = calc_net_pnl(active_strategy, gap_now)
        he, hd    = get_pairs_health(active_strategy, gap_now)
        track_section = (
            f"\n*Posisi aktif {active_strategy.value}:*\n"
            f"┌─────────────────────\n"
            f"│ Entry: {entry_gap_value:+.2f}% | Sekarang: {gap_str} | Terbaik: {trailing_gap_best:+.2f}%\n"
            f"│ TP: {tpl:+.2f}% | TSL: {tsl:+.2f}%\n"
            f"│ Net P&L: {net:+.2f}% | {he} {hd}\n"
            f"└─────────────────────\n"
        )

    live_line = ""
    if PARADEX_LIVE_AVAILABLE and is_live_active():
        live_line = f"*🔴 Live:* AKTIF | "

    last_r = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"
    return (
        f"💓 *Bot aktif dan memantau pasar.*\n\n"
        f"Mode: *{current_mode.value}* | Peak: {peak_str}\n"
        f"{live_line}Scan: {scan_stats['count']}x | Sinyal: {scan_stats['signals_sent']}x\n\n"
        f"*Harga & Gap ({lb}):*\n┌─────────────────────\n"
        f"│ BTC: {btc_str}\n│ ETH: {eth_str}\n│ Gap: {gap_str}\n{peak_line}"
        f"└─────────────────────\n{ratio_line}{track_section}\n"
        f"Data: {data_status} | Redis: {last_r} 🔒\n\n"
        f"_Laporan berikutnya dalam {settings['heartbeat_minutes']} menit. ⚡_"
    )

# =============================================================================
# Heartbeat
# =============================================================================
def send_heartbeat() -> bool:
    global scan_stats
    success = send_alert(build_heartbeat_message())
    scan_stats["count"] = 0; scan_stats["signals_sent"] = 0
    return success

def should_send_heartbeat(now: datetime) -> bool:
    if settings["heartbeat_minutes"] == 0 or last_heartbeat_time is None:
        return False
    return (now - last_heartbeat_time).total_seconds() / 60 >= settings["heartbeat_minutes"]

# =============================================================================
# API & Price History
# =============================================================================
def parse_iso_timestamp(ts_str):
    try:
        ts_str = ts_str.replace("Z", "+00:00")
        if "." in ts_str:
            base, rest = ts_str.split(".", 1)
            tz_start = next((i for i, c in enumerate(rest) if c in ("+", "-")), -1)
            if tz_start > 6:
                rest = rest[:6] + rest[tz_start:]
            ts_str = base + "." + rest
        dt = datetime.fromisoformat(ts_str)
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except (ValueError, AttributeError) as e:
        logger.error(f"Gagal parse timestamp '{ts_str}': {e}")
        return None

def fetch_prices():
    url = f"{API_BASE_URL}{API_ENDPOINT}"
    try:
        resp = requests.get(url, timeout=30); resp.raise_for_status()
        data = resp.json()
    except (requests.RequestException, ValueError) as e:
        logger.error(f"API request gagal: {e}")
        return None

    listings = data.get("listings", [])
    btc_data = next((l for l in listings if l.get("ticker", "").upper() == "BTC"), None)
    eth_data = next((l for l in listings if l.get("ticker", "").upper() == "ETH"), None)
    if not btc_data or not eth_data:
        logger.warning("Data BTC atau ETH tidak ditemukan"); return None

    try:
        btc_price = Decimal(btc_data["mark_price"])
        eth_price = Decimal(eth_data["mark_price"])
    except (KeyError, InvalidOperation) as e:
        logger.error(f"Harga tidak valid: {e}"); return None

    btc_upd = parse_iso_timestamp(btc_data.get("quotes", {}).get("updated_at", ""))
    eth_upd = parse_iso_timestamp(eth_data.get("quotes", {}).get("updated_at", ""))
    if not btc_upd or not eth_upd:
        return None
    return PriceData(btc_price, eth_price, btc_upd, eth_upd)

def prune_history(now):
    global price_history
    cutoff = now - timedelta(hours=settings["lookback_hours"], minutes=HISTORY_BUFFER_MINUTES)
    price_history = [p for p in price_history if p.timestamp >= cutoff]

def get_lookback_price(now):
    target    = now - timedelta(hours=settings["lookback_hours"])
    best, diff = None, timedelta(minutes=30)
    for point in price_history:
        d = abs(point.timestamp - target)
        if d < diff:
            diff, best = d, point
    return best

def compute_returns(btc_now, eth_now, btc_prev, eth_prev):
    btc_chg = (btc_now - btc_prev) / btc_prev * Decimal("100")
    eth_chg = (eth_now - eth_prev) / eth_prev * Decimal("100")
    return btc_chg, eth_chg, eth_chg - btc_chg

def is_data_fresh(now, btc_upd, eth_upd):
    threshold = timedelta(minutes=FRESHNESS_THRESHOLD_MINUTES)
    return (now - btc_upd) <= threshold and (now - eth_upd) <= threshold

# =============================================================================
# State Reset
# =============================================================================
def reset_to_scan() -> None:
    global current_mode, active_strategy, entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global entry_btc_ret, entry_eth_ret, entry_driver, exit_confirm_count
    current_mode = Mode.SCAN; active_strategy = None
    entry_gap_value = None; trailing_gap_best = None
    entry_btc_price = None; entry_eth_price = None
    entry_btc_lb = None; entry_eth_lb = None
    entry_btc_ret = None; entry_eth_ret = None
    entry_driver = None; exit_confirm_count = 0

# =============================================================================
# TP + TSL Check
# =============================================================================
def check_sltp(gap_float, btc_ret, eth_ret, gap):
    global trailing_gap_best
    if entry_gap_value is None or active_strategy is None or trailing_gap_best is None:
        return False
    et     = settings["exit_threshold"]
    sl_pct = settings["sl_pct"]

    if active_strategy == Strategy.S1:
        if gap_float < trailing_gap_best:
            trailing_gap_best = gap_float
        tsl_level = trailing_gap_best + sl_pct
        if gap_float <= et:
            eth_target, _ = calc_tp_target_price(Strategy.S1)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, et, eth_target))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return True
        if gap_float >= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return True

    elif active_strategy == Strategy.S2:
        if gap_float > trailing_gap_best:
            trailing_gap_best = gap_float
        tsl_level = trailing_gap_best - sl_pct
        if gap_float >= -et:
            eth_target, _ = calc_tp_target_price(Strategy.S2)
            send_alert(build_tp_message(btc_ret, eth_ret, gap, entry_gap_value, -et, eth_target))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return True
        if gap_float <= tsl_level:
            send_alert(build_trailing_sl_message(btc_ret, eth_ret, gap, entry_gap_value, trailing_gap_best, tsl_level))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return True

    return False

# =============================================================================
# Sinkronisasi pos_data dari Paradex setelah live open
# =============================================================================
def _sync_pos_from_paradex(strategy: str):
    try:
        executor = get_executor()
        if executor is None or not executor.is_ready():
            return
        time.sleep(2.0)
        executor.sync_all()
        eth_pos = executor.get_live_position("ETH-USD-PERP")
        btc_pos = executor.get_live_position("BTC-USD-PERP")
        if eth_pos:
            pos_data["eth_entry_price"] = eth_pos["avg_entry"]
            pos_data["eth_qty"]         = abs(eth_pos["size"]) if eth_pos["side"].upper() == "LONG" else -abs(eth_pos["size"])
            pos_data["eth_leverage"]    = eth_pos.get("leverage", live_settings.get("leverage", 10))
            pos_data["eth_liq_price"]   = eth_pos.get("liq_price")
        if btc_pos:
            pos_data["btc_entry_price"] = btc_pos["avg_entry"]
            pos_data["btc_qty"]         = abs(btc_pos["size"]) if btc_pos["side"].upper() == "LONG" else -abs(btc_pos["size"])
            pos_data["btc_leverage"]    = btc_pos.get("leverage", live_settings.get("leverage", 10))
            pos_data["btc_liq_price"]   = btc_pos.get("liq_price")
        pos_data["strategy"] = strategy
        pos_data["set_at"]   = datetime.now(timezone.utc).isoformat()
        save_pos_data()
        logger.info(f"pos_data disinkronkan dari Paradex: {strategy}")
    except Exception as e:
        logger.warning(f"_sync_pos_from_paradex gagal: {e}")

# =============================================================================
# State Machine
# =============================================================================
def evaluate_and_transition(btc_ret, eth_ret, gap, btc_now, eth_now, btc_lb, eth_lb):
    global current_mode, active_strategy, peak_gap, peak_strategy
    global entry_gap_value, trailing_gap_best
    global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
    global entry_btc_ret, entry_eth_ret, entry_driver

    gap_float      = float(gap)
    entry_thresh   = settings["entry_threshold"]
    exit_thresh    = settings["exit_threshold"]
    invalid_thresh = settings["invalidation_threshold"]
    peak_reversal  = settings["peak_reversal"]
    peak_enabled   = settings["peak_enabled"]

    def do_entry(strategy: Strategy, is_direct: bool = False):
        global current_mode, active_strategy, entry_gap_value, trailing_gap_best
        global entry_btc_price, entry_eth_price, entry_btc_lb, entry_eth_lb
        global entry_btc_ret, entry_eth_ret, entry_driver, peak_gap, peak_strategy

        active_strategy   = strategy
        current_mode      = Mode.TRACK
        entry_gap_value   = gap_float
        trailing_gap_best = gap_float
        entry_btc_price   = btc_now; entry_eth_price = eth_now
        entry_btc_lb      = btc_lb;  entry_eth_lb    = eth_lb
        entry_btc_ret     = float(btc_ret); entry_eth_ret = float(eth_ret)
        drv, _, _         = analyze_gap_driver(float(btc_ret), float(eth_ret), gap_float)
        entry_driver      = drv
        peak              = peak_gap if not is_direct else 0.0
        peak_gap, peak_strategy = None, None

        send_alert(build_entry_message(
            strategy, btc_ret, eth_ret, gap,
            peak, btc_now, eth_now, btc_lb, eth_lb,
            is_direct=is_direct,
        ))

        if PARADEX_LIVE_AVAILABLE and is_live_active():
            exec_msg = live_open_or_sim(strategy.value, float(btc_now), float(eth_now), sim_fn=None)
            if exec_msg:
                send_alert(exec_msg)
            _sync_pos_from_paradex(strategy.value)
        else:
            sim_msg = sim_open_position(strategy, float(btc_now), float(eth_now))
            if sim_msg:
                send_alert(sim_msg)

        scan_stats["signals_sent"] += 1

    # SCAN
    if current_mode == Mode.SCAN:
        if gap_float >= entry_thresh:
            if peak_enabled:
                current_mode = Mode.PEAK_WATCH; peak_strategy = Strategy.S1; peak_gap = gap_float
                send_alert(build_peak_watch_message(Strategy.S1, gap))
            else:
                do_entry(Strategy.S1, is_direct=True)
        elif gap_float <= -entry_thresh:
            if peak_enabled:
                current_mode = Mode.PEAK_WATCH; peak_strategy = Strategy.S2; peak_gap = gap_float
                send_alert(build_peak_watch_message(Strategy.S2, gap))
            else:
                do_entry(Strategy.S2, is_direct=True)
        else:
            logger.debug(f"SCAN: Tidak ada sinyal. Gap: {gap_float:.2f}%")

    # PEAK_WATCH
    elif current_mode == Mode.PEAK_WATCH:
        if peak_strategy == Strategy.S1:
            if gap_float > peak_gap:
                peak_gap = gap_float
            elif gap_float < entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S1, gap))
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
            elif peak_gap - gap_float >= peak_reversal:
                do_entry(Strategy.S1, is_direct=False)
        elif peak_strategy == Strategy.S2:
            if gap_float < peak_gap:
                peak_gap = gap_float
            elif gap_float > -entry_thresh:
                send_alert(build_peak_cancelled_message(Strategy.S2, gap))
                current_mode, peak_gap, peak_strategy = Mode.SCAN, None, None
            elif gap_float - peak_gap >= peak_reversal:
                do_entry(Strategy.S2, is_direct=False)

    # TRACK
    elif current_mode == Mode.TRACK:
        if check_sltp(gap_float, btc_ret, eth_ret, gap):
            return

        confirm_scans  = int(settings["exit_confirm_scans"])
        confirm_buffer = float(settings["exit_confirm_buffer"])
        pnl_gate       = float(settings["exit_pnl_gate"])
        s1_exit_level  = exit_thresh - confirm_buffer
        s2_exit_level  = -exit_thresh + confirm_buffer

        in_exit_zone = (
            (active_strategy == Strategy.S1 and gap_float <= s1_exit_level) or
            (active_strategy == Strategy.S2 and gap_float >= s2_exit_level)
        )

        if in_exit_zone:
            global exit_confirm_count
            exit_confirm_count += 1

            pnl_gate_ok = True
            if pnl_gate > 0 and pos_data.get("eth_entry_price"):
                h = calc_position_pnl()
                if h and h.get("net_pnl_pct", 0) < pnl_gate:
                    pnl_gate_ok = False

            if exit_confirm_count >= max(1, confirm_scans) and pnl_gate_ok:
                confirm_note = ""
                if confirm_scans > 1:
                    confirm_note = f" ✅ Konfirmasi {exit_confirm_count} scan"
                if confirm_buffer > 0:
                    confirm_note += f" | buffer +{confirm_buffer}%"
                send_alert(build_exit_message(btc_ret, eth_ret, gap, confirm_note=confirm_note))

                if PARADEX_LIVE_AVAILABLE and is_live_active():
                    exec_msg = live_close_or_sim(
                        active_strategy.value, float(btc_now), float(eth_now),
                        reason="EXIT", sim_fn=None,
                    )
                    if exec_msg: send_alert(exec_msg)
                else:
                    sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="EXIT")
                    if sim_msg: send_alert(sim_msg)

                logger.info(f"EXIT {active_strategy.value}. Gap: {gap_float:.2f}%")
                reset_to_scan()
                return
            else:
                if exit_confirm_count == 1 and confirm_scans > 1:
                    remaining = max(1, confirm_scans) - exit_confirm_count
                    send_alert(
                        f"⏳ *Pre-exit: Gap menyentuh zona TP*\n"
                        f"Gap: {gap_float:+.2f}% | Menunggu {remaining} scan lagi\n"
                        f"_Menunggu konfirmasi exit..._"
                    )
        else:
            if exit_confirm_count > 0:
                logger.info(f"Zona exit hilang. Reset konfirmasi ({exit_confirm_count}→0).")
                exit_confirm_count = 0

        if active_strategy == Strategy.S1 and gap_float >= invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S1, btc_ret, eth_ret, gap))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(btc_now), float(eth_now), reason="INVALID", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return

        if active_strategy == Strategy.S2 and gap_float <= -invalid_thresh:
            send_alert(build_invalidation_message(Strategy.S2, btc_ret, eth_ret, gap))
            if PARADEX_LIVE_AVAILABLE and is_live_active():
                exec_msg = live_close_or_sim(active_strategy.value, float(btc_now), float(eth_now), reason="INVALID", sim_fn=None)
                if exec_msg: send_alert(exec_msg)
            else:
                sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
                if sim_msg: send_alert(sim_msg)
            reset_to_scan(); return

        logger.debug(f"TRACK {active_strategy.value}: Gap {gap_float:.2f}%")

# =============================================================================
# Command Handlers
# =============================================================================

def handle_settings_command(reply_chat):
    hb     = settings["heartbeat_minutes"]; hb_str = f"{hb} menit" if hb > 0 else "Nonaktif"
    rr     = settings["redis_refresh_minutes"]; rr_str = f"{rr} menit" if rr > 0 else "Nonaktif"
    peak_s = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "Belum diset"
    eth_sr  = settings["eth_size_ratio"]; btc_sr = 100.0 - eth_sr
    ec_s   = int(settings["exit_confirm_scans"]); ec_b = float(settings["exit_confirm_buffer"]); ec_p = float(settings["exit_pnl_gate"])
    live_s     = "🟢 AKTIF" if is_live_active() else "🔴 OFF"
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    mode_s     = "🟣 Mode X" if _live_mode == "x" else "🔵 Normal"
    send_reply(
        f"⚙️ *Konfigurasi Bot Saat Ini*\n\n"
        f"📊 Interval Scan:   {settings['scan_interval']}s\n"
        f"🕐 Lookback:        {settings['lookback_hours']}h\n"
        f"💓 Heartbeat:       {hb_str}\n"
        f"🔄 Redis Refresh:   {rr_str}\n"
        f"📈 Entry:           ±{settings['entry_threshold']}%\n"
        f"📉 Exit/TP:         ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidasi:      ±{settings['invalidation_threshold']}%\n"
        f"🔍 Peak Mode:       {peak_s} ({settings['peak_reversal']}% reversal)\n"
        f"🛑 Trailing SL:     {settings['sl_pct']}%\n"
        f"🛡️ Exit Confirm:    {ec_s}x | buffer {ec_b:.2f}% | P&L gate {ec_p:.2f}%\n"
        f"📐 Rasio Sizing:    ETH {eth_sr:.0f}% / BTC {btc_sr:.0f}%\n"
        f"💰 Modal:           {cap_str}\n"
        f"🔴 Live Trading:    {live_s} | {mode_s}\n\n"
        f"_Ketik `/help` untuk daftar perintah lengkap._",
        reply_chat,
    )

def handle_status_command(reply_chat):
    hours_data = len(price_history) * settings["scan_interval"] / 3600
    lookback   = settings["lookback_hours"]
    ready      = f"✅ {hours_data:.1f}h" if hours_data >= lookback else f"⏳ {hours_data:.1f}h / {lookback}h"
    peak_s     = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    last_r     = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"
    live_s     = "🟢 AKTIF" if is_live_active() else "🔴 OFF"
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    mode_s     = " 🟣 Mode X" if _live_mode == "x" else " 🔵 Normal"

    scan_section = ""
    if current_mode == Mode.SCAN:
        gap_now = scan_stats.get("last_gap"); btc_r = scan_stats.get("last_btc_ret"); eth_r = scan_stats.get("last_eth_ret")
        gap_str = format_value(gap_now) + "%" if gap_now is not None else "N/A"
        driver_line = ""
        if gap_now is not None and btc_r is not None and eth_r is not None:
            drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), float(gap_now))
            driver_line = f"│ Driver: {drv_e} {drv} — _{drv_ex}_\n"
        curr_r, _, _, _, pct_r = calc_ratio_percentile()
        ratio_line = f"│ Rasio: {curr_r:.5f} ({pct_r}th pct)\n" if curr_r and pct_r is not None else ""
        scan_section = (
            f"\n*Gap sekarang ({lookback}h):*\n┌─────────────────────\n"
            f"│ BTC: {format_value(btc_r)}% | ETH: {format_value(eth_r)}%\n"
            f"│ Gap: *{gap_str}* (threshold ±{settings['entry_threshold']}%)\n"
            f"{driver_line}{ratio_line}└─────────────────────\n"
        )

    track_section = ""
    if current_mode == Mode.TRACK and entry_gap_value is not None and trailing_gap_best is not None:
        et  = settings["exit_threshold"]; sl = settings["sl_pct"]
        tpl = et if active_strategy == Strategy.S1 else -et
        tsl = trailing_gap_best + sl if active_strategy == Strategy.S1 else trailing_gap_best - sl
        gap_now  = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
        _, _, net = calc_net_pnl(active_strategy, gap_now)
        he, hd = get_pairs_health(active_strategy, gap_now)
        track_section = (
            f"\n*Posisi aktif {active_strategy.value}:*\n┌─────────────────────\n"
            f"│ Entry: {entry_gap_value:+.2f}% | Sekarang: {format_value(scan_stats['last_gap'])}% | Terbaik: {trailing_gap_best:+.2f}%\n"
            f"│ TP: {tpl:+.2f}% | TSL: {tsl:+.2f}%\n"
            f"│ Net P&L: {net:+.2f}% | {he} {hd}\n└─────────────────────\n"
        )

    send_reply(
        f"📊 *Status Bot*\n\nMode: *{current_mode.value}* | Peak: {peak_s} | Live: {live_s}{mode_s if is_live_active() else ''}\n"
        f"{scan_section}{track_section}Data: {ready} | Redis: {last_r} 🔒\n",
        reply_chat,
    )

def handle_pnl_command(reply_chat):
    if current_mode != Mode.TRACK or active_strategy is None or entry_gap_value is None:
        send_reply("Belum ada posisi aktif saat ini.\nBot masih dalam mode SCAN.", reply_chat)
        return
    gap_now = float(scan_stats["last_gap"]) if scan_stats.get("last_gap") is not None else entry_gap_value
    leg_e, leg_b, net = calc_net_pnl(active_strategy, gap_now)
    he, hd = get_pairs_health(active_strategy, gap_now)
    capital = settings["capital"]
    if leg_e is not None and leg_b is not None and capital > 0:
        half = capital / 2.0
        _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
        if _live_mode == "x":
            # Mode X: keduanya searah
            if active_strategy == Strategy.S1:
                pnl_body = (f"│ Short ETH: {leg_e:+.2f}% (${leg_e/100*half:+.2f})\n"
                            f"│ Short BTC: {leg_b:+.2f}% (${leg_b/100*half:+.2f})\n"
                            f"│ *Net: {net:+.2f}% (${(leg_e+leg_b)/100*half:+.2f})* 🟣\n")
            else:
                pnl_body = (f"│ Long ETH: {leg_e:+.2f}% (${leg_e/100*half:+.2f})\n"
                            f"│ Long BTC: {leg_b:+.2f}% (${leg_b/100*half:+.2f})\n"
                            f"│ *Net: {net:+.2f}% (${(leg_e+leg_b)/100*half:+.2f})* 🟣\n")
        else:
            if active_strategy == Strategy.S1:
                pnl_body = (f"│ Long BTC: {leg_b:+.2f}% (${leg_b/100*half:+.2f})\n"
                            f"│ Short ETH: {leg_e:+.2f}% (${leg_e/100*half:+.2f})\n"
                            f"│ *Net: {net:+.2f}% (${(leg_e+leg_b)/100*half:+.2f})*\n")
            else:
                pnl_body = (f"│ Long ETH: {leg_e:+.2f}% (${leg_e/100*half:+.2f})\n"
                            f"│ Short BTC: {leg_b:+.2f}% (${leg_b/100*half:+.2f})\n"
                            f"│ *Net: {net:+.2f}% (${(leg_e+leg_b)/100*half:+.2f})*\n")
    else:
        pnl_body = f"│ Pergerakan gap net: *{net:+.2f}%*\n" if net else "│ Data tidak cukup\n"

    send_reply(
        f"📊 *Net P&L — Pairs Trade*\nPosisi: *{active_strategy.value}* | Entry: {entry_gap_value:+.2f}%\n"
        f"Gap sekarang: *{format_value(scan_stats['last_gap'])}%*\n\n"
        f"*Estimasi P&L:*\n┌─────────────────────\n{pnl_body}└─────────────────────\n\n"
        f"*Kesehatan:* {he} {hd}\n\n_💡 Dalam pairs trade, nilai NET yang paling penting._",
        reply_chat,
    )

def handle_ratio_command(reply_chat):
    curr_r, avg_r, hi_r, lo_r, pct_r = calc_ratio_percentile()
    if curr_r is None:
        send_reply(f"Data belum mencukupi. Saat ini: {len(price_history)} titik data.", reply_chat)
        return
    window = settings["ratio_window_days"]
    stars_s1, _ = get_ratio_conviction(Strategy.S1, pct_r)
    stars_s2, _ = get_ratio_conviction(Strategy.S2, pct_r)
    ext      = _calc_ratio_extended_stats(curr_r, avg_r, hi_r, lo_r, pct_r)
    if pct_r <= 20:   signal = "🟢 *ETH sangat murah vs BTC* — momentum S2 kuat"
    elif pct_r <= 40: signal = "🟡 *ETH relatif murah* — setup S2 cukup bagus"
    elif pct_r >= 80: signal = "🔴 *ETH sangat mahal vs BTC* — momentum S1 kuat"
    elif pct_r >= 60: signal = "🟠 *ETH relatif mahal* — setup S1 cukup bagus"
    else:             signal = "⚪ *Netral* — ETH di area tengah vs BTC"
    bar_pos  = min(10, int(pct_r / 10))
    bar      = "─" * bar_pos + "●" + "─" * (10 - bar_pos)
    detail_s1 = _build_conviction_detail(Strategy.S1, stars_s1, pct_r, curr_r, avg_r, hi_r, lo_r, ext)
    detail_s2 = _build_conviction_detail(Strategy.S2, stars_s2, pct_r, curr_r, avg_r, hi_r, lo_r, ext)
    ready_s1  = build_entry_readiness(Strategy.S1, pct_r, curr_r, avg_r, ext)
    ready_s2  = build_entry_readiness(Strategy.S2, pct_r, curr_r, avg_r, ext)
    send_reply(
        f"📈 *Monitor Rasio ETH/BTC*\n\n"
        f"┌─────────────────────\n│ Sekarang: {curr_r:.5f}\n│ Rata-rata {window}d: {avg_r:.5f}\n"
        f"│ Tertinggi {window}d: {hi_r:.5f}\n│ Terendah {window}d: {lo_r:.5f}\n│ Persentil: *{pct_r}th*\n└─────────────────────\n\n"
        f"`[lo]─{bar}─[hi]`\n_{signal}_\n\n"
        f"──────────────────────\n{detail_s1}\n\n──────────────────────\n{detail_s2}\n\n"
        f"══════════════════════\n{ready_s1}\n══════════════════════\n{ready_s2}\n\n"
        f"_Berdasarkan {len(price_history)} titik data._",
        reply_chat,
    )

def handle_analysis_command(reply_chat):
    gap_now = scan_stats.get("last_gap"); btc_r = scan_stats.get("last_btc_ret"); eth_r = scan_stats.get("last_eth_ret")
    if gap_now is None:
        send_reply("Data harga belum tersedia.", reply_chat); return
    lb = get_lookback_label(); gap_f = float(gap_now); et = settings["entry_threshold"]
    drv, drv_e, drv_ex = analyze_gap_driver(float(btc_r), float(eth_r), gap_f)
    reg = detect_market_regime()
    def _pct(v): return f"{v:+.2f}%" if v is not None else "N/A"
    reg_block = (
        f"*🌍 Regime Pasar:*\n┌─────────────────────\n"
        f"│ {reg['emoji']} *{reg['regime']}* — {reg['strength']}\n│ _{reg['description']}_\n│\n"
        f"│         BTC        ETH\n│ 1j: {_pct(reg['btc_1h']):>9}  {_pct(reg['eth_1h'])}\n"
        f"│ 4j: {_pct(reg['btc_4h']):>9}  {_pct(reg['eth_4h'])}\n│ 24j:{_pct(reg['btc_24h']):>9}  {_pct(reg['eth_24h'])}\n"
        f"│ Volatilitas: {reg['volatility']}\n└─────────────────────\n_{reg['implications']}_\n"
    )
    curr_r, _, _, _, pct_r = calc_ratio_percentile()
    ratio_str = f"{curr_r:.5f} ({pct_r}th pct)" if curr_r else "N/A"
    gap_abs = abs(gap_f)
    if gap_abs < et * 0.5:  gap_status = "💤 Jauh dari threshold"
    elif gap_abs < et:       gap_status = f"🔔 Mendekati ±{et}%"
    elif gap_abs < et * 1.5: gap_status = f"🚨 Zona entry"
    else:                    gap_status = f"⚡ Divergence ekstrem"
    if gap_f >= et:
        cand_str = f"🔍 Kandidat *S1*"; stars, cv = get_ratio_conviction(Strategy.S1, pct_r)
    elif gap_f <= -et:
        cand_str = f"🔍 Kandidat *S2*"; stars, cv = get_ratio_conviction(Strategy.S2, pct_r)
    else:
        cand_str = "💤 Belum ada kandidat"; stars, cv = "—", "—"
    send_reply(
        f"🧠 *Analisis Pasar Lengkap*\n\n{reg_block}\n"
        f"*📊 Gap ({lb}):*\n┌─────────────────────\n│ BTC: {format_value(btc_r)}% | ETH: {format_value(eth_r)}%\n"
        f"│ Gap: *{format_value(gap_now)}%* | Driver: {drv_e} *{drv}*\n└─────────────────────\n{gap_status}\n\n"
        f"*📈 Rasio ETH/BTC:* {ratio_str}\n\n*Setup:*\n{cand_str}\nConviction: {stars} — _{cv}_\n\n"
        f"Mode: *{current_mode.value}* | Live: {'🟢 AKTIF' if is_live_active() else '🔴 OFF'}\n"
        f"_Gunakan `/ratio` untuk detail, `/pnl` untuk P&L._",
        reply_chat,
    )

def handle_capital_command(args, reply_chat):
    if not args:
        cap = settings["capital"]
        send_reply(f"💰 *Modal:* ${cap:,.0f}\nPenggunaan: `/capital <jumlah USD>`", reply_chat)
        return
    try:
        val = float(args[0])
        if val < 0 or val > 10_000_000:
            send_reply("Modal harus antara $0 – $10,000,000.", reply_chat); return
        settings["capital"] = val
        save_settings()
        send_reply(f"💰 Modal *${val:,.0f}* berhasil disimpan.", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_sizeratio_command(args, reply_chat):
    curr_eth = settings["eth_size_ratio"]; curr_btc = 100.0 - curr_eth
    if not args:
        send_reply(
            f"📐 *Rasio Sizing*\nLeg ETH: *{curr_eth:.0f}%* | Leg BTC: *{curr_btc:.0f}%*\n"
            f"Penggunaan: `/sizeratio <eth_pct>` — contoh: `/sizeratio 60`",
            reply_chat,
        ); return
    try:
        val = float(args[0])
        if not (10.0 <= val <= 90.0):
            send_reply("Rentang harus 10–90%.", reply_chat); return
        settings["eth_size_ratio"] = val
        send_reply(f"✅ Leg ETH: *{val:.0f}%* / Leg BTC: *{100-val:.0f}%*", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_peak_command(args, reply_chat):
    peak_s = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    if not args:
        send_reply(f"🔍 *Peak Watch:* {peak_s} | Reversal: *{settings['peak_reversal']}%*\nPenggunaan: `/peak on|off|<nilai reversal>`", reply_chat)
        return
    first = args[0].lower()
    if first == "on":
        settings["peak_enabled"] = True
        send_reply("✅ *Peak Watch diaktifkan.*", reply_chat)
    elif first == "off":
        _cancel_peak_watch_if_active(reply_chat)
        settings["peak_enabled"] = False
        send_reply("❌ *Peak Watch dinonaktifkan.*", reply_chat)
    else:
        try:
            val = float(first)
            if val <= 0 or val > 3.0:
                send_reply("Harus antara 0 – 3.0.", reply_chat); return
            settings["peak_reversal"] = val
            send_reply(f"Reversal diatur ke *{val}%*.", reply_chat)
        except ValueError:
            send_reply("Gunakan `on`, `off`, atau nilai reversal.", reply_chat)

def _cancel_peak_watch_if_active(reply_chat=None):
    global current_mode, peak_gap, peak_strategy
    if current_mode == Mode.PEAK_WATCH and peak_strategy is not None:
        if reply_chat:
            send_reply(f"⚠️ Peak Watch *{peak_strategy.value}* dibatalkan.\nKembali ke mode SCAN.", reply_chat)
        current_mode = Mode.SCAN; peak_gap = None; peak_strategy = None

def handle_sltp_command(args, reply_chat):
    if not args:
        send_reply(f"🛑 *Trailing SL:* {settings['sl_pct']}%\nPenggunaan: `/sltp sl <nilai>`", reply_chat)
        return
    if len(args) < 2:
        send_reply("Penggunaan: `/sltp sl <nilai>`.", reply_chat); return
    try:
        key, val = args[0].lower(), float(args[1])
        if val <= 0 or val > 10:
            send_reply("Harus antara 0 – 10.", reply_chat); return
        if key == "sl":
            settings["sl_pct"] = val
            save_settings()
            send_reply(f"✅ Trailing SL: *{val}%*\n_Tersimpan._", reply_chat)
        else:
            send_reply("Gunakan `sl`.", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_interval_command(args, reply_chat):
    if not args:
        send_reply(f"Interval: *{settings['scan_interval']}s*\nPenggunaan: `/interval <60-3600>`", reply_chat); return
    try:
        val = int(args[0])
        if val < 60 or val > 3600:
            send_reply("Harus antara 60 – 3600 detik.", reply_chat); return
        settings["scan_interval"] = val
        send_reply(f"Scan setiap *{val}s*.", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_threshold_command(args, reply_chat):
    if len(args) < 2:
        send_reply("Penggunaan:\n`/threshold entry <val>`\n`/threshold exit <val>`\n`/threshold invalid <val>`", reply_chat); return
    try:
        t_type, val = args[0].lower(), float(args[1])
        if val <= 0 or val > 20:
            send_reply("Harus antara 0 – 20.", reply_chat); return
        if t_type == "entry":
            settings["entry_threshold"] = val
            save_settings()
            send_reply(f"✅ Entry threshold: ±{val}%\n_Tersimpan — akan digunakan kembali saat bot restart._", reply_chat)
        elif t_type == "exit":
            settings["exit_threshold"] = val
            save_settings()
            send_reply(f"✅ Exit/TP threshold: ±{val}%\n_Tersimpan._", reply_chat)
        elif t_type in ("invalid", "invalidation"):
            settings["invalidation_threshold"] = val
            save_settings()
            send_reply(f"✅ Invalidasi threshold: ±{val}%\n_Tersimpan._", reply_chat)
        else:
            send_reply("Gunakan `entry`, `exit`, atau `invalid`.", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_lookback_command(args, reply_chat):
    if not args:
        send_reply(f"Lookback: *{settings['lookback_hours']}h*\nPenggunaan: `/lookback <1-24>`", reply_chat); return
    try:
        val = int(args[0])
        if val < 1 or val > 24:
            send_reply("Harus antara 1 – 24 jam.", reply_chat); return
        settings["lookback_hours"] = val
        save_settings()
        prune_history(datetime.now(timezone.utc))
        send_reply(f"Lookback diatur ke *{val}h*\n_Tersimpan._", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_heartbeat_command(args, reply_chat):
    if not args:
        send_reply(f"Heartbeat: *{settings['heartbeat_minutes']} menit*\nPenggunaan: `/heartbeat <0-120>`", reply_chat); return
    try:
        val = int(args[0])
        if val < 0 or val > 120:
            send_reply("Harus antara 0 – 120 menit.", reply_chat); return
        settings["heartbeat_minutes"] = val
        settings["heartbeat_minutes"] = val
        save_settings()
        send_reply("Heartbeat *dinonaktifkan*." if val == 0 else f"Heartbeat setiap *{val} menit*. _Tersimpan._", reply_chat)
    except ValueError:
        send_reply("Angka tidak valid.", reply_chat)

def handle_redis_command(reply_chat):
    if not UPSTASH_REDIS_URL:
        send_reply("Redis belum dikonfigurasi.", reply_chat); return
    result = _redis_request("GET", f"/get/{REDIS_KEY}")
    if not result or result.get("result") is None:
        send_reply("❌ Bot A belum menyimpan data.", reply_chat); return
    try:
        data = json.loads(result["result"])
        hrs_stored = len(data) * settings["scan_interval"] / 3600
        lookback   = settings["lookback_hours"]
        status     = "✅ Siap" if hrs_stored >= lookback else f"⏳ {hrs_stored:.1f}h / {lookback}h"
        last_r     = last_redis_refresh.strftime("%H:%M UTC") if last_redis_refresh else "Belum"
        send_reply(f"⚡ *Status Redis*\nData: {len(data)} titik | {hrs_stored:.1f}h | {status}\nTerakhir refresh: {last_r}", reply_chat)
    except Exception as e:
        send_reply(f"Gagal membaca: `{e}`", reply_chat)

def handle_setpos_command(args, reply_chat):
    usage = (
        "*Penggunaan:*\n`/setpos <S1|S2> eth <entry> <qty> <lev>x btc <entry> <qty> <lev>x`\n\n"
        "qty *positif* = Long | qty *negatif* = Short\n\n"
        "*S1:* `/setpos S1 eth 2011.56 -1.4907 50x btc 67794.76 0.029491 50x`\n"
        "*S2:* `/setpos S2 eth 1956.40 15.58 10x btc 67586.10 -0.4439 10x`"
    )
    if len(args) < 9:
        send_reply(usage, reply_chat); return
    try:
        strat_str = args[0].upper()
        if strat_str not in ("S1", "S2"):
            send_reply("Strategi harus *S1* atau *S2*.", reply_chat); return
        if args[1].lower() != "eth" or args[5].lower() != "btc":
            send_reply(usage, reply_chat); return
        def _pf(s): return float(s.replace(",", ""))
        eth_entry = _pf(args[2]); eth_qty = _pf(args[3]); eth_lev = _pf(args[4].lower().replace("x", ""))
        btc_entry = _pf(args[6]); btc_qty = _pf(args[7]); btc_lev = _pf(args[8].lower().replace("x", ""))
        if eth_entry <= 0 or btc_entry <= 0:
            send_reply("Harga entry harus positif.", reply_chat); return
        if not (1 <= eth_lev <= 200) or not (1 <= btc_lev <= 200):
            send_reply("Leverage harus antara 1x – 200x.", reply_chat); return
        now_iso = datetime.now(timezone.utc).isoformat()
        pos_data.update({
            "eth_entry_price": eth_entry, "eth_qty": eth_qty, "eth_notional_usd": None,
            "eth_leverage": eth_lev, "eth_liq_price": None,
            "btc_entry_price": btc_entry, "btc_qty": btc_qty, "btc_notional_usd": None,
            "btc_leverage": btc_lev, "btc_liq_price": None,
            "strategy": strat_str, "set_at": now_iso,
        })
        saved  = save_pos_data()
        sv_str = "✅ Tersimpan ke Redis" if saved else "⚠️ Redis tidak tersedia"
        eth_dir = "Long 📈" if eth_qty > 0 else "Short 📉"
        btc_dir = "Long 📈" if btc_qty > 0 else "Short 📉"
        send_reply(
            f"✅ *Posisi {strat_str} berhasil disimpan.*\n\n"
            f"ETH: *{eth_dir}* {abs(eth_qty):.4f} @ ${eth_entry:,.2f} | {eth_lev:.0f}x\n"
            f"BTC: *{btc_dir}* {abs(btc_qty):.6f} @ ${btc_entry:,.2f} | {btc_lev:.0f}x\n\n{sv_str}\n"
            f"Ketik `/health` untuk memantau.",
            reply_chat,
        )
    except (ValueError, IndexError) as e:
        send_reply(f"Format salah.\n\n{usage}", reply_chat)

def handle_setfunding_command(args, reply_chat):
    usage = "*Penggunaan:*\n`/setfunding eth <rate%> btc <rate%>`\nContoh: `/setfunding eth 0.0100 btc 0.0080`"
    if len(args) < 4:
        send_reply(usage, reply_chat); return
    try:
        def _pf(s): return float(s.replace(",", ""))
        if args[0].lower() != "eth" or args[2].lower() != "btc":
            send_reply(usage, reply_chat); return
        eth_fr = _pf(args[1]); btc_fr = _pf(args[3])
        pos_data["eth_funding_rate"] = eth_fr; pos_data["btc_funding_rate"] = btc_fr
        save_pos_data()
        send_reply(f"✅ *Funding rate berhasil disimpan.*\nETH: {eth_fr:+.4f}%/8h | BTC: {btc_fr:+.4f}%/8h\nKetik `/health` untuk melihat break-even timer.", reply_chat)
    except (ValueError, IndexError) as e:
        send_reply(f"Format salah.\n\n{usage}", reply_chat)

def handle_velocity_command(reply_chat):
    vel = calc_gap_velocity()
    if not vel:
        send_reply(f"Data belum cukup. Saat ini: {len(gap_history)} titik | Minimal 3 titik.", reply_chat); return
    curr_gap = vel["curr_gap"]
    d15 = vel.get("delta_15m"); d30 = vel.get("delta_30m"); d60 = vel.get("delta_60m")
    eta = vel.get("eta_min"); accel = vel.get("accel")
    def _ds(v): return f"{v:+.3f}%" if v is not None else "N/A"
    conv_15  = d15 is not None and abs(curr_gap + d15) < abs(curr_gap)
    trend_e  = "⬇️ konvergen" if conv_15 else "⬆️ melebar"
    momentum = ("📈 *makin cepat*" if accel > 1.2 else "📉 *makin lambat*" if accel < 0.8 else "➡️ *stabil*") if accel is not None else "N/A"
    eta_s    = f"~{int(eta)}m" if eta is not None and eta < 10000 else "tidak dapat dihitung"
    send_reply(
        f"📡 *Monitor Kecepatan Gap*\n\n┌─────────────────────\n│ Gap: *{curr_gap:+.3f}%*\n"
        f"│ Δ15m: {_ds(d15)} {trend_e} | Δ30m: {_ds(d30)} | Δ60m: {_ds(d60)}\n└─────────────────────\n\n"
        f"*Momentum:* {momentum} | *Estimasi TP:* {eta_s}\n_Berdasarkan {vel['n_pts']} titik data._",
        reply_chat,
    )

def handle_exitconf_command(args, reply_chat):
    conf_s = int(settings["exit_confirm_scans"]); conf_b = float(settings["exit_confirm_buffer"]); pnl_g = float(settings["exit_pnl_gate"])
    if not args or args[0].lower() == "show":
        send_reply(
            f"*🛡️ Konfirmasi Exit*\n\n│ Scan: *{conf_s}x* | Buffer: *{conf_b:.2f}%* | P&L gate: *{pnl_g:.2f}%*\n\n"
            f"*Perintah:*\n`/exitconf scans 3` | `/exitconf buffer 0.3` | `/exitconf pnl 0.5` | `/exitconf off`",
            reply_chat,
        ); return
    if args[0].lower() == "off":
        settings["exit_confirm_scans"] = 0; settings["exit_confirm_buffer"] = 0.0; settings["exit_pnl_gate"] = 0.0
        send_reply("⚡ *Konfirmasi exit dinonaktifkan.*", reply_chat); return
    if len(args) < 2:
        send_reply("Penggunaan: `/exitconf scans|buffer|pnl <nilai>` atau `/exitconf off`.", reply_chat); return
    try:
        key = args[0].lower(); val = float(args[1].replace(",", ""))
        if key == "scans":   settings["exit_confirm_scans"] = max(0, int(val)); send_reply(f"✅ Konfirmasi scan: *{int(val)}x*.", reply_chat)
        elif key == "buffer":settings["exit_confirm_buffer"] = max(0.0, val); send_reply(f"✅ Exit buffer: *{val:.2f}%*.", reply_chat)
        elif key in ("pnl", "pnlgate"): settings["exit_pnl_gate"] = max(0.0, val); send_reply(f"✅ P&L gate: *{val:.2f}%*.", reply_chat)
        else: send_reply("Gunakan: `scans`, `buffer`, atau `pnl`.", reply_chat)
    except (ValueError, IndexError):
        send_reply("Format salah. Contoh: `/exitconf scans 2`", reply_chat)

def handle_health_command(reply_chat):
    if pos_data["eth_entry_price"] is None:
        send_reply(
            "Belum ada posisi yang terdaftar.\n\nSilakan gunakan `/setpos` terlebih dahulu.\n\n"
            "*Contoh S1:* `/setpos S1 eth 2011.56 -1.4907 50x btc 67794.76 0.029491 50x`",
            reply_chat,
        ); return
    if scan_stats.get("last_btc_price") is None:
        send_reply("Mohon tunggu sebentar. Bot belum mendapatkan harga terbaru.", reply_chat); return
    h = calc_position_pnl()
    if not h:
        send_reply("Kalkulasi P&L gagal. Silakan coba `/setpos` ulang.", reply_chat); return
    send_reply(build_position_health_message(h), reply_chat)

def handle_clearpos_command(reply_chat):
    for k in pos_data: pos_data[k] = None
    clear_pos_data_redis()
    send_reply("🗑️ *Data posisi berhasil dihapus.*", reply_chat)

def handle_sim_command(args, reply_chat):
    enabled = settings["sim_enabled"]; margin = settings["sim_margin_usd"]
    lev = settings["sim_leverage"]; fee = settings["sim_fee_pct"]
    active = sim_trade["active"]; n_trades = len(sim_trade["history"])

    if not args or args[0].lower() == "status":
        sim_state = "🟢 AKTIF" if enabled else "🔴 NONAKTIF"
        pos_state = f"📍 {sim_trade['strategy']}" if active else "💤 Tidak ada posisi"
        regime_s  = "✅ ON" if settings["sim_regime_filter"] else "❌ OFF"
        send_reply(
            f"🤖 *Mode Simulasi*\n\nStatus: {sim_state}\nPosisi: {pos_state}\n"
            f"Margin: ${margin:,.0f} | Lev: {lev:.0f}x | Fee: {fee:.3f}%\nTotal trade: {n_trades}\nFilter regime: {regime_s}\n\n"
            f"`/sim on|off` | `/sim margin <usd>` | `/sim lev <n>` | `/sim regime on|off` | `/sim reset`",
            reply_chat,
        ); return

    cmd = args[0].lower()
    if cmd == "on":
        settings["sim_enabled"] = True
        send_reply(f"🟢 *Mode simulasi AKTIF.* Margin: ${margin:,.0f} | Lev: {lev:.0f}x", reply_chat)
    elif cmd == "off":
        settings["sim_enabled"] = False
        send_reply("🔴 *Mode simulasi NONAKTIF.*", reply_chat)
    elif cmd == "margin":
        try:
            val = float(args[1])
            if val <= 0 or val > 100000: send_reply("Margin $1 – $100,000.", reply_chat); return
            settings["sim_margin_usd"] = val
            send_reply(f"✅ Margin per leg: *${val:,.0f}*.", reply_chat)
        except (IndexError, ValueError): send_reply("Penggunaan: `/sim margin <usd>`.", reply_chat)
    elif cmd == "lev":
        try:
            val = float(args[1])
            if not 1 <= val <= 200: send_reply("Leverage 1x – 200x.", reply_chat); return
            settings["sim_leverage"] = val
            send_reply(f"✅ Leverage: *{val:.0f}x*.", reply_chat)
        except (IndexError, ValueError): send_reply("Penggunaan: `/sim lev <n>`.", reply_chat)
    elif cmd == "fee":
        try:
            settings["sim_fee_pct"] = float(args[1])
            send_reply(f"✅ Fee: *{settings['sim_fee_pct']:.4f}%*.", reply_chat)
        except (IndexError, ValueError): send_reply("Penggunaan: `/sim fee <pct>`.", reply_chat)
    elif cmd == "reset":
        sim_trade["history"].clear(); send_reply("✅ History trade direset.", reply_chat)
    elif cmd == "regime":
        sub = args[1].lower() if len(args) > 1 else ""
        if sub == "on":
            settings["sim_regime_filter"] = True; send_reply("✅ *Filter regime AKTIF.*", reply_chat)
        elif sub == "off":
            settings["sim_regime_filter"] = False; send_reply("⚠️ *Filter regime NONAKTIF.*", reply_chat)
        else:
            send_reply(f"Filter regime: {'✅ ON' if settings['sim_regime_filter'] else '❌ OFF'}\n`/sim regime on|off`", reply_chat)
    else:
        send_reply("Perintah tidak dikenal. Ketik `/sim` untuk daftar perintah.", reply_chat)

def handle_simstats_command(reply_chat):
    history = sim_trade["history"]
    active_str = ""
    if sim_trade["active"]:
        h = calc_position_pnl()
        if h:
            s = "+" if h["net_pnl"] >= 0 else ""
            active_str = f"📍 *Posisi: {sim_trade['strategy']}* | Net: *{s}${h['net_pnl']:.2f}*\n\n"

    if not history:
        send_reply(f"{active_str}📊 *Statistik Simulasi*\n\nBelum ada trade yang selesai.", reply_chat); return

    total = len(history); wins = [t for t in history if t["net_pnl"] >= 0]; losses = [t for t in history if t["net_pnl"] < 0]
    total_net = sum(t["net_pnl"] for t in history); total_fee = sum(t["fee"] for t in history)
    win_rate  = len(wins) / total * 100
    avg_win   = sum(t["net_pnl"] for t in wins) / len(wins) if wins else 0
    avg_loss  = sum(t["net_pnl"] for t in losses) / len(losses) if losses else 0
    best = max(history, key=lambda t: t["net_pnl"]); worst = min(history, key=lambda t: t["net_pnl"])
    rr   = abs(avg_win / avg_loss) if avg_loss != 0 else 0
    sign = "+" if total_net >= 0 else ""; emoji = "🟢" if total_net >= 0 else "🔴"
    recent = history[-5:]
    recent_lines = "".join(
        f"│ {'✅' if t['net_pnl']>=0 else '❌'} {t['strategy']} {t['reason']}: "
        f"{'+' if t['net_pnl']>=0 else ''}${t['net_pnl']:.2f} {t['duration']}\n"
        for t in reversed(recent)
    )
    send_reply(
        f"{active_str}📊 *Statistik Simulasi — {total} trade*\n"
        f"┌─────────────────────\n│ {emoji} *Net: {sign}${total_net:.2f}* | Fee: -${total_fee:.3f}\n"
        f"│ {len(wins)}W/{len(losses)}L ({win_rate:.0f}%) | R:R 1:{rr:.2f}\n"
        f"│ Terbaik: +${best['net_pnl']:.2f} | Terburuk: ${worst['net_pnl']:.2f}\n"
        f"├─────────────────────\n│ *5 Trade Terakhir:*\n{recent_lines}└─────────────────────\n",
        reply_chat,
    )

def handle_pdx_close_with_reset(args: list, chat_id: str):
    """
    /pdx close — tutup semua posisi Paradex DAN reset state bot ke SCAN.
    Ini penting supaya bot bisa scan sinyal baru setelah posisi ditutup manual.
    """
    from paradex_live import live_close_all_command

    # Step 1: Tutup posisi di Paradex
    close_msg = live_close_all_command()
    send_reply(close_msg, chat_id)

    # Step 2: Reset state bot ke SCAN
    was_tracking = current_mode == Mode.TRACK
    was_strategy = active_strategy.value if active_strategy else None
    reset_to_scan()

    # Step 3: Bersihkan pos_data
    for k in pos_data:
        pos_data[k] = None
    clear_pos_data_redis()

    # Step 4: Bersihkan sim_trade jika aktif
    if sim_trade.get("active"):
        sim_trade.update({
            "active": False, "strategy": None, "eth_entry": None, "btc_entry": None,
            "eth_qty": None, "btc_qty": None, "eth_notional": None, "btc_notional": None,
            "eth_margin": None, "btc_margin": None, "fee_open": None, "opened_at": None,
        })

    if was_tracking:
        send_reply(
            f"🔄 *State bot direset ke SCAN.*\n"
            f"Strategi {was_strategy} dihapus dari memory.\n"
            f"Bot siap mendeteksi sinyal baru.",
            chat_id,
        )
        logger.info(f"State bot direset ke SCAN setelah /pdx close (was {was_strategy})")
    else:
        send_reply("ℹ️ Bot sudah dalam mode SCAN — tidak ada state yang perlu direset.", chat_id)


def handle_forceclose_command(chat_id: str):
    """
    /forceclose atau /resetstate — reset state bot ke SCAN tanpa menutup posisi Paradex.
    Berguna jika posisi sudah ditutup manual di UI tapi state bot masih TRACK.
    """
    was_mode     = current_mode.value
    was_strategy = active_strategy.value if active_strategy else None

    reset_to_scan()
    for k in pos_data:
        pos_data[k] = None
    clear_pos_data_redis()

    if sim_trade.get("active"):
        sim_trade.update({
            "active": False, "strategy": None, "eth_entry": None, "btc_entry": None,
            "eth_qty": None, "btc_qty": None, "eth_notional": None, "btc_notional": None,
            "eth_margin": None, "btc_margin": None, "fee_open": None, "opened_at": None,
        })

    logger.info(f"State bot di-force reset: {was_mode} {was_strategy} → SCAN")
    send_reply(
        f"🔄 *State Bot Direset ke SCAN*\n\n"
        f"Mode sebelumnya: *{was_mode}*\n"
        f"Strategi: *{was_strategy or '-'}*\n\n"
        f"✅ Bot sekarang dalam mode SCAN dan siap mendeteksi sinyal baru.\n\n"
        f"_⚠️ Catatan: command ini hanya mereset state bot, tidak menutup posisi di Paradex._\n"
        f"_Gunakan `/pdx close` untuk menutup posisi sekaligus._",
        chat_id,
    )



def handle_help_command(reply_chat):
    enabled = settings["sim_enabled"]; n_trade = len(sim_trade["history"]); active = sim_trade["active"]
    gap_s = f"{float(scan_stats['last_gap']):+.2f}%" if scan_stats.get("last_gap") is not None else "—"
    sim_s = "🟢 ON" if enabled else "🔴 OFF"
    live_s = "🟢 AKTIF" if is_live_active() else "🔴 OFF"

    send_reply(
        f"🤖 *Monk Bot B — BTC/ETH Divergence*\n"
        f"_Gap: {gap_s} | Sim: {sim_s} | Live: {live_s} | Trade: {n_trade}_\n\n"
        f"*— Pasar & Status —*\n"
        f"`/status`           — gap & mode saat ini\n"
        f"`/analysis`         — snapshot pasar lengkap\n"
        f"`/ratio`            — detail rasio ETH/BTC\n"
        f"`/pnl`              — P&L posisi aktif\n"
        f"`/velocity`         — kecepatan gap\n\n"
        f"*— Simulasi —*\n"
        f"`/sim on|off`       — aktifkan/nonaktifkan simulasi\n"
        f"`/sim margin <usd>` — modal per leg\n"
        f"`/sim lev <n>`      — leverage\n"
        f"`/simstats`         — rekap statistik trade\n\n"
        f"*— 🔴 Paradex Live Trading —*\n"
        f"`/pdx`                    — status koneksi Paradex\n"
        f"`/pdx init l2 <key> <addr>` — hubungkan akun\n"
        f"`/pdx balance`            — saldo & ekuitas\n"
        f"`/pdx fills`              — riwayat transaksi\n"
        f"`/pdx sync`               — perbarui data posisi\n"
        f"`/pdx cancel`             — batalkan pending orders\n"
        f"`/pdx close`              — *tutup semua posisi*\n"
        f"`/pdx lev [nilai]`        — cek / atur leverage\n"
        f"`/pdx preview <btc> <eth>` — preview ukuran order\n"
        f"`/live`                   — status & pengaturan live\n"
        f"`/live on|off`            — aktifkan/nonaktifkan live\n"
        f"`/live mode normal|x`     — *ganti mode trading*\n"
        f"`/live margin <usd>`      — set margin per pair\n"
        f"`/live lev <n>`           — set leverage\n"
        f"`/live type market|limit` — jenis order\n"
        f"`/live dryrun on|off`     — mode uji coba\n\n"
        f"*— Kesehatan Posisi —*\n"
        f"`/health`           — P&L + margin + likuidasi\n"
        f"`/setpos S1|S2 ...` — daftarkan posisi manual\n"
        f"`/setfunding eth <r> btc <r>` — atur funding rate\n"
        f"`/clearpos`         — hapus data posisi\n\n"
        f"*— Konfigurasi —*\n"
        f"`/settings`         — tampilkan semua konfigurasi\n"
        f"`/threshold entry|exit|invalid <val>`\n"
        f"`/peak on|off|<val>`\n"
        f"`/exitconf scans|buffer|pnl <val>`\n"
        f"`/capital <usd>` | `/lookback <jam>` | `/interval <detik>`",
        reply_chat,
    )

# =============================================================================
# Pesan Startup
# =============================================================================
def send_startup_message():
    price_data  = fetch_prices()
    price_info  = (
        f"\n💰 BTC: ${float(price_data.btc_price):,.2f} | ETH: ${float(price_data.eth_price):,.2f}\n"
        if price_data else "\n⚠️ Gagal mengambil data harga.\n"
    )
    hrs_loaded = len(price_history) * settings["scan_interval"] / 3600
    peak_s  = "✅ ON" if settings["peak_enabled"] else "❌ OFF"
    cap_str = f"${settings['capital']:,.0f}" if settings["capital"] > 0 else "belum diset"

    pos_info = ""
    if pos_data.get("strategy") and pos_data.get("eth_entry_price"):
        strat   = pos_data["strategy"]
        eth_dir = "Long" if (pos_data["eth_qty"] or 0) > 0 else "Short"
        btc_dir = "Long" if (pos_data["btc_qty"] or 0) > 0 else "Short"
        pos_info = (
            f"\n🏥 *Posisi {strat} dipulihkan:*\n"
            f"ETH {eth_dir} @ ${pos_data['eth_entry_price']:,.2f} | "
            f"BTC {btc_dir} @ ${pos_data['btc_entry_price']:,.2f}\n"
        )

    live_info = ""
    if PARADEX_LIVE_AVAILABLE:
        live_info = f"\n🔴 *Paradex Live tersedia.* Gunakan `/pdx init` untuk terhubung.\n"

    return send_alert(
        f"………\n*Bot siap memantau pasar.* ⚡\n"
        f"_Swing / Day Trade Edition + Paradex Live_\n"
        f"{price_info}\n"
        f"📊 Scan: {settings['scan_interval']}s | Lookback: {settings['lookback_hours']}h\n"
        f"📈 Entry: ±{settings['entry_threshold']}% | 📉 Exit: ±{settings['exit_threshold']}%\n"
        f"⚠️ Invalidasi: ±{settings['invalidation_threshold']}% | 🛑 TSL: {settings['sl_pct']}%\n"
        f"🔍 Peak Mode: {peak_s} | 💰 Modal: {cap_str}\n"
        f"{pos_info}{live_info}\n"
        f"Ketik `/help` untuk daftar lengkap perintah. ⚡"
    )

# =============================================================================
# Thread Polling Command
# =============================================================================
def command_polling_thread():
    while True:
        try:
            process_commands()
        except Exception as e:
            logger.debug(f"Error polling command: {e}")
            time.sleep(5)

# =============================================================================
# Main Loop
# =============================================================================
def main_loop():
    global last_heartbeat_time, last_redis_refresh

    logger.info("=" * 60)
    logger.info("Monk Bot B — BTC/ETH Divergence | Swing/Day Trade | + Paradex Live")
    logger.info(
        f"Entry: ±{settings['entry_threshold']}% | Exit: ±{settings['exit_threshold']}% | "
        f"Invalidasi: ±{settings['invalidation_threshold']}% | TSL: {settings['sl_pct']}%"
    )
    logger.info("=" * 60)

    threading.Thread(target=command_polling_thread, daemon=True).start()
    logger.info("Command listener aktif")

    # Muat settings tersimpan dari Redis (entry_threshold dll)
    load_settings()
    logger.info(
        f"Settings aktif — Entry: ±{settings['entry_threshold']}% | "
        f"Exit: ±{settings['exit_threshold']}% | "
        f"Invalid: ±{settings['invalidation_threshold']}% | "
        f"SL: {settings['sl_pct']}%"
    )

    load_history()
    prune_history(datetime.now(timezone.utc))
    last_redis_refresh = datetime.now(timezone.utc)
    logger.info(f"History dimuat: {len(price_history)} titik data")

    load_pos_data()
    if pos_data.get("strategy"):
        logger.info(f"Data posisi dipulihkan: {pos_data['strategy']} "
                    f"ETH@{pos_data['eth_entry_price']} BTC@{pos_data['btc_entry_price']}")

    if TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID:
        send_startup_message()

    last_heartbeat_time = datetime.now(timezone.utc)

    while True:
        try:
            now = datetime.now(timezone.utc)
            if should_send_heartbeat(now):
                if send_heartbeat():
                    last_heartbeat_time = now

            refresh_history_from_redis(now)
            price_data = fetch_prices()
            if price_data is None:
                logger.warning("Gagal mengambil data harga")
            else:
                scan_stats["count"]         += 1
                scan_stats["last_btc_price"] = price_data.btc_price
                scan_stats["last_eth_price"] = price_data.eth_price

                if not is_data_fresh(now, price_data.btc_updated_at, price_data.eth_updated_at):
                    logger.warning("Data tidak segar, dilewati")
                else:
                    price_history.append(PricePoint(now, price_data.btc_price, price_data.eth_price))
                    prune_history(now)

                    price_then = get_lookback_price(now)
                    if price_then is None:
                        hrs = len(price_history) * settings["scan_interval"] / 3600
                        logger.info(f"Menunggu data... ({hrs:.1f}h / {settings['lookback_hours']}h)")
                    else:
                        btc_ret, eth_ret, gap = compute_returns(
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )
                        scan_stats["last_gap"]     = gap
                        scan_stats["last_btc_ret"] = btc_ret
                        scan_stats["last_eth_ret"] = eth_ret

                        gap_history.append((datetime.now(timezone.utc), float(gap)))
                        if len(gap_history) > MAX_GAP_HISTORY:
                            gap_history.pop(0)

                        logger.info(
                            f"Mode: {current_mode.value} | "
                            f"BTC {settings['lookback_hours']}h: {format_value(btc_ret)}% | "
                            f"ETH: {format_value(eth_ret)}% | Gap: {format_value(gap)}%"
                        )
                        evaluate_and_transition(
                            btc_ret, eth_ret, gap,
                            price_data.btc_price, price_data.eth_price,
                            price_then.btc, price_then.eth,
                        )

            time.sleep(settings["scan_interval"])

        except KeyboardInterrupt:
            logger.info("Bot dihentikan")
            break
        except Exception as e:
            logger.exception(f"Error tidak terduga: {e}")
            time.sleep(60)

# =============================================================================
# Entry Point
# =============================================================================
if __name__ == "__main__":
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN belum diset")
    if not TELEGRAM_CHAT_ID:
        logger.warning("TELEGRAM_CHAT_ID belum diset")
    main_loop()

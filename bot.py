#!/usr/bin/env python3
"""
Hinata — Simulator Pairs Trade BTC/ETH

Hinata membaca data harga dari Redis secara mandiri,
mendeteksi sinyal entry/exit secara mandiri, dan mensimulasikan
eksekusi posisi secara otomatis.

Commands:
  /sim      — konfigurasi & status simulasi
  /health   — PnL posisi aktif secara langsung
  /pnl      — ringkasan & statistik semua trade
"""
import json
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Optional, List

import requests

from config import (
    UPSTASH_REDIS_URL,
    UPSTASH_REDIS_TOKEN,
    logger,
)

# =============================================================================
# Konfigurasi — sesuaikan di sini
# =============================================================================

# Token & chat ID Telegram khusus Hinata (gunakan token terpisah dari bot utama)
SIM_BOT_TOKEN   = "ISI_TOKEN_SIM_BOT_DISINI"
SIM_CHAT_ID     = "ISI_CHAT_ID_DISINI"

# Redis key yang dibaca dari bot pengumpul harga
REDIS_PRICE_KEY = "monk_bot:price_history"

SCAN_INTERVAL   = 30    # detik antar scan

# =============================================================================
# State Simulasi
# =============================================================================

settings = {
    "enabled":      False,    # True = bot aktif open/close otomatis
    "margin_usd":   100.0,    # margin per leg (USD)
    "leverage":     10.0,     # leverage
    "fee_pct":      0.06,     # taker fee per side (%)
    "entry_thresh": 1.5,      # % gap untuk entry
    "exit_thresh":  0.2,      # % gap untuk exit (TP)
    "invalid_thresh": 4.0,    # % gap untuk invalidasi (SL)
    "eth_ratio":    50.0,     # % alokasi ke ETH leg (50 = dollar-neutral)
}

# Posisi aktif
position = {
    "open":         False,
    "strategy":     None,     # "S1" / "S2"
    "eth_entry":    None,     # float
    "btc_entry":    None,     # float
    "eth_qty":      None,     # float, + = long, - = short
    "btc_qty":      None,     # float
    "eth_notional": None,     # float USD
    "btc_notional": None,     # float USD
    "eth_margin":   None,     # float USD
    "btc_margin":   None,     # float USD
    "fee_open":     None,     # float USD
    "opened_at":    None,     # datetime
}

# Riwayat trade yang sudah selesai
trade_history: List[dict] = []

# Harga terakhir dari Redis
last_prices = {
    "btc": None,
    "eth": None,
    "gap": None,
    "updated_at": None,
}

# Telegram polling state
last_update_id = 0

# =============================================================================
# Telegram
# =============================================================================

def tg_url(method: str) -> str:
    return f"https://api.telegram.org/bot{SIM_BOT_TOKEN}/{method}"


def send_message(text: str, chat_id: str = SIM_CHAT_ID) -> None:
    try:
        requests.post(
            tg_url("sendMessage"),
            json={"chat_id": chat_id, "text": text, "parse_mode": "Markdown"},
            timeout=10,
        )
    except Exception as e:
        logger.error(f"[Hinata] Gagal kirim pesan: {e}")


def get_updates() -> list:
    global last_update_id
    try:
        r = requests.get(
            tg_url("getUpdates"),
            params={"offset": last_update_id + 1, "timeout": 20},
            timeout=25,
        )
        data = r.json()
        if data.get("ok"):
            return data.get("result", [])
    except Exception as e:
        logger.error(f"[Hinata] getUpdates error: {e}")
    return []

# =============================================================================
# Redis — baca harga dari bot pengumpul
# =============================================================================

def fetch_prices_from_redis() -> bool:
    """
    Baca price history dari Redis.
    Ambil dua titik terakhir untuk hitung gap.
    Return True jika berhasil.
    """
    try:
        url  = f"{UPSTASH_REDIS_URL}/get/{REDIS_PRICE_KEY}"
        hdrs = {"Authorization": f"Bearer {UPSTASH_REDIS_TOKEN}"}
        r    = requests.get(url, headers=hdrs, timeout=8)
        data = r.json()
        raw  = data.get("result")
        if not raw:
            return False

        history = json.loads(raw)
        if len(history) < 2:
            return False

        latest = history[-1]
        prev   = history[-2]

        btc_now  = float(latest["btc"])
        eth_now  = float(latest["eth"])
        btc_prev = float(prev["btc"])
        eth_prev = float(prev["eth"])

        btc_ret = (btc_now - btc_prev) / btc_prev * 100
        eth_ret = (eth_now - eth_prev) / eth_prev * 100
        gap     = eth_ret - btc_ret

        last_prices["btc"]        = btc_now
        last_prices["eth"]        = eth_now
        last_prices["gap"]        = gap
        last_prices["updated_at"] = datetime.now(timezone.utc)
        return True

    except Exception as e:
        logger.error(f"[Hinata] Redis error: {e}")
        return False

# =============================================================================
# Simulasi — Open & Close Posisi
# =============================================================================

def open_position(strategy: str) -> None:
    btc_price = last_prices["btc"]
    eth_price = last_prices["eth"]
    if not btc_price or not eth_price:
        return

    margin    = settings["margin_usd"]
    lev       = settings["leverage"]
    fee_pct   = settings["fee_pct"] / 100.0
    eth_r     = settings["eth_ratio"] / 100.0
    btc_r     = 1.0 - eth_r

    notional      = margin * lev
    eth_notional  = notional * eth_r
    btc_notional  = notional * btc_r
    eth_margin_   = eth_notional / lev
    btc_margin_   = btc_notional / lev
    eth_qty_abs   = eth_notional / eth_price
    btc_qty_abs   = btc_notional / btc_price

    if strategy == "S1":
        # Long BTC, Short ETH
        eth_qty = -eth_qty_abs
        btc_qty = +btc_qty_abs
    else:
        # Long ETH, Short BTC
        eth_qty = +eth_qty_abs
        btc_qty = -btc_qty_abs

    fee_open = (eth_notional + btc_notional) * fee_pct

    position.update({
        "open":         True,
        "strategy":     strategy,
        "eth_entry":    eth_price,
        "btc_entry":    btc_price,
        "eth_qty":      eth_qty,
        "btc_qty":      btc_qty,
        "eth_notional": eth_notional,
        "btc_notional": btc_notional,
        "eth_margin":   eth_margin_,
        "btc_margin":   btc_margin_,
        "fee_open":     fee_open,
        "opened_at":    datetime.now(timezone.utc),
    })

    eth_dir  = "Long  📈" if eth_qty > 0 else "Short 📉"
    btc_dir  = "Long  📈" if btc_qty > 0 else "Short 📉"
    strat_label = "S1 — Long BTC / Short ETH" if strategy == "S1" else "S2 — Long ETH / Short BTC"
    er = int(settings["eth_ratio"]); br = 100 - er

    send_message(
        f"📂 *Posisi Dibuka — {strat_label}*\n"
        f"\n"
        f"```\n"
        f"ETH  {eth_dir}  {abs(eth_qty):.4f} @ ${eth_price:,.2f}\n"
        f"     Nilai    : ${eth_notional:,.2f}\n"
        f"     Margin   : ${eth_margin_:,.2f}\n"
        f"\n"
        f"BTC  {btc_dir}  {abs(btc_qty):.6f} @ ${btc_price:,.2f}\n"
        f"     Nilai    : ${btc_notional:,.2f}\n"
        f"     Margin   : ${btc_margin_:,.2f}\n"
        f"\n"
        f"Leverage : {lev:.0f}x\n"
        f"Alokasi  : ETH {er}% / BTC {br}%\n"
        f"Biaya    : ${fee_open:.3f}\n"
        f"```\n"
        f"_Gunakan /health untuk memantau PnL secara langsung._"
    )
    logger.info(f"[Hinata] OPEN {strategy}: ETH {eth_qty:.4f}@{eth_price} | BTC {btc_qty:.6f}@{btc_price}")


def close_position(reason: str = "EXIT") -> None:
    if not position["open"]:
        return

    btc_price = last_prices["btc"]
    eth_price = last_prices["eth"]
    if not btc_price or not eth_price:
        return

    eth_qty      = position["eth_qty"]
    btc_qty      = position["btc_qty"]
    eth_entry    = position["eth_entry"]
    btc_entry    = position["btc_entry"]
    eth_notional = position["eth_notional"]
    btc_notional = position["btc_notional"]
    eth_margin   = position["eth_margin"]
    btc_margin   = position["btc_margin"]
    fee_pct      = settings["fee_pct"] / 100.0
    fee_open     = position["fee_open"]
    fee_close    = (eth_notional + btc_notional) * fee_pct
    total_fee    = fee_open + fee_close
    total_margin = eth_margin + btc_margin

    eth_pnl   = eth_qty * (eth_price - eth_entry)
    btc_pnl   = btc_qty * (btc_price - btc_entry)
    gross_pnl = eth_pnl + btc_pnl
    net_pnl   = gross_pnl - total_fee
    net_pct   = net_pnl / total_margin * 100

    # Durasi trade
    opened_at = position["opened_at"]
    dur_str   = "N/A"
    if opened_at:
        dur_min = int((datetime.now(timezone.utc) - opened_at).total_seconds() / 60)
        h, m    = divmod(dur_min, 60)
        dur_str = f"{h}j {m}m" if h > 0 else f"{m}m"

    # Arah pergerakan harga
    eth_chg = (eth_price - eth_entry) / eth_entry * 100
    btc_chg = (btc_price - btc_entry) / btc_entry * 100

    profit  = net_pnl >= 0
    result  = "PROFIT ✅" if profit else "LOSS ❌"
    sign    = "+" if net_pnl >= 0 else ""
    reason_label = "Take Profit" if reason == "EXIT" else "Stop Loss / Invalidasi"

    # Simpan ke history
    trade_history.append({
        "strategy":  position["strategy"],
        "reason":    reason,
        "eth_entry": eth_entry, "eth_exit": eth_price,
        "btc_entry": btc_entry, "btc_exit": btc_price,
        "eth_pnl":   eth_pnl,   "btc_pnl": btc_pnl,
        "gross_pnl": gross_pnl, "fee":     total_fee,
        "net_pnl":   net_pnl,   "net_pct": net_pct,
        "duration":  dur_str,
        "closed_at": datetime.now(timezone.utc).isoformat(),
    })

    # Reset posisi
    for k in position:
        if k != "open":
            position[k] = None
    position["open"] = False

    eth_sign = "+" if eth_pnl >= 0 else ""
    btc_sign = "+" if btc_pnl >= 0 else ""

    send_message(
        f"📁 *Posisi Ditutup — {result}*\n"
        f"_{reason_label}_\n"
        f"\n"
        f"```\n"
        f"ETH  {eth_entry:,.2f} → {eth_price:,.2f}  ({'+' if eth_chg>=0 else ''}{eth_chg:.2f}%)\n"
        f"     Laba/Rugi : {'+' if eth_pnl>=0 else ''}${eth_pnl:.2f}\n"
        f"\n"
        f"BTC  {btc_entry:,.2f} → {btc_price:,.2f}  ({'+' if btc_chg>=0 else ''}{btc_chg:.2f}%)\n"
        f"     Laba/Rugi : {'+' if btc_pnl>=0 else ''}${btc_pnl:.2f}\n"
        f"\n"
        f"Laba/Rugi Kotor : {'+' if gross_pnl>=0 else ''}${gross_pnl:.2f}\n"
        f"Total Biaya     : -${total_fee:.3f}\n"
        f"Laba/Rugi Bersih: {sign}${net_pnl:.2f} ({sign}{net_pct:.2f}%)\n"
        f"\n"
        f"Modal    : ${total_margin:.2f}\n"
        f"Durasi   : {dur_str}\n"
        f"```\n"
        f"_Jumlah trade selesai: {len(trade_history)}. Gunakan /pnl untuk melihat statistik lengkap._"
    )
    logger.info(f"[Hinata] CLOSE {reason}: net ${net_pnl:.2f} ({net_pct:.2f}%)")

# =============================================================================
# Kalkulasi PnL Aktif
# =============================================================================

def calc_live_pnl() -> Optional[dict]:
    if not position["open"]:
        return None
    btc_price = last_prices["btc"]
    eth_price = last_prices["eth"]
    if not btc_price or not eth_price:
        return None

    eth_qty   = position["eth_qty"]
    btc_qty   = position["btc_qty"]
    eth_pnl   = eth_qty * (eth_price - position["eth_entry"])
    btc_pnl   = btc_qty * (btc_price - position["btc_entry"])
    gross_pnl = eth_pnl + btc_pnl
    # Estimasi fee close
    fee_pct   = settings["fee_pct"] / 100.0
    fee_est   = (position["eth_notional"] + position["btc_notional"]) * fee_pct
    net_pnl   = gross_pnl - position["fee_open"] - fee_est
    margin    = position["eth_margin"] + position["btc_margin"]
    net_pct   = net_pnl / margin * 100

    # Durasi
    dur_min = int((datetime.now(timezone.utc) - position["opened_at"]).total_seconds() / 60)
    h, m    = divmod(dur_min, 60)
    dur_str = f"{h}j {m}m" if h > 0 else f"{m}m"

    return {
        "eth_pnl":   eth_pnl,
        "btc_pnl":   btc_pnl,
        "gross_pnl": gross_pnl,
        "net_pnl":   net_pnl,
        "net_pct":   net_pct,
        "margin":    margin,
        "duration":  dur_str,
        "eth_price": eth_price,
        "btc_price": btc_price,
        "fee_est":   position["fee_open"] + fee_est,
    }

# =============================================================================
# Scan Loop — deteksi sinyal & eksekusi otomatis
# =============================================================================

def scan_loop() -> None:
    logger.info("[Hinata] Scan loop dimulai.")
    while True:
        try:
            ok = fetch_prices_from_redis()
            if not ok or last_prices["gap"] is None:
                time.sleep(SCAN_INTERVAL)
                continue

            gap     = last_prices["gap"]
            enabled = settings["enabled"]
            et      = settings["entry_thresh"]
            xt      = settings["exit_thresh"]
            it      = settings["invalid_thresh"]

            if not position["open"]:
                # Cari sinyal entry
                if enabled:
                    if gap >= et:
                        open_position("S1")
                    elif gap <= -et:
                        open_position("S2")
            else:
                strat = position["strategy"]
                # Cek exit (TP)
                if strat == "S1" and gap <= xt:
                    close_position("EXIT")
                elif strat == "S2" and gap >= -xt:
                    close_position("EXIT")
                # Cek invalidasi (SL)
                elif strat == "S1" and gap <= -it:
                    close_position("INVALID")
                elif strat == "S2" and gap >= it:
                    close_position("INVALID")

        except Exception as e:
            logger.error(f"[Hinata] Scan error: {e}")

        time.sleep(SCAN_INTERVAL)

# =============================================================================
# Command Handlers
# =============================================================================

def handle_sim(args: list, chat_id: str) -> None:
    """
    /sim              — status & konfigurasi
    /sim on / off     — aktifkan / matikan
    /sim margin <usd> — set margin per leg
    /sim lev <n>      — set leverage
    /sim fee <pct>    — set taker fee
    /sim entry <pct>  — set entry threshold
    /sim exit <pct>   — set exit threshold
    """
    enabled = settings["enabled"]
    margin  = settings["margin_usd"]
    lev     = settings["leverage"]
    fee     = settings["fee_pct"]
    et      = settings["entry_thresh"]
    xt      = settings["exit_thresh"]
    it      = settings["invalid_thresh"]
    er      = int(settings["eth_ratio"])
    br      = 100 - er

    if not args:
        # Tampilkan status lengkap
        gap_str = f"{last_prices['gap']:+.2f}%" if last_prices["gap"] is not None else "—"
        btc_str = f"${last_prices['btc']:,.2f}" if last_prices["btc"] else "—"
        eth_str = f"${last_prices['eth']:,.2f}" if last_prices["eth"] else "—"
        pos_str = f"📍 {position['strategy']} (terbuka)" if position["open"] else "💤 Tidak ada posisi"
        sim_str = "🟢 Aktif" if enabled else "🔴 Nonaktif"

        send_message(
            f"🤖 *Hinata — Status*\n"
            f"\n"
            f"Status  : {sim_str}\n"
            f"Posisi  : {pos_str}\n"
            f"\n"
            f"*Harga Terakhir*\n"
            f"BTC : {btc_str}\n"
            f"ETH : {eth_str}\n"
            f"Gap : {gap_str}\n"
            f"\n"
            f"*Konfigurasi*\n"
            f"```\n"
            f"Modal     : ${margin:,.0f} / leg\n"
            f"Leverage  : {lev:.0f}x\n"
            f"Biaya     : {fee:.3f}% / sisi\n"
            f"Alokasi   : ETH {er}% / BTC {br}%\n"
            f"Entry     : ±{et}%\n"
            f"Exit TP   : ±{xt}%\n"
            f"Invalidasi: ±{it}%\n"
            f"```\n"
            f"*Perintah*\n"
            f"`/sim on` `/sim off`\n"
            f"`/sim margin <usd>`\n"
            f"`/sim lev <n>`\n"
            f"`/sim fee <pct>`\n"
            f"`/sim entry <pct>`\n"
            f"`/sim exit <pct>`",
            chat_id,
        )
        return

    cmd = args[0].lower()

    if cmd == "on":
        settings["enabled"] = True
        total = margin * lev * 2
        send_message(
            f"✅ *Simulasi diaktifkan.*\n"
            f"\n"
            f"Modal per leg  : ${margin:,.0f}\n"
            f"Leverage       : {lev:.0f}x\n"
            f"Total eksposur : ${total:,.0f}\n"
            f"\n"
            f"_Hinata akan membuka posisi secara otomatis saat gap mencapai ±{et}%._",
            chat_id,
        )

    elif cmd == "off":
        settings["enabled"] = False
        note = "\n⚠️ _Posisi masih terbuka. Akan ditutup saat sinyal exit terdeteksi._" if position["open"] else ""
        send_message(f"⏹️ *Simulasi dinonaktifkan.*{note}", chat_id)

    elif cmd == "margin":
        try:
            val = float(args[1])
            if not (1 <= val <= 100_000):
                send_message("⚠️ Modal harus antara $1 hingga $100.000.", chat_id)
                return
            settings["margin_usd"] = val
            send_message(
                f"✅ Modal per leg diperbarui: *${val:,.0f}*\n"
                f"Total eksposur per trade: ${val * settings['leverage'] * 2:,.0f}",
                chat_id,
            )
        except (IndexError, ValueError):
            send_message("⚠️ Format: `/sim margin <usd>` — contoh: `/sim margin 200`", chat_id)

    elif cmd == "lev":
        try:
            val = float(args[1])
            if not (1 <= val <= 200):
                send_message("⚠️ Leverage harus antara 1x hingga 200x.", chat_id)
                return
            settings["leverage"] = val
            send_message(
                f"✅ Leverage diperbarui: *{val:.0f}x*\n"
                f"Total eksposur per trade: ${settings['margin_usd'] * val * 2:,.0f}",
                chat_id,
            )
        except (IndexError, ValueError):
            send_message("⚠️ Format: `/sim lev <n>` — contoh: `/sim lev 20`", chat_id)

    elif cmd == "fee":
        try:
            val = float(args[1])
            settings["fee_pct"] = val
            send_message(f"✅ Biaya taker diperbarui: *{val:.4f}%* per sisi.", chat_id)
        except (IndexError, ValueError):
            send_message("⚠️ Format: `/sim fee <pct>` — contoh: `/sim fee 0.06`", chat_id)

    elif cmd == "entry":
        try:
            val = float(args[1])
            settings["entry_thresh"] = val
            send_message(f"✅ Ambang batas entry diperbarui: *±{val}%*", chat_id)
        except (IndexError, ValueError):
            send_message("⚠️ Format: `/sim entry <pct>` — contoh: `/sim entry 1.5`", chat_id)

    elif cmd == "exit":
        try:
            val = float(args[1])
            settings["exit_thresh"] = val
            send_message(f"✅ Ambang batas exit diperbarui: *±{val}%*", chat_id)
        except (IndexError, ValueError):
            send_message("⚠️ Format: `/sim exit <pct>` — contoh: `/sim exit 0.2`", chat_id)

    else:
        send_message("⚠️ Perintah tidak dikenali. Gunakan `/sim` untuk daftar lengkap.", chat_id)


def handle_health(chat_id: str) -> None:
    """/health — tampilkan PnL posisi aktif."""
    if not position["open"]:
        gap_str = f"{last_prices['gap']:+.2f}%" if last_prices["gap"] is not None else "—"
        btc_str = f"${last_prices['btc']:,.2f}" if last_prices["btc"] else "—"
        eth_str = f"${last_prices['eth']:,.2f}" if last_prices["eth"] else "—"
        status  = "🟢 Aktif" if settings["enabled"] else "🔴 Nonaktif"
        send_message(
            f"💤 *Tidak ada posisi aktif.*\n"
            f"\n"
            f"Status simulasi : {status}\n"
            f"BTC : {btc_str}\n"
            f"ETH : {eth_str}\n"
            f"Gap : {gap_str}\n"
            f"\n"
            f"_Hinata akan membuka posisi secara otomatis saat gap mencapai ±{settings['entry_thresh']}%._",
            chat_id,
        )
        return

    h = calc_live_pnl()
    if not h:
        send_message("⚠️ Gagal menghitung PnL. Data harga belum tersedia.", chat_id)
        return

    strat     = position["strategy"]
    eth_entry = position["eth_entry"]
    btc_entry = position["btc_entry"]
    eth_dir   = "Long  📈" if position["eth_qty"] > 0 else "Short 📉"
    btc_dir   = "Long  📈" if position["btc_qty"] > 0 else "Short 📉"

    eth_chg = (h["eth_price"] - eth_entry) / eth_entry * 100
    btc_chg = (h["btc_price"] - btc_entry) / btc_entry * 100

    net_emoji = "🟢" if h["net_pnl"] >= 0 else "🔴"
    eth_emoji = "🟢" if h["eth_pnl"] >= 0 else "🔴"
    btc_emoji = "🟢" if h["btc_pnl"] >= 0 else "🔴"
    sign      = "+" if h["net_pnl"] >= 0 else ""
    gap_str   = f"{last_prices['gap']:+.2f}%" if last_prices["gap"] is not None else "—"
    et_label  = "S1 — Long BTC / Short ETH" if strat == "S1" else "S2 — Long ETH / Short BTC"

    send_message(
        f"📊 *Health Posisi — {et_label}*\n"
        f"_Durasi: {h['duration']}_\n"
        f"\n"
        f"```\n"
        f"ETH  {eth_dir}\n"
        f"     Harga Masuk : ${eth_entry:,.2f}\n"
        f"     Harga Kini  : ${h['eth_price']:,.2f}  ({'+' if eth_chg>=0 else ''}{eth_chg:.2f}%)\n"
        f"     Laba/Rugi   : {eth_emoji} {'+' if h['eth_pnl']>=0 else ''}${h['eth_pnl']:.2f}\n"
        f"\n"
        f"BTC  {btc_dir}\n"
        f"     Harga Masuk : ${btc_entry:,.2f}\n"
        f"     Harga Kini  : ${h['btc_price']:,.2f}  ({'+' if btc_chg>=0 else ''}{btc_chg:.2f}%)\n"
        f"     Laba/Rugi   : {btc_emoji} {'+' if h['btc_pnl']>=0 else ''}${h['btc_pnl']:.2f}\n"
        f"\n"
        f"Gap               : {gap_str}\n"
        f"Estimasi biaya    : -${h['fee_est']:.3f}\n"
        f"Laba/Rugi Bersih  : {net_emoji} {sign}${h['net_pnl']:.2f} ({sign}{h['net_pct']:.2f}%)\n"
        f"Modal             : ${h['margin']:.2f}\n"
        f"```",
        chat_id,
    )


def handle_pnl(chat_id: str) -> None:
    """/pnl — ringkasan & statistik semua trade."""
    # PnL posisi aktif
    active_str = ""
    if position["open"]:
        h = calc_live_pnl()
        if h:
            sign = "+" if h["net_pnl"] >= 0 else ""
            em   = "🟢" if h["net_pnl"] >= 0 else "🔴"
            active_str = (
                f"📍 *Posisi Aktif: {position['strategy']}*\n"
                f"Laba/Rugi Bersih saat ini: {em} *{sign}${h['net_pnl']:.2f} ({sign}{h['net_pct']:.2f}%)*\n"
                f"Durasi: {h['duration']}\n\n"
            )

    if not trade_history:
        send_message(
            f"{active_str}"
            f"📋 *Statistik Trade*\n\n"
            f"Belum ada trade yang selesai.\n"
            f"Aktifkan simulasi dengan `/sim on` dan tunggu sinyal entry.",
            chat_id,
        )
        return

    total     = len(trade_history)
    wins      = [t for t in trade_history if t["net_pnl"] >= 0]
    losses    = [t for t in trade_history if t["net_pnl"] < 0]
    total_net = sum(t["net_pnl"] for t in trade_history)
    total_fee = sum(t["fee"] for t in trade_history)
    win_rate  = len(wins) / total * 100
    avg_win   = sum(t["net_pnl"] for t in wins)   / len(wins)   if wins   else 0.0
    avg_loss  = sum(t["net_pnl"] for t in losses) / len(losses) if losses else 0.0
    best      = max(trade_history, key=lambda t: t["net_pnl"])
    worst     = min(trade_history, key=lambda t: t["net_pnl"])
    rr        = abs(avg_win / avg_loss) if avg_loss != 0 else 0.0

    net_em    = "🟢" if total_net >= 0 else "🔴"
    sign      = "+" if total_net >= 0 else ""

    recent = trade_history[-5:]
    recent_lines = ""
    for t in reversed(recent):
        em  = "✅" if t["net_pnl"] >= 0 else "❌"
        s   = "+" if t["net_pnl"] >= 0 else ""
        rc  = "TP" if t["reason"] == "EXIT" else "SL"
        recent_lines += (
            f"  {em} {t['strategy']} {rc}  "
            f"{s}${t['net_pnl']:.2f} ({s}{t['net_pct']:.2f}%)  "
            f"{t['duration']}\n"
        )

    send_message(
        f"{active_str}"
        f"📋 *Statistik Trade — {total} Trade*\n"
        f"\n"
        f"```\n"
        f"Laba/Rugi Bersih : {sign}${total_net:.2f}  {net_em}\n"
        f"Total Biaya      : -${total_fee:.3f}\n"
        f"\n"
        f"Tingkat Kemenangan : {len(wins)}M / {len(losses)}K  ({win_rate:.0f}%)\n"
        f"Rata-rata menang   : +${avg_win:.2f}\n"
        f"Rata-rata kalah    : ${avg_loss:.2f}\n"
        f"Rasio R:R          : 1:{rr:.2f}\n"
        f"\n"
        f"Trade terbaik  : +${best['net_pnl']:.2f}  ({best['strategy']})\n"
        f"Trade terburuk :  ${worst['net_pnl']:.2f}  ({worst['strategy']})\n"
        f"\n"
        f"5 Trade Terakhir:\n"
        f"{recent_lines}"
        f"```",
        chat_id,
    )

# =============================================================================
# Telegram Polling Loop
# =============================================================================

def polling_loop() -> None:
    global last_update_id
    logger.info("[Hinata] Polling loop dimulai.")
    while True:
        try:
            updates = get_updates()
            for upd in updates:
                last_update_id = upd["update_id"]
                msg = upd.get("message", {})
                text = msg.get("text", "").strip()
                chat_id = str(msg.get("chat", {}).get("id", ""))
                if not text or not chat_id:
                    continue

                parts   = text.split()
                command = parts[0].split("@")[0].lower()
                args    = parts[1:]

                if command == "/sim":
                    handle_sim(args, chat_id)
                elif command == "/health":
                    handle_health(chat_id)
                elif command == "/pnl":
                    handle_pnl(chat_id)
                elif command == "/start":
                    send_message(
                        "👋 *Selamat datang, Senpai.*\n"
                        "\n"
                        "Hinata akan mensimulasikan pairs trade BTC/ETH secara otomatis "
                        "berdasarkan sinyal gap yang terdeteksi dari data pasar.\n"
                        "\n"
                        "*Perintah tersedia:*\n"
                        "`/sim`    — konfigurasi & status\n"
                        "`/health` — laba/rugi posisi aktif\n"
                        "`/pnl`    — statistik semua trade\n"
                        "\n"
                        "_Mulai dengan `/sim on` setelah mengatur modal dan leverage._",
                        chat_id,
                    )
                else:
                    send_message(
                        "⚠️ Perintah tidak dikenal.\n\n"
                        "`/sim`    — konfigurasi & status\n"
                        "`/health` — laba/rugi posisi aktif\n"
                        "`/pnl`    — statistik semua trade",
                        chat_id,
                    )
        except Exception as e:
            logger.error(f"[Hinata] Polling error: {e}")
        time.sleep(1)

# =============================================================================
# Entry Point
# =============================================================================

def main() -> None:
    logger.info("[Hinata] Memulai Hinata...")

    # Jalankan scan loop di thread terpisah
    scan_thread = threading.Thread(target=scan_loop, daemon=True)
    scan_thread.start()

    # Kirim pesan startup
    send_message(
        "🤖 *Hinata siap digunakan, Senpai.*\n"
        "\n"
        "Gunakan `/sim on` untuk memulai simulasi, "
        "atau `/sim` untuk melihat konfigurasi.",
        SIM_CHAT_ID,
    )

    # Jalankan polling di main thread
    polling_loop()


if __name__ == "__main__":
    main()

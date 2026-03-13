"""
paradex_live.py — Paradex Live Trading Module
High-level interface untuk bot: command handler, open/close, toggle live.
"""
import os
import logging
from typing import Optional

from paradex_executor import ParadexExecutor

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Global state
# ─────────────────────────────────────────────────────────────────────────────

live_settings: dict = {
    "active":     False,
    "margin_usd": 100.0,
    "leverage":   10.0,
    "order_type": "MARKET",
    "dryrun":     False,      # True = log only, tidak kirim order
}

_executor: Optional[ParadexExecutor] = None

# Market symbols di Paradex
MARKET_ETH = "ETH-USD-PERP"
MARKET_BTC = "BTC-USD-PERP"


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers (dipanggil dari monk_bot_b.py)
# ─────────────────────────────────────────────────────────────────────────────

def get_executor() -> Optional[ParadexExecutor]:
    return _executor


def is_live_active() -> bool:
    return live_settings["active"] and _executor is not None and _executor.is_ready()


def get_health_positions() -> str:
    """
    Dipanggil dari /health di monk_bot_b.py.
    Return string blok posisi Paradex live, atau "" kalau tidak ada.
    """
    if not is_live_active():
        return ""
    try:
        _executor.sync_all()
        eth_pos = _executor.get_live_position(MARKET_ETH)
        btc_pos = _executor.get_live_position(MARKET_BTC)
        if not eth_pos and not btc_pos:
            return "\n_🔴 Live aktif tapi belum ada posisi di Paradex._\n"

        def _line(p, label):
            if not p:
                return f"│ {label}: — tidak ada\n"
            sign = "+" if p["unrealized_pnl"] >= 0 else ""
            e    = "🟢" if p["unrealized_pnl"] >= 0 else "🔴"
            return (
                f"│ {label}: *{p['side']}* {abs(p['size'])} "
                f"@ ${p['avg_entry']:,.2f} | UPnL: {e} *{sign}${p['unrealized_pnl']:,.2f}*"
                + (f" | Liq: ${p['liq_price']:,.2f}" if p.get("liq_price") else "")
                + "\n"
            )

        return (
            f"\n*🔴 Paradex Live Positions:*\n"
            f"┌─────────────────────\n"
            + _line(eth_pos, "ETH")
            + _line(btc_pos, "BTC")
            + f"│ Margin: ${live_settings['margin_usd']:,.0f} | Lev: {live_settings['leverage']:.0f}x\n"
            + f"└─────────────────────\n"
        )
    except Exception as e:
        logger.warning(f"get_health_positions error: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Sizing helper
# ─────────────────────────────────────────────────────────────────────────────

def _calc_sizes(btc_price: float, eth_price: float):
    """
    Return (eth_qty, btc_qty) berdasarkan live_settings margin & leverage.
    Dollar-neutral 50/50 antara ETH dan BTC leg.
    """
    margin  = float(live_settings["margin_usd"])
    lev     = float(live_settings["leverage"])
    notional = margin * lev
    half     = notional / 2.0

    eth_qty = round(half / eth_price, 4)
    btc_qty = round(half / btc_price, 6)
    return eth_qty, btc_qty


# ─────────────────────────────────────────────────────────────────────────────
# Live open / close
# ─────────────────────────────────────────────────────────────────────────────

def live_open_or_sim(
    strategy: str,
    btc_price: float,
    eth_price: float,
    sim_fn=None,
) -> str:
    """
    Buka posisi live di Paradex.
    strategy: "S1" (Long BTC / Short ETH) atau "S2" (Long ETH / Short BTC)
    Returns pesan ringkasan untuk dikirim ke Telegram.
    """
    if not is_live_active():
        if sim_fn:
            return sim_fn()
        return ""

    eth_qty, btc_qty = _calc_sizes(btc_price, eth_price)
    margin  = live_settings["margin_usd"]
    lev     = live_settings["leverage"]
    otype   = live_settings["order_type"]
    dryrun  = live_settings["dryrun"]

    if strategy.upper() == "S1":
        eth_side = "SELL"   # Short ETH
        btc_side = "BUY"    # Long BTC
        direction = "Long BTC / Short ETH"
    else:
        eth_side = "BUY"    # Long ETH
        btc_side = "SELL"   # Short BTC
        direction = "Long ETH / Short BTC"

    if dryrun:
        logger.info(f"[DRYRUN] Would open {strategy}: ETH {eth_side} {eth_qty} | BTC {btc_side} {btc_qty}")
        return (
            f"\n🧪 *[DRYRUN] Live Order Disimulasikan — {strategy}*\n"
            f"┌─────────────────────\n"
            f"│ ETH: *{eth_side}* {eth_qty} @ ${eth_price:,.2f}\n"
            f"│ BTC: *{btc_side}* {btc_qty} @ ${btc_price:,.2f}\n"
            f"│ Margin: ${margin:,.0f} | Lev: {lev:.0f}x\n"
            f"└─────────────────────\n"
            f"_Dryrun mode — tidak ada order sungguhan yang dikirim._\n"
        )

    # Kirim order ke Paradex
    eth_result = _executor.place_order(MARKET_ETH, eth_side, eth_qty, order_type=otype)
    btc_result = _executor.place_order(MARKET_BTC, btc_side, btc_qty, order_type=otype)

    eth_ok = eth_result is not None
    btc_ok = btc_result is not None

    eth_id = eth_result.get("id", "?") if eth_result else "FAILED"
    btc_id = btc_result.get("id", "?") if btc_result else "FAILED"

    status_e = "✅" if eth_ok else "❌"
    status_b = "✅" if btc_ok else "❌"

    logger.info(f"Live open {strategy}: ETH={eth_ok} BTC={btc_ok}")

    return (
        f"\n🔴 *[LIVE] Posisi Dibuka — {strategy}*\n"
        f"┌─────────────────────\n"
        f"│ *{direction}*\n"
        f"│ ETH: {status_e} {eth_side} {eth_qty} @ ${eth_price:,.2f} | ID: `{eth_id}`\n"
        f"│ BTC: {status_b} {btc_side} {btc_qty} @ ${btc_price:,.2f} | ID: `{btc_id}`\n"
        f"│ Margin: ${margin:,.0f} | Lev: {lev:.0f}x | Type: {otype}\n"
        f"└─────────────────────\n"
        + ("_⚠️ Salah satu order GAGAL — cek posisi manual!_\n" if not (eth_ok and btc_ok) else "")
    )


def live_close_or_sim(
    strategy: str,
    btc_price: float,
    eth_price: float,
    reason:  str  = "EXIT",
    sim_fn   = None,
) -> str:
    """
    Tutup posisi live di Paradex.
    Returns pesan ringkasan untuk Telegram.
    """
    if not is_live_active():
        if sim_fn:
            return sim_fn()
        return ""

    dryrun = live_settings["dryrun"]
    otype  = live_settings["order_type"]

    if dryrun:
        logger.info(f"[DRYRUN] Would close {strategy} reason={reason}")
        return (
            f"\n🧪 *[DRYRUN] Close Order Disimulasikan — {strategy}*\n"
            f"_Reason: {reason} | Dryrun mode aktif._\n"
        )

    # Sync dulu baru close
    _executor.sync_all()

    eth_result = _executor.close_position(MARKET_ETH, order_type=otype)
    btc_result = _executor.close_position(MARKET_BTC, order_type=otype)

    eth_ok = eth_result is not None
    btc_ok = btc_result is not None
    eth_id = eth_result.get("id", "?") if eth_result else "FAILED / No pos"
    btc_id = btc_result.get("id", "?") if btc_result else "FAILED / No pos"

    status_e = "✅" if eth_ok else "⚠️"
    status_b = "✅" if btc_ok else "⚠️"

    reason_emoji = {"EXIT": "✅", "TP": "🎯", "TSL": "⛔", "INVALID": "⚠️"}.get(reason, "🔴")

    logger.info(f"Live close {strategy} [{reason}]: ETH={eth_ok} BTC={btc_ok}")

    return (
        f"\n🔴 *[LIVE] Posisi Ditutup — {reason_emoji} {reason}*\n"
        f"┌─────────────────────\n"
        f"│ ETH: {status_e} Close @ ${eth_price:,.2f} | ID: `{eth_id}`\n"
        f"│ BTC: {status_b} Close @ ${btc_price:,.2f} | ID: `{btc_id}`\n"
        f"└─────────────────────\n"
        + ("_⚠️ Salah satu order GAGAL — cek posisi manual!_\n" if not (eth_ok and btc_ok) else "")
    )


# ─────────────────────────────────────────────────────────────────────────────
# /pdx command handler
# ─────────────────────────────────────────────────────────────────────────────

def handle_pdx_command(args: list, chat_id: str, send_reply_fn=None):
    """
    /pdx                          — status koneksi
    /pdx init <jwt> <address>     — init executor
    /pdx balance                  — saldo akun
    /pdx fills                    — history fills
    /pdx cancel                   — cancel semua order
    /pdx sync                     — refresh posisi
    """
    global _executor  # ← harus di sini, paling atas fungsi

    def reply(msg):
        if send_reply_fn:
            send_reply_fn(msg, chat_id)

    global _executor

    sub = args[0].lower() if args else "status"

    # ── Status ──────────────────────────────────────────────────
    if sub in ("status", ""):
        if _executor is None:
            reply(
                "🔴 *Paradex: Belum terhubung*\n\n"
                "Gunakan:\n`/pdx init <jwt_token> <account_address>`\n\n"
                "_JWT dan address bisa didapat dari Paradex app > Settings > API Keys_"
            )
        elif not _executor.is_ready():
            reply("⚠️ *Paradex: Executor ada tapi koneksi gagal.*\nCoba `/pdx init` ulang.")
        else:
            bal      = _executor.get_balance()
            margin   = live_settings["margin_usd"]
            lev      = live_settings["leverage"]
            otype    = live_settings["order_type"]
            dryrun   = live_settings["dryrun"]
            active   = live_settings["active"]
            notional = margin * lev

            bal_str = (
                f"│ Equity:          *${bal.get('equity', 0):,.2f}*\n"
                f"│ Free Collateral: ${bal.get('free_collateral', 0):,.2f}\n"
                f"│ UPnL:            ${bal.get('unrealized_pnl', 0):+,.2f}\n"
            ) if bal else "│ Balance: (gagal load)\n"

            # Sync dan tampilkan posisi aktif
            _executor.sync_all()
            eth_pos = _executor.get_live_position(MARKET_ETH)
            btc_pos = _executor.get_live_position(MARKET_BTC)

            def _pos_line(p, label):
                if not p:
                    return f"│ {label}: — tidak ada posisi\n"
                sign = "+" if p["unrealized_pnl"] >= 0 else ""
                return (
                    f"│ {label}: *{p['side']}* {abs(p['size'])} "
                    f"@ ${p['avg_entry']:,.2f} | UPnL: *{sign}${p['unrealized_pnl']:,.2f}*\n"
                )

            pos_block = (
                f"│\n│ 📍 *Posisi Aktif:*\n"
                + _pos_line(eth_pos, "ETH")
                + _pos_line(btc_pos, "BTC")
            ) if (eth_pos or btc_pos) else "│\n│ 📍 Posisi: — tidak ada\n"

            live_str  = "🟢 AKTIF" if active else "🔴 OFF"
            dr_str    = " 🧪 DRYRUN" if dryrun else ""
            reply(
                f"✅ *Paradex Dashboard*{dr_str}\n"
                f"┌─────────────────────\n"
                f"│ Account: `{_executor.account_address[:18]}...`\n"
                f"{bal_str}"
                f"│\n│ ⚙️ *Trading Settings:*\n"
                f"│ Status:     {live_str}\n"
                f"│ Margin:     *${margin:,.0f}* per pair\n"
                f"│ Leverage:   *{lev:.0f}x*\n"
                f"│ Notional:   ~${notional:,.0f} (margin × lev)\n"
                f"│ Order type: {otype}\n"
                f"│ Dryrun:     {'ON 🧪' if dryrun else 'OFF'}\n"
                f"{pos_block}"
                f"└─────────────────────\n"
                f"_`/pdx balance` | `/pdx fills` | `/pdx cancel` | `/pdx sync`_\n"
                f"_`/live on|off` | `/live margin` | `/live lev` | `/live dryrun`_"
            )

    # ── Init ────────────────────────────────────────────────────
    elif sub == "init":
        if len(args) < 3:
            reply(
                "⚠️ *Usage:*\n\n"
                "*Mode L2 (Paradex Key — Recommended):*\n"
                "`/pdx init l2 <paradex_private_key> <l2_address>`\n\n"
                "*Mode L1 (Ethereum Key):*\n"
                "`/pdx init l1 <eth_private_key> <eth_address>`\n\n"
                "📍 *Cara dapat L2 key:*\n"
                "Paradex app → klik address kanan atas → *Export Private Key*\n\n"
                "⚠️ _Kirim hanya di chat pribadi yang aman!_"
            )
            return

        # Auto-detect: kalau args[1] adalah "l1" atau "l2", args[2] = key, args[3] = address
        # Kalau tidak ada prefix, fallback ke L2
        if args[1].lower() in ("l1", "l2"):
            mode    = args[1].lower()
            if len(args) < 4:
                reply("⚠️ Kurang argumen. Contoh: `/pdx init l2 <key> <address>`")
                return
            key     = args[2]
            address = args[3]
        else:
            # Tanpa prefix = L2
            mode    = "l2"
            key     = args[1]
            address = args[2]

        reply(f"⏳ Menghubungkan ke Paradex (mode {mode.upper()})...")
        try:
            if mode == "l2":
                new_exec = ParadexExecutor(l2_private_key=key, l2_address=address)
            else:
                new_exec = ParadexExecutor(l1_private_key=key, l1_address=address)

            if new_exec.is_ready():
                _executor = new_exec
                bal = _executor.get_balance()
                eq  = bal.get("equity", 0)
                reply(
                    f"✅ *Paradex terhubung! (Mode {mode.upper()})*\n"
                    f"Account: `{address[:16]}...`\n"
                    f"Equity: *${eq:,.2f}*\n\n"
                    f"Gunakan `/live on` untuk mulai live trading."
                )
            else:
                reply(
                    "❌ *Gagal terhubung ke Paradex.*\n"
                    "Pastikan key dan address benar.\n"
                    "_Cek Railway logs untuk detail error._"
                )
        except Exception as e:
            reply(f"❌ *Error saat init:* `{e}`")

    # ── Balance ─────────────────────────────────────────────────
    elif sub == "balance":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung. Gunakan `/pdx init` dulu."); return
        bal = _executor.get_balance()
        if not bal:
            reply("❌ Gagal ambil balance dari Paradex."); return
        reply(
            f"💰 *Paradex Balance*\n"
            f"┌─────────────────────\n"
            f"│ Equity:          *${bal.get('equity', 0):,.2f}*\n"
            f"│ Free Collateral: ${bal.get('free_collateral', 0):,.2f}\n"
            f"│ Total Collateral:${bal.get('total_collateral', 0):,.2f}\n"
            f"│ Unrealized PnL:  ${bal.get('unrealized_pnl', 0):+,.2f}\n"
            f"└─────────────────────\n"
        )

    # ── Fills ───────────────────────────────────────────────────
    elif sub == "fills":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        fills = _executor.get_fills(limit=10)
        if not fills:
            reply("📋 *Fills:* Tidak ada data atau belum ada trade."); return
        lines = []
        for f in fills[:8]:
            mkt   = f.get("market", "?")
            side  = f.get("side", "?")
            size  = f.get("size", "?")
            price = f.get("price", "?")
            try:
                price = f"${float(price):,.2f}"
            except Exception:
                pass
            lines.append(f"│ {side} {size} {mkt} @ {price}")
        reply(
            f"📋 *Recent Fills ({len(fills)}):*\n"
            f"┌─────────────────────\n"
            + "\n".join(lines) +
            f"\n└─────────────────────\n"
        )

    # ── Cancel ──────────────────────────────────────────────────
    elif sub == "cancel":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        ok = _executor.cancel_all_orders()
        reply("✅ *Semua open orders dibatalkan.*" if ok else "❌ Gagal cancel orders.")

    # ── Sync ────────────────────────────────────────────────────
    elif sub == "sync":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        _executor.sync_all()
        eth_pos = _executor.get_live_position(MARKET_ETH)
        btc_pos = _executor.get_live_position(MARKET_BTC)
        def _pos_str(p, label):
            if not p: return f"│ {label}: Tidak ada posisi\n"
            return (
                f"│ {label}: {p['side']} {abs(p['size'])} "
                f"@ ${p['avg_entry']:,.2f} | UPnL: ${p['unrealized_pnl']:+,.2f}\n"
            )
        reply(
            f"🔄 *Positions synced:*\n"
            f"┌─────────────────────\n"
            + _pos_str(eth_pos, "ETH")
            + _pos_str(btc_pos, "BTC")
            + "└─────────────────────\n"
        )

    else:
        reply(
            "❓ Command tidak dikenal.\n\n"
            "*Usage:*\n`/pdx` — status\n`/pdx init <jwt> <addr>` — connect\n"
            "`/pdx balance` — saldo\n`/pdx fills` — history\n`/pdx cancel` — cancel orders\n`/pdx sync` — refresh posisi"
        )


# ─────────────────────────────────────────────────────────────────────────────
# /live command handler
# ─────────────────────────────────────────────────────────────────────────────

def handle_live_command(args: list, chat_id: str, send_reply_fn=None):
    """
    /live                    — status live trading
    /live on|off             — toggle live trading
    /live margin <usd>       — set modal per pair
    /live lev <n>            — set leverage
    /live type market|limit  — order type
    /live dryrun on|off      — dryrun mode
    """
    def reply(msg):
        if send_reply_fn:
            send_reply_fn(msg, chat_id)

    sub = args[0].lower() if args else "status"

    margin = live_settings["margin_usd"]
    lev    = live_settings["leverage"]
    otype  = live_settings["order_type"]
    dryrun = live_settings["dryrun"]

    # ── Status ──────────────────────────────────────────────────
    if sub in ("status", ""):
        connected = _executor is not None and _executor.is_ready()
        active    = live_settings["active"]
        dr_str    = " 🧪 *DRYRUN*" if dryrun else ""
        reply(
            f"🔴 *Live Trading Settings*{dr_str}\n\n"
            f"┌─────────────────────\n"
            f"│ Status:     {'🟢 ON' if active else '🔴 OFF'}\n"
            f"│ Paradex:    {'✅ Connected' if connected else '❌ Not connected'}\n"
            f"│ Margin:     ${margin:,.0f} per pair\n"
            f"│ Leverage:   {lev:.0f}x\n"
            f"│ Order type: {otype}\n"
            f"│ Dryrun:     {'ON 🧪' if dryrun else 'OFF'}\n"
            f"└─────────────────────\n\n"
            f"*Commands:*\n"
            f"`/live on|off` | `/live margin <usd>`\n"
            f"`/live lev <n>` | `/live type market|limit`\n"
            f"`/live dryrun on|off`"
        )

    # ── Toggle on/off ────────────────────────────────────────────
    elif sub == "on":
        if _executor is None or not _executor.is_ready():
            reply(
                "⚠️ *Paradex belum terhubung!*\n"
                "Gunakan dulu: `/pdx init <jwt> <address>`"
            ); return
        live_settings["active"] = True
        dr_note = "\n🧪 *Dryrun mode ON* — tidak ada order sungguhan." if dryrun else ""
        reply(
            f"🟢 *Live trading AKTIF!*{dr_note}\n\n"
            f"Margin: ${margin:,.0f} | Lev: {lev:.0f}x | Type: {otype}\n"
            f"_Bot akan kirim order ke Paradex saat sinyal entry/exit muncul._\n\n"
            f"⚠️ Pastikan balance cukup dan setting sudah benar!"
        )

    elif sub == "off":
        live_settings["active"] = False
        reply("🔴 *Live trading DIMATIKAN.* Bot kembali ke mode simulasi.")

    # ── Margin ──────────────────────────────────────────────────
    elif sub == "margin":
        if len(args) < 2:
            reply(f"💰 Margin sekarang: *${margin:,.0f}*\nUsage: `/live margin <usd>`"); return
        try:
            val = float(args[1])
            if val < 10 or val > 100_000:
                reply("Margin harus antara $10 – $100,000."); return
            live_settings["margin_usd"] = val
            reply(f"✅ Margin: *${val:,.0f}*")
        except ValueError:
            reply("Angkanya tidak valid.")

    # ── Leverage ────────────────────────────────────────────────
    elif sub == "lev":
        if len(args) < 2:
            reply(f"📊 Leverage sekarang: *{lev:.0f}x*\nUsage: `/live lev <n>`"); return
        try:
            val = float(args[1])
            if not (1 <= val <= 50):
                reply("Leverage harus 1x – 50x."); return
            live_settings["leverage"] = val
            reply(f"✅ Leverage: *{val:.0f}x*")
        except ValueError:
            reply("Angkanya tidak valid.")

    # ── Order type ──────────────────────────────────────────────
    elif sub == "type":
        if len(args) < 2:
            reply(f"Order type sekarang: *{otype}*\nUsage: `/live type market|limit`"); return
        t = args[1].upper()
        if t not in ("MARKET", "LIMIT"):
            reply("Harus `market` atau `limit`."); return
        live_settings["order_type"] = t
        reply(f"✅ Order type: *{t}*")

    # ── Dryrun ──────────────────────────────────────────────────
    elif sub == "dryrun":
        if len(args) < 2:
            reply(f"Dryrun mode: *{'ON' if dryrun else 'OFF'}*\nUsage: `/live dryrun on|off`"); return
        val = args[1].lower()
        if val == "on":
            live_settings["dryrun"] = True
            reply("🧪 *Dryrun mode ON* — order akan dilog tapi tidak dikirim ke Paradex.")
        elif val == "off":
            live_settings["dryrun"] = False
            reply("✅ *Dryrun mode OFF* — order sungguhan akan dikirim ke Paradex.")
        else:
            reply("Gunakan `on` atau `off`.")

    else:
        reply(
            "❓ Command tidak dikenal.\n\n"
            "*Usage:*\n`/live` — status\n`/live on|off` — toggle\n"
            "`/live margin <usd>` | `/live lev <n>`\n"
            "`/live type market|limit` | `/live dryrun on|off`"
        )

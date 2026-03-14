"""
paradex_live.py — Paradex Live Trading Module

FIXES:
1. /pdx close — close semua posisi via command
2. Sizing formula yang benar + penjelasan margin vs notional
3. set_leverage dipanggil sebelum setiap order
4. /live dryrun + preview sizing

═══════════════════════════════════════════════════
SIZING FORMULA — BACA INI DULU
═══════════════════════════════════════════════════

margin_usd = modal yang kamu RISIKO (yang bisa hilang kalau liq)
leverage   = multiplier
notional   = margin_usd × leverage  ← nilai posisi sebenarnya

Contoh:
  margin=$30, lev=40x
  notional = $30 × 40 = $1,200
  eth_qty  = $1,200 / $2,100 = 0.5714 ETH
  btc_qty  = $1,200 / $85,000 = 0.01412 BTC

Dengan balance $100:
  Pakai margin=$30/pair × 2 pair = $60 total margin
  Notional exposed = $1,200 × 2 = $2,400
  Yang bisa liq: hanya $60

Kenapa exchange tampilkan "Margin Req $0.3"?
  → Itu kalkulasi exchange berdasarkan leverage yang MEREKA set
  → Kalau leverage default exchange = 50x, margin req = $15/50 = $0.3
  → SOLUSI: set_leverage() HARUS berhasil sebelum order
═══════════════════════════════════════════════════
"""
import logging
from typing import Optional

from paradex_executor import ParadexExecutor

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────────────────────
# Global state
# ─────────────────────────────────────────────────────────────────────────────

live_settings: dict = {
    "active":     False,
    "margin_usd": 100.0,   # margin per pair yang kamu risiko
    "leverage":   10.0,
    "order_type": "MARKET",
    "dryrun":     False,
    "mode":       "normal",  # "normal" = pair divergence | "x" = ikut arah ETH
}

# ─────────────────────────────────────────────────────────────────────────────
# Mode Trading
# ─────────────────────────────────────────────────────────────────────────────
#
# MODE NORMAL (default):
#   S1: Long BTC / Short ETH  ← BTC outperform, fade ETH
#   S2: Short BTC / Long ETH  ← ETH outperform, fade BTC
#   → Strategi divergence: taruhan gap akan konvergen
#
# MODE X (momentum):
#   S1: Long BTC / Long ETH   ← BTC kuat, ETH ikut momentum
#   S2: Short BTC / Short ETH ← BTC lemah, ETH ikut turun
#   → Strategi momentum: keduanya searah mengikuti tren BTC
#
# Ganti mode: /live mode normal | /live mode x
# ─────────────────────────────────────────────────────────────────────────────

_executor: Optional[ParadexExecutor] = None

MARKET_ETH = "ETH-USD-PERP"
MARKET_BTC = "BTC-USD-PERP"
ALL_MARKETS = [MARKET_ETH, MARKET_BTC]


# ─────────────────────────────────────────────────────────────────────────────
# Public helpers
# ─────────────────────────────────────────────────────────────────────────────

def get_executor() -> Optional[ParadexExecutor]:
    return _executor


def is_live_active() -> bool:
    return live_settings["active"] and _executor is not None and _executor.is_ready()


def get_health_positions() -> str:
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
            + f"│ Margin: ${live_settings['margin_usd']:,.0f}/pair | Lev: {live_settings['leverage']:.0f}x\n"
            + f"└─────────────────────\n"
        )
    except Exception as e:
        logger.warning(f"get_health_positions error: {e}")
        return ""


# ─────────────────────────────────────────────────────────────────────────────
# Sizing
# ─────────────────────────────────────────────────────────────────────────────

def _calc_sizes(btc_price: float, eth_price: float):
    """
    notional = margin × leverage
    qty      = notional / price

    Contoh margin=$30, lev=40x, eth=$2100:
      notional = $1,200
      eth_qty  = 0.5714
    """
    margin   = float(live_settings["margin_usd"])
    lev      = float(live_settings["leverage"])
    notional = margin * lev

    eth_qty = notional / eth_price
    btc_qty = notional / btc_price

    logger.info(
        f"Sizing: margin=${margin} × lev={lev}x = notional=${notional:.2f}/pair | "
        f"ETH={eth_qty:.6f}@${eth_price} BTC={btc_qty:.7f}@${btc_price}"
    )
    return eth_qty, btc_qty


def calc_order_preview(btc_price: float, eth_price: float) -> str:
    margin   = float(live_settings["margin_usd"])
    lev      = float(live_settings["leverage"])
    notional = margin * lev
    eth_qty, btc_qty = _calc_sizes(btc_price, eth_price)
    return (
        f"┌─────────────────────\n"
        f"│ *Order Preview (per pair):*\n"
        f"│ Margin risiko: *${margin:,.2f}*/pair\n"
        f"│ Leverage:      *{lev:.0f}x*\n"
        f"│ Notional:      *${notional:,.2f}*/pair\n"
        f"│ ETH qty:  {eth_qty:.4f} ETH @ ${eth_price:,.2f}\n"
        f"│ BTC qty:  {btc_qty:.5f} BTC @ ${btc_price:,.2f}\n"
        f"│ Margin req exchange: ~${notional/lev:,.2f}/pair\n"
        f"│ Total margin 2 pair: *${margin * 2:,.2f}*\n"
        f"└─────────────────────\n"
        f"_💡 Margin Req di exchange = Notional ÷ Leverage yang aktif_\n"
        f"_Kalau tidak sesuai, cek `/pdx` apakah leverage berhasil diset_\n"
    )


# ─────────────────────────────────────────────────────────────────────────────
# Live open / close
# ─────────────────────────────────────────────────────────────────────────────

def _get_order_sides(strategy: str) -> tuple:
    """
    Tentukan arah order ETH dan BTC berdasarkan strategi dan mode trading.

    Return: (eth_side, btc_side, direction_label)

    Mode NORMAL (divergence — pairs trade):
        S1: Short ETH + Long BTC  — ETH pump lebih, fade dengan short ETH
        S2: Long ETH  + Short BTC — ETH dump lebih, fade dengan long ETH

    Mode X (searah — keduanya sama arah):
        S1: Short ETH + Short BTC — ETH pump lebih → short keduanya
        S2: Long ETH  + Long BTC  — ETH dump lebih → long keduanya
    """
    mode = live_settings.get("mode", "normal").lower()
    s    = strategy.upper()

    if mode == "x":
        # Mode X: keduanya searah mengikuti sinyal ETH
        # S1 = ETH pump lebih dari BTC → Short ETH + Short BTC
        # S2 = ETH dump lebih dari BTC → Long ETH  + Long BTC
        if s == "S1":
            return "SELL", "SELL", "Short ETH + Short BTC (Mode X)"
        else:  # S2
            return "BUY",  "BUY",  "Long ETH + Long BTC (Mode X)"
    else:
        # Mode Normal: divergence / pairs trade
        if s == "S1":
            return "SELL", "BUY",  "Long BTC / Short ETH"
        else:
            return "BUY",  "SELL", "Long ETH / Short BTC"


def live_open_or_sim(
    strategy:  str,
    btc_price: float,
    eth_price: float,
    sim_fn=None,
) -> str:
    if not is_live_active():
        if sim_fn:
            return sim_fn()
        return ""

    eth_qty, btc_qty = _calc_sizes(btc_price, eth_price)
    margin   = live_settings["margin_usd"]
    lev      = live_settings["leverage"]
    notional = margin * lev
    otype    = live_settings["order_type"]
    dryrun   = live_settings["dryrun"]

    # Tentukan arah order sesuai mode (normal / x)
    eth_side, btc_side, direction = _get_order_sides(strategy)

    if dryrun:
        logger.info(f"[DRYRUN] Would open {strategy}: ETH {eth_side} {eth_qty:.4f} | BTC {btc_side} {btc_qty:.5f}")
        return (
            f"\n🧪 *[DRYRUN] Live Order Disimulasikan — {strategy}*\n"
            f"┌─────────────────────\n"
            f"│ *{direction}*\n"
            f"│ ETH: *{eth_side}* {eth_qty:.4f} @ ${eth_price:,.2f}\n"
            f"│ BTC: *{btc_side}* {btc_qty:.5f} @ ${btc_price:,.2f}\n"
            f"│ Margin: ${margin:,.2f}/pair | Lev: {lev:.0f}x | Notional: ~${notional:,.2f}/pair\n"
            f"└─────────────────────\n"
            f"_Dryrun mode — tidak ada order sungguhan yang dikirim._\n"
        )

    # ── Set leverage SEBELUM order ─────────────────────────────────────────
    eth_lev_ok = _executor.set_leverage(MARKET_ETH, lev)
    btc_lev_ok = _executor.set_leverage(MARKET_BTC, lev)
    lev_status = "✅" if (eth_lev_ok and btc_lev_ok) else "⚠️ gagal set lev"

    # ── Kirim order ────────────────────────────────────────────────────────
    eth_result = _executor.place_order(MARKET_ETH, eth_side, eth_qty, order_type=otype)
    btc_result = _executor.place_order(MARKET_BTC, btc_side, btc_qty, order_type=otype)

    eth_ok = eth_result is not None
    btc_ok = btc_result is not None
    eth_id = eth_result.get("id", "?") if eth_result else "FAILED"
    btc_id = btc_result.get("id", "?") if btc_result else "FAILED"

    status_e = "✅" if eth_ok else "❌"
    status_b = "✅" if btc_ok else "❌"

    logger.info(f"Live open {strategy}: ETH={eth_ok} BTC={btc_ok} | lev={lev_status}")

    return (
        f"\n🔴 *[LIVE] Posisi Dibuka — {strategy}*\n"
        f"┌─────────────────────\n"
        f"│ *{direction}*\n"
        f"│ ETH: {status_e} {eth_side} {eth_qty:.4f} @ ${eth_price:,.2f} | ID: `{eth_id}`\n"
        f"│ BTC: {status_b} {btc_side} {btc_qty:.5f} @ ${btc_price:,.2f} | ID: `{btc_id}`\n"
        f"│ Margin: ${margin:,.2f}/pair | Lev: {lev:.0f}x {lev_status}\n"
        f"│ Notional: ~${notional:,.2f}/pair\n"
        f"└─────────────────────\n"
        + ("_⚠️ Salah satu order GAGAL — cek posisi manual!_\n" if not (eth_ok and btc_ok) else "")
        + ("_⚠️ Leverage gagal diset — cek log untuk detail_\n" if not (eth_lev_ok and btc_lev_ok) else "")
    )


def live_close_or_sim(
    strategy:  str,
    btc_price: float,
    eth_price: float,
    reason:    str = "EXIT",
    sim_fn=None,
) -> str:
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


def live_close_all_command() -> str:
    """
    Close semua posisi via command manual (/pdx close).
    Tidak butuh price karena pakai MARKET order.
    """
    if _executor is None or not _executor.is_ready():
        return "⚠️ Paradex belum terhubung."

    dryrun = live_settings.get("dryrun", False)
    if dryrun:
        return "🧪 *[DRYRUN]* Close semua posisi disimulasikan — dryrun mode aktif."

    _executor.sync_all()
    positions = _executor.get_all_positions()

    if not positions:
        return "ℹ️ *Tidak ada posisi terbuka di Paradex.*"

    results = _executor.close_all_positions(order_type="MARKET")

    lines = []
    all_ok = True
    for market, result in results.items():
        ok = result is not None
        if not ok:
            all_ok = False
        order_id = result.get("id", "?") if result else "FAILED"
        pos      = positions.get(market, {})
        side     = pos.get("side", "?")
        size     = abs(pos.get("size", 0))
        lines.append(f"│ {'✅' if ok else '❌'} {market}: close {side} {size} | ID: `{order_id}`")

    return (
        f"🔴 *[LIVE] Close All Positions*\n"
        f"┌─────────────────────\n"
        + "\n".join(lines)
        + f"\n└─────────────────────\n"
        + ("✅ Semua posisi berhasil ditutup.\n" if all_ok else "⚠️ Beberapa posisi gagal ditutup — cek manual!\n")
    )


# ─────────────────────────────────────────────────────────────────────────────
# /pdx command handler
# ─────────────────────────────────────────────────────────────────────────────

def handle_pdx_command(args: list, chat_id: str, send_reply_fn=None):
    """
    /pdx                             — status koneksi
    /pdx init [l1|l2] <key> <addr>  — init executor
    /pdx balance                     — saldo akun
    /pdx fills                       — history fills
    /pdx cancel                      — cancel open orders (bukan close posisi)
    /pdx close                       — close semua posisi ← [NEW]
    /pdx sync                        — refresh posisi
    /pdx preview <btc> <eth>         — preview sizing
    /pdx lev                         — cek / set leverage langsung
    """
    global _executor

    def reply(msg):
        if send_reply_fn:
            send_reply_fn(msg, chat_id)

    sub = args[0].lower() if args else "status"

    # ── Status ──────────────────────────────────────────────────
    if sub in ("status", ""):
        if _executor is None:
            reply(
                "🔴 *Paradex: Belum terhubung*\n\n"
                "Gunakan:\n`/pdx init l2 <key> <address>`\n\n"
                "📍 *Cara dapat key:*\n"
                "Paradex app → klik address kanan atas → *Export Private Key*"
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

            _executor.sync_all()
            eth_pos = _executor.get_live_position(MARKET_ETH)
            btc_pos = _executor.get_live_position(MARKET_BTC)

            def _pos_line(p, label):
                if not p:
                    return f"│ {label}: — tidak ada posisi\n"
                sign = "+" if p["unrealized_pnl"] >= 0 else ""
                return (
                    f"│ {label}: *{p['side']}* {abs(p['size'])} "
                    f"@ ${p['avg_entry']:,.2f} | Lev: {p['leverage']:.0f}x | "
                    f"UPnL: *{sign}${p['unrealized_pnl']:,.2f}*\n"
                )

            pos_block = (
                f"│\n│ 📍 *Posisi Aktif:*\n"
                + _pos_line(eth_pos, "ETH")
                + _pos_line(btc_pos, "BTC")
            ) if (eth_pos or btc_pos) else "│\n│ 📍 Posisi: — tidak ada\n"

            live_str = "🟢 AKTIF" if active else "🔴 OFF"
            dr_str   = " 🧪 DRYRUN" if dryrun else ""
            reply(
                f"✅ *Paradex Dashboard*{dr_str}\n"
                f"┌─────────────────────\n"
                f"│ Account: `{_executor.account_address[:18]}...`\n"
                f"{bal_str}"
                f"│\n│ ⚙️ *Trading Settings:*\n"
                f"│ Status:     {live_str}\n"
                f"│ Margin:     *${margin:,.2f}* per pair (risiko)\n"
                f"│ Leverage:   *{lev:.0f}x*\n"
                f"│ Notional:   ~*${notional:,.2f}*/pair\n"
                f"│ Order type: {otype}\n"
                f"│ Dryrun:     {'ON 🧪' if dryrun else 'OFF'}\n"
                f"{pos_block}"
                f"└─────────────────────\n"
                f"_`/pdx balance` | `/pdx fills` | `/pdx sync`_\n"
                f"_`/pdx cancel` — cancel orders | `/pdx close` — close posisi_\n"
                f"_`/pdx preview <btc> <eth>` | `/pdx lev` — cek leverage_\n"
                f"_`/live on|off` | `/live margin` | `/live lev`_"
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

        if args[1].lower() in ("l1", "l2"):
            mode = args[1].lower()
            if len(args) < 4:
                reply("⚠️ Kurang argumen. Contoh: `/pdx init l2 <key> <address>`")
                return
            key     = args[2]
            address = args[3]
        else:
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

    # ── Close ALL positions ← [NEW] ──────────────────────────────
    elif sub == "close":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        reply("⏳ Menutup semua posisi...")
        msg = live_close_all_command()
        reply(msg)

    # ── Leverage check/set ← [NEW] ────────────────────────────────
    elif sub == "lev":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return

        # Kalau ada arg kedua = set leverage sekarang
        if len(args) >= 2:
            try:
                lev_val = float(args[1])
                if not (1 <= lev_val <= 200):
                    reply("Leverage harus 1–200x."); return
                live_settings["leverage"] = lev_val
                eth_ok = _executor.set_leverage(MARKET_ETH, lev_val)
                btc_ok = _executor.set_leverage(MARKET_BTC, lev_val)
                reply(
                    f"✅ *Leverage diupdate: {lev_val:.0f}x*\n"
                    f"ETH: {'✅' if eth_ok else '❌'} | BTC: {'✅' if btc_ok else '❌'}\n"
                    f"_Cek posisi di Paradex UI untuk konfirmasi_"
                )
            except ValueError:
                reply("Angkanya tidak valid. Contoh: `/pdx lev 40`")
        else:
            # Tampilkan leverage dari posisi aktif
            _executor.sync_all()
            eth_pos = _executor.get_live_position(MARKET_ETH)
            btc_pos = _executor.get_live_position(MARKET_BTC)
            eth_lev = eth_pos.get("leverage", "?") if eth_pos else "No pos"
            btc_lev = btc_pos.get("leverage", "?") if btc_pos else "No pos"
            lev_set = live_settings["leverage"]
            reply(
                f"📊 *Leverage Status*\n"
                f"┌─────────────────────\n"
                f"│ Setting bot: *{lev_set:.0f}x*\n"
                f"│ ETH aktif:   {eth_lev}x\n"
                f"│ BTC aktif:   {btc_lev}x\n"
                f"└─────────────────────\n"
                f"_Untuk set: `/pdx lev <nilai>` atau `/live lev <nilai>`_"
            )

    # ── Preview sizing ───────────────────────────────────────────
    elif sub == "preview":
        if len(args) < 3:
            reply(
                "Usage: `/pdx preview <btc_price> <eth_price>`\n"
                "Contoh: `/pdx preview 85000 2100`"
            ); return
        try:
            btc_p = float(args[1])
            eth_p = float(args[2])
            reply(f"📐 *Order Size Preview:*\n{calc_order_preview(btc_p, eth_p)}")
        except ValueError:
            reply("Harga tidak valid.")

    # ── Balance ─────────────────────────────────────────────────
    elif sub == "balance":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
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
            + "\n".join(lines)
            + f"\n└─────────────────────\n"
        )

    # ── Cancel open orders (bukan close posisi) ──────────────────
    elif sub == "cancel":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        ok = _executor.cancel_all_orders()
        reply(
            "✅ *Semua open orders dibatalkan.*\n"
            "_💡 Ini hanya cancel pending orders, bukan close posisi._\n"
            "_Untuk close posisi: `/pdx close`_"
            if ok else "❌ Gagal cancel orders."
        )

    # ── Sync ────────────────────────────────────────────────────
    elif sub == "sync":
        if _executor is None or not _executor.is_ready():
            reply("⚠️ Paradex belum terhubung."); return
        _executor.sync_all()
        eth_pos = _executor.get_live_position(MARKET_ETH)
        btc_pos = _executor.get_live_position(MARKET_BTC)
        def _pos_str(p, label):
            if not p:
                return f"│ {label}: Tidak ada posisi\n"
            return (
                f"│ {label}: {p['side']} {abs(p['size'])} "
                f"@ ${p['avg_entry']:,.2f} | Lev: {p['leverage']:.0f}x | "
                f"UPnL: ${p['unrealized_pnl']:+,.2f}\n"
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
            "*Usage:*\n"
            "`/pdx` — status\n"
            "`/pdx init l2 <key> <addr>` — connect\n"
            "`/pdx balance` | `/pdx fills` | `/pdx sync`\n"
            "`/pdx cancel` — cancel pending orders\n"
            "`/pdx close` — *close semua posisi*\n"
            "`/pdx lev [nilai]` — cek / set leverage\n"
            "`/pdx preview <btc> <eth>` — preview sizing"
        )


# ─────────────────────────────────────────────────────────────────────────────
# /live command handler
# ─────────────────────────────────────────────────────────────────────────────

def handle_live_command(args: list, chat_id: str, send_reply_fn=None):
    """
    /live                    — status
    /live on|off             — toggle
    /live margin <usd>       — set margin per pair (yang kamu risiko)
    /live lev <n>            — set leverage + apply ke exchange sekarang
    /live type market|limit  — order type
    /live dryrun on|off      — dryrun mode
    """
    def reply(msg):
        if send_reply_fn:
            send_reply_fn(msg, chat_id)

    sub    = args[0].lower() if args else "status"
    margin = live_settings["margin_usd"]
    lev    = live_settings["leverage"]
    otype  = live_settings["order_type"]
    dryrun = live_settings["dryrun"]

    if sub in ("status", ""):
        connected = _executor is not None and _executor.is_ready()
        active    = live_settings["active"]
        notional  = margin * lev
        mode_val  = live_settings.get("mode", "normal")
        mode_str  = "🔵 NORMAL" if mode_val == "normal" else "🟣 MODE X"
        mode_hint = (
            "S1: Long BTC/Short ETH | S2: Long ETH/Short BTC"
            if mode_val == "normal" else
            "S1: Short ETH+BTC | S2: Long ETH+BTC"
        )
        dr_str    = " 🧪 *DRYRUN*" if dryrun else ""
        reply(
            f"🔴 *Live Trading Settings*{dr_str}\n\n"
            f"┌─────────────────────\n"
            f"│ Status:     {'🟢 ON' if active else '🔴 OFF'}\n"
            f"│ Paradex:    {'✅ Connected' if connected else '❌ Not connected'}\n"
            f"│ Mode:       *{mode_str}*\n"
            f"│ {mode_hint}\n"
            f"│ Margin:     *${margin:,.2f}* per pair (risiko)\n"
            f"│ Leverage:   *{lev:.0f}x*\n"
            f"│ Notional:   ~*${notional:,.2f}*/pair\n"
            f"│ Total used: ~*${margin * 2:,.2f}* (2 pairs)\n"
            f"│ Order type: {otype}\n"
            f"│ Dryrun:     {'ON 🧪' if dryrun else 'OFF'}\n"
            f"└─────────────────────\n\n"
            f"💡 _`/pdx preview <btc> <eth>` untuk lihat qty_\n\n"
            f"*Commands:*\n"
            f"`/live on|off` | `/live margin <usd>`\n"
            f"`/live lev <n>` | `/live type market|limit`\n"
            f"`/live dryrun on|off` | `/live mode normal|x`"
        )

    elif sub == "on":
        if _executor is None or not _executor.is_ready():
            reply(
                "⚠️ *Paradex belum terhubung!*\n"
                "Gunakan dulu: `/pdx init l2 <key> <address>`"
            ); return
        live_settings["active"] = True
        notional = margin * lev
        dr_note  = "\n🧪 *Dryrun mode ON* — tidak ada order sungguhan." if dryrun else ""
        mode_val = live_settings.get("mode", "normal")
        mode_str = "🔵 NORMAL" if mode_val == "normal" else "🟣 MODE X"
        reply(
            f"🟢 *Live trading AKTIF!*{dr_note}\n\n"
            f"Mode: *{mode_str}*\n"
            f"Margin: *${margin:,.2f}*/pair | Lev: *{lev:.0f}x*\n"
            f"Notional: ~*${notional:,.2f}*/pair\n"
            f"Total margin 2 pair: *${margin * 2:,.2f}*\n\n"
            f"_Bot akan kirim order ke Paradex saat sinyal._\n\n"
            f"⚠️ Pastikan balance ≥ ${margin * 2:,.2f}!"
        )

    elif sub == "off":
        live_settings["active"] = False
        reply("🔴 *Live trading DIMATIKAN.* Bot kembali ke mode simulasi.")

    elif sub == "margin":
        if len(args) < 2:
            notional = margin * lev
            reply(
                f"💰 *Margin sekarang: ${margin:,.2f}/pair*\n"
                f"Notional: ${notional:,.2f}/pair\n\n"
                f"_margin = modal yang kamu RISIKO per pair_\n"
                f"_notional = margin × leverage = nilai posisi_\n\n"
                f"Usage: `/live margin <usd>`"
            ); return
        try:
            val = float(args[1])
            if val < 1 or val > 100_000:
                reply("Margin harus antara $1 – $100,000."); return
            live_settings["margin_usd"] = val
            notional_new = val * lev
            reply(
                f"✅ *Margin: ${val:,.2f}/pair*\n"
                f"Notional: ~${notional_new:,.2f}/pair (× {lev:.0f}x)\n"
                f"Total margin 2 pair: ${val * 2:,.2f}"
            )
        except ValueError:
            reply("Angkanya tidak valid.")

    elif sub == "lev":
        if len(args) < 2:
            reply(
                f"📊 Leverage sekarang: *{lev:.0f}x*\n"
                f"Usage: `/live lev <n>`\n\n"
                f"_💡 Ini juga set leverage langsung ke exchange kalau Paradex terhubung_"
            ); return
        try:
            val = float(args[1])
            if not (1 <= val <= 200):
                reply("Leverage harus 1x – 200x."); return
            live_settings["leverage"] = val
            notional_new = margin * val
            reply_msg = (
                f"✅ *Leverage: {val:.0f}x*\n"
                f"Notional: ~${notional_new:,.2f}/pair"
            )
            # Apply ke exchange sekarang juga jika connected
            if _executor and _executor.is_ready():
                eth_ok = _executor.set_leverage(MARKET_ETH, val)
                btc_ok = _executor.set_leverage(MARKET_BTC, val)
                reply_msg += (
                    f"\n\n*Apply ke exchange:*\n"
                    f"ETH: {'✅' if eth_ok else '❌'} | BTC: {'✅' if btc_ok else '❌'}"
                )
            reply(reply_msg)
        except ValueError:
            reply("Angkanya tidak valid.")

    elif sub == "type":
        if len(args) < 2:
            reply(f"Order type sekarang: *{otype}*\nUsage: `/live type market|limit`"); return
        t = args[1].upper()
        if t not in ("MARKET", "LIMIT"):
            reply("Harus `market` atau `limit`."); return
        live_settings["order_type"] = t
        reply(f"✅ Order type: *{t}*")

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

    elif sub == "mode":
        current_mode_val = live_settings.get("mode", "normal")
        if len(args) < 2:
            _mode_desc = (
                "🔵 *NORMAL* — Pair divergence (satu long, satu short)"
                if current_mode_val == "normal" else
                "🟣 *MODE X* — Momentum (BTC ikut arah ETH)"
            )
            reply(
                f"⚙️ *Mode Trading Saat Ini:* {_mode_desc}\n\n"
                f"*Mode NORMAL (default — divergence):*\n"
                f"│ S1: Long BTC / Short ETH\n"
                f"│ S2: Long ETH / Short BTC\n"
                f"│ → Pairs trade, taruhan gap konvergen\n\n"
                f"*Mode X (searah — keduanya 1 arah):*\n"
                f"│ S1 (ETH pump > BTC): *Short ETH + Short BTC*\n"
                f"│ S2 (ETH dump > BTC): *Long ETH + Long BTC*\n"
                f"│ → Tidak ada hedge, keduanya sama arah\n\n"
                f"Ganti mode: `/live mode normal` atau `/live mode x`"
            )
            return
        val = args[1].lower()
        if val in ("normal", "n"):
            live_settings["mode"] = "normal"
            reply(
                "🔵 *Mode NORMAL diaktifkan*\n\n"
                "│ S1: Long BTC / Short ETH\n"
                "│ S2: Short BTC / Long ETH\n\n"
                "_Strategi pair divergence — gap akan konvergen._"
            )
        elif val in ("x", "momentum"):
            live_settings["mode"] = "x"
            reply(
                "🟣 *Mode X diaktifkan*\n\n"
                "│ S1 (ETH pump > BTC): *Short ETH + Short BTC*\n"
                "│ S2 (ETH dump > BTC): *Long ETH + Long BTC*\n\n"
                "_Kedua posisi searah — tidak ada hedge._\n"
                "⚠️ _Risiko lebih tinggi dari Mode Normal._"
            )
        else:
            reply("Gunakan `/live mode normal` atau `/live mode x`.")

    else:
        reply(
            "❓ Command tidak dikenal.\n\n"
            "*Usage:*\n`/live` — status\n`/live on|off` — toggle\n"
            "`/live margin <usd>` | `/live lev <n>`\n"
            "`/live type market|limit` | `/live dryrun on|off`\n"
            "`/live mode normal|x` — ganti mode trading"
        )

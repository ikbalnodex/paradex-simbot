"""
PATCH FILE untuk monk_bot_b.py
================================
File ini menjelaskan perubahan yang perlu diterapkan ke monk_bot_b.py
untuk mengaktifkan fitur auto_close toggle.

PERUBAHAN 1: Tambah helper function _exec_live_close()
PERUBAHAN 2: Ganti 7 blok live_close_or_sim di check_sltp() dan evaluate_and_transition()
PERUBAHAN 3: Update /help dan /settings untuk tampilkan status auto_close

=======================================================================
CARA PAKAI:
Terapkan perubahan ini ke monk_bot_b.py sesuai instruksi di bawah.
=======================================================================
"""

# =============================================================================
# PERUBAHAN 1 — Tambahkan helper function ini
# Letakkan setelah fungsi reset_to_scan() di monk_bot_b.py
# =============================================================================

def _exec_live_close(strategy_val: str, btc_price: float, eth_price: float, reason: str):
    """
    Helper terpusat untuk eksekusi live close dengan cek auto_close.

    Jika auto_close ON  → eksekusi close order ke Paradex + kirim notif
    Jika auto_close OFF → hanya kirim notifikasi sinyal, tidak eksekusi close

    Return: pesan yang sudah dikirim (str) atau "" jika tidak ada
    """
    from paradex_live import live_close_or_sim, live_settings, is_live_active

    if not is_live_active():
        return ""

    auto_close_on = live_settings.get("auto_close", True)
    reason_emoji  = {"EXIT": "✅", "TP": "🎯", "TSL": "⛔", "INVALID": "⚠️"}.get(reason, "🔴")

    if auto_close_on:
        # Auto-close ON: eksekusi close order
        exec_msg = live_close_or_sim(
            strategy_val, btc_price, eth_price,
            reason=reason, sim_fn=None,
        )
        if exec_msg:
            send_alert(exec_msg)
        return exec_msg or ""
    else:
        # Auto-close OFF: hanya notifikasi, tidak eksekusi
        notif_msg = (
            f"\n⚠️ *[LIVE] Auto-Close OFF — Sinyal {reason_emoji} {reason} Terdeteksi!*\n"
            f"┌─────────────────────\n"
            f"│ Strategi: *{strategy_val}*\n"
            f"│ Sinyal:   *{reason}*\n"
            f"│ Auto-close dinonaktifkan — posisi TIDAK otomatis ditutup.\n"
            f"└─────────────────────\n"
            f"_Tutup manual: `/pdx close` atau UI Paradex._\n"
            f"_Aktifkan auto-close: `/live autoclose on`_"
        )
        send_alert(notif_msg)
        return notif_msg


# =============================================================================
# PERUBAHAN 2A — Di check_sltp(), ganti SEMUA 4 blok ini:
# =============================================================================

# ── SEBELUM (TP S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP", sim_fn=None)
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SESUDAH (TP S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SEBELUM (TSL S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL", sim_fn=None)
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SESUDAH (TSL S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SEBELUM (TP S2) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP", sim_fn=None)
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SESUDAH (TP S2) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TP")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SEBELUM (TSL S2) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL", sim_fn=None)
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""

# ── SESUDAH (TSL S2) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
else:
    sim_msg = sim_close_position(float(scan_stats["last_btc_price"]), float(scan_stats["last_eth_price"]), reason="TSL")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return True
"""


# =============================================================================
# PERUBAHAN 2B — Di evaluate_and_transition(), ganti 3 blok EXIT + INVALID:
# =============================================================================

# ── SEBELUM (EXIT di blok in_exit_zone) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(
        active_strategy.value, float(btc_now), float(eth_now),
        reason="EXIT", sim_fn=None,
    )
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="EXIT")
    if sim_msg: send_alert(sim_msg)
"""

# ── SESUDAH (EXIT) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(btc_now), float(eth_now), reason="EXIT")
else:
    sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="EXIT")
    if sim_msg: send_alert(sim_msg)
"""

# ── SEBELUM (INVALID S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    exec_msg = live_close_or_sim(active_strategy.value, float(btc_now), float(eth_now), reason="INVALID", sim_fn=None)
    if exec_msg: send_alert(exec_msg)
else:
    sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return
"""

# ── SESUDAH (INVALID S1) ──
"""
if PARADEX_LIVE_AVAILABLE and is_live_active():
    _exec_live_close(active_strategy.value, float(btc_now), float(eth_now), reason="INVALID")
else:
    sim_msg = sim_close_position(float(btc_now), float(eth_now), reason="INVALID")
    if sim_msg: send_alert(sim_msg)
reset_to_scan(); return
"""

# ── SEBELUM (INVALID S2) — sama persis dengan INVALID S1 ──
# ── SESUDAH (INVALID S2) — sama persis dengan INVALID S1 ──


# =============================================================================
# PERUBAHAN 3 — Update handle_settings_command()
# Tambahkan baris auto_close di blok live settings
# =============================================================================

# Di dalam handle_settings_command(), cari baris ini:
"""
    live_s     = "🟢 AKTIF" if is_live_active() else "🔴 OFF"
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    mode_s     = "🟣 Mode X" if _live_mode == "x" else "🔵 Normal"
    send_reply(
        ...
        f"🔴 Live Trading:    {live_s} | {mode_s}\n\n"
        ...
    )
"""

# Ganti dengan:
"""
    live_s     = "🟢 AKTIF" if is_live_active() else "🔴 OFF"
    _live_mode = live_settings.get("mode", "normal") if PARADEX_LIVE_AVAILABLE else "normal"
    mode_s     = "🟣 Mode X" if _live_mode == "x" else "🔵 Normal"
    _ac_on     = live_settings.get("auto_close", True) if PARADEX_LIVE_AVAILABLE else True
    ac_s       = "✅ ON" if _ac_on else "❌ OFF (manual)"
    send_reply(
        ...
        f"🔴 Live Trading:    {live_s} | {mode_s}\n"
        f"🔄 Auto-Close:      {ac_s}\n\n"
        ...
    )
"""


# =============================================================================
# PERUBAHAN 4 — Update handle_help_command()
# Tambahkan baris /live autoclose di bagian Paradex Live Trading
# =============================================================================

# Cari baris ini di handle_help_command():
"""
        f"`/live dryrun on|off`     — mode uji coba\n\n"
"""

# Ganti dengan:
"""
        f"`/live dryrun on|off`     — mode uji coba\n"
        f"`/live autoclose on|off`  — *toggle auto-close posisi*\n\n"
"""


# =============================================================================
# RINGKASAN PERUBAHAN
# =============================================================================
"""
Total perubahan di monk_bot_b.py:

1. Tambah fungsi _exec_live_close() setelah reset_to_scan()
   → Helper terpusat: cek auto_close, eksekusi atau kirim notif

2. check_sltp() — 4 titik:
   - TP S1   : ganti blok PARADEX_LIVE_AVAILABLE
   - TSL S1  : ganti blok PARADEX_LIVE_AVAILABLE  
   - TP S2   : ganti blok PARADEX_LIVE_AVAILABLE
   - TSL S2  : ganti blok PARADEX_LIVE_AVAILABLE

3. evaluate_and_transition() — 3 titik:
   - EXIT    : ganti blok PARADEX_LIVE_AVAILABLE
   - INVALID S1 : ganti blok PARADEX_LIVE_AVAILABLE
   - INVALID S2 : ganti blok PARADEX_LIVE_AVAILABLE

4. handle_settings_command() — tambah baris auto_close
5. handle_help_command() — tambah baris /live autoclose

TIDAK ADA perubahan logic lainnya.
Default auto_close = True → perilaku lama tetap berjalan normal.
"""

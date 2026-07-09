"""
SOS GAMMA/SQUEEZE ENGINE — expiry premium-explosion detection
Detects, both CE (bullish) and PE (bearish) side:
  1. GAMMA BLAST      — morning compression -> post-1:45 directional break, premium 2-3x
  2. SHORT COVERING   — premium up + OI FALLING + price into strike (writers trapped)
  3. WRITING PRESSURE — premium down + OI RISING (wall building = fade/resistance)
  4. UNWINDING        — premium down + OI FALLING (positions exiting, continuation fuel)

Uses full-mode ticks (LTP + OI). Pure-logic; caller feeds tick history + session context.
Research-grounded thresholds (Nifty gamma-blast literature):
  - compression = spot intraday range < 1.0% by 13:45
  - time gate = post 13:45 IST on expiry (DTE 0)
  - premium multiplier = 2x+ from a meaningful base (>= floor), NOT raw %ratio
"""

from datetime import time as dtime

# ── thresholds ──
PREMIUM_FLOOR      = 20.0    # ignore options under Rs.20 (cheap-option %-explosion artifact)
MIN_RUPEE_MOVE     = 8.0     # premium must move at least Rs.8 in the window
BLAST_MULT         = 1.6     # premium >= 1.6x over the blast window = accelerating (2-3x is full blast)
OI_FALL_PCT        = 3.0     # OI down >=3% in window = covering/unwinding
OI_RISE_PCT        = 3.0     # OI up   >=3% = fresh writing
COMPRESSION_PCT    = 1.0     # morning spot range < 1.0% = coiled
GATE_TIME          = dtime(13, 45)   # post this IST on expiry
MIN_SPOT_BREAK_PCT = 0.10    # spot must break the morning range by this % to confirm release


def pct_change(old, new):
    if old is None or old == 0:
        return None
    return (new - old) / old * 100


def classify(prem_old, prem_new, oi_old, oi_new, spot_dir, opt_type):
    """
    Returns (event_type, bias, detail) or None.
    spot_dir: +1 up, -1 down, 0 flat. opt_type: 'CE'|'PE'.
    """
    if prem_new < PREMIUM_FLOOR:
        return None
    prem_pct = pct_change(prem_old, prem_new)
    oi_pct = pct_change(oi_old, oi_new)
    if prem_pct is None:
        return None
    rupee_move = prem_new - prem_old

    # direction alignment: CE bullish (spot up), PE bearish (spot down)
    aligned = (opt_type == "CE" and spot_dir > 0) or (opt_type == "PE" and spot_dir < 0)
    bias = "BULLISH" if opt_type == "CE" else "BEARISH"

    # ---- premium RISING events ----
    if prem_pct > 0 and rupee_move >= MIN_RUPEE_MOVE:
        mult = prem_new / prem_old if prem_old else 0
        if oi_pct is not None and oi_pct <= -OI_FALL_PCT and aligned:
            # premium up + OI down + move into strike = SHORT COVERING (strongest)
            return ("SHORT COVERING", bias,
                    f"prem {mult:.2f}x, OI {oi_pct:+.1f}% (writers exiting)")
        if mult >= BLAST_MULT and aligned:
            # premium 1.6x+ with directional break = GAMMA BLAST
            return ("GAMMA BLAST", bias,
                    f"prem {mult:.2f}x in window, OI {oi_pct:+.1f}%")
        # premium up + OI up = fresh buying (weaker; report only if strong multiplier)
        if mult >= BLAST_MULT and oi_pct is not None and oi_pct >= OI_RISE_PCT and aligned:
            return ("FRESH BUYING", bias,
                    f"prem {mult:.2f}x, OI {oi_pct:+.1f}% (new longs — weaker)")

    # ---- premium FALLING events (structural, direction from opt side) ----
    if prem_pct is not None and prem_pct < 0 and oi_pct is not None:
        if oi_pct >= OI_RISE_PCT:
            # premium down + OI up = fresh WRITING = wall building on this strike
            # CE writing = bearish pressure above; PE writing = bullish pressure below
            wbias = "BEARISH" if opt_type == "CE" else "BULLISH"
            return ("WRITING PRESSURE", wbias,
                    f"OI {oi_pct:+.1f}% building on {opt_type} (wall/resistance)")
    return None


def compression_state(day_high, day_low, ref_price):
    """Morning coil check: intraday range as % of price."""
    if not ref_price:
        return None, 0.0
    rng_pct = (day_high - day_low) / ref_price * 100 if ref_price else 0
    return (rng_pct < COMPRESSION_PCT), round(rng_pct, 2)


def in_gate(now_ist_time, dte):
    """Expiry-day post-1:45 gate for classic gamma blast."""
    return dte == 0 and now_ist_time >= GATE_TIME


def spot_broke_range(spot, day_high, day_low, ref_price):
    """Did spot break out of the morning compression range (release)?"""
    up = spot > day_high * (1 + MIN_SPOT_BREAK_PCT / 100)
    dn = spot < day_low * (1 - MIN_SPOT_BREAK_PCT / 100)
    if up:
        return +1
    if dn:
        return -1
    return 0

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


def classify(prem_old, prem_new, oi_old, oi_new, spot_dir, opt_type, strike=None, spot=None):
    """
    Returns (event_type, bias, detail) or None.
    spot_dir: +1 up, -1 down, 0 flat. opt_type: 'CE'|'PE'.
    strike/spot: used to require OTM for wall logic (ITM writing != resistance).
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

    # OTM check: CE is OTM when strike > spot; PE is OTM when strike < spot.
    # Wall/writing logic is only meaningful for OTM strikes.
    is_otm = True
    if strike is not None and spot is not None:
        is_otm = (opt_type == "CE" and strike > spot) or (opt_type == "PE" and strike < spot)

    # ---- premium RISING events ----
    if prem_pct > 0 and rupee_move >= MIN_RUPEE_MOVE:
        mult = prem_new / prem_old if prem_old else 0
        if oi_pct is not None and oi_pct <= -OI_FALL_PCT and aligned:
            return ("SHORT COVERING", bias,
                    f"prem {mult:.2f}x, OI {oi_pct:+.1f}% (writers exiting)")
        if mult >= BLAST_MULT and aligned:
            return ("GAMMA BLAST", bias,
                    f"prem {mult:.2f}x in window, OI {oi_pct:+.1f}%")
        if mult >= BLAST_MULT and oi_pct is not None and oi_pct >= OI_RISE_PCT and aligned:
            return ("FRESH BUYING", bias,
                    f"prem {mult:.2f}x, OI {oi_pct:+.1f}% (new longs — weaker)")

    # ---- premium FALLING + OI RISING = fresh writing (wall) — OTM only ----
    if prem_pct is not None and prem_pct < 0 and oi_pct is not None:
        if oi_pct >= OI_RISE_PCT and is_otm:
            # CE writing (OTM, above spot) = bearish resistance
            # PE writing (OTM, below spot) = bullish support
            wbias = "BEARISH" if opt_type == "CE" else "BULLISH"
            return ("WRITING PRESSURE", wbias,
                    f"OI {oi_pct:+.1f}% building on OTM {opt_type} (wall)")
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


# ─────────────────────────────────────────────────────────
# GUIDANCE + RUNNING SITUATION
# ─────────────────────────────────────────────────────────
def event_guidance(event_type, bias, opt_type, strike, spot):
    """One-line READ + WATCH per event. Context guidance, never an entry call."""
    if event_type == "WRITING PRESSURE":
        if opt_type == "CE":
            return (f"READ: resistance ceiling building at {strike:.0f}.",
                    f"WATCH: break ABOVE {strike:.0f} = bullish release. Holding below = capped.")
        else:
            return (f"READ: support floor building at {strike:.0f}.",
                    f"WATCH: break BELOW {strike:.0f} = bearish release. Holding above = supported.")
    if event_type == "SHORT COVERING":
        return (f"READ: {opt_type} writers trapped, covering — fuel for {'up' if opt_type=='CE' else 'down'}move.",
                f"WATCH: continuation while covering persists. Fades when OI stops falling.")
    if event_type == "GAMMA BLAST":
        return (f"READ: coil released, premium accelerating {bias.lower()}.",
                f"WATCH: ride toward next strike; take fast — gamma round-trips hard.")
    if event_type == "PREMIUM SURGE":
        return (f"READ: {opt_type} premium accelerating but not full expiry-coil blast.",
                f"WATCH: confirm with spot break before trusting.")
    if event_type == "FRESH BUYING":
        return (f"READ: new {opt_type} longs (weaker — could be trapped).",
                f"WATCH: needs OI to keep rising + spot follow-through.")
    return ("", "")


def build_situation(struct, spot, day_range, compressed, rng_pct):
    """Running summary of the accumulated structure for this index."""
    ce_wall = struct.get("ce_wall")
    pe_wall = struct.get("pe_wall")
    lines = ["── SITUATION ──"]

    if ce_wall and pe_wall:
        band = ce_wall - pe_wall
        lines.append(f"Coil {pe_wall:.0f} — {ce_wall:.0f} ({band:.0f}pt band), spot {spot:.0f}")
    elif ce_wall:
        lines.append(f"Resistance {ce_wall:.0f}, spot {spot:.0f} (no floor mapped yet)")
    elif pe_wall:
        lines.append(f"Support {pe_wall:.0f}, spot {spot:.0f} (no ceiling mapped yet)")
    else:
        lines.append(f"Spot {spot:.0f} — walls still forming")

    # position within coil
    if ce_wall and pe_wall and ce_wall > pe_wall:
        pos = (spot - pe_wall) / (ce_wall - pe_wall) * 100
        if pos > 70:
            lines.append(f"Spot near ceiling ({pos:.0f}% up the band) — watch {ce_wall:.0f} break for upside.")
        elif pos < 30:
            lines.append(f"Spot near floor ({pos:.0f}% up the band) — watch {pe_wall:.0f} break for downside.")
        else:
            lines.append(f"Spot mid-band ({pos:.0f}%) — pinned; wait for edge break.")

    lines.append(f"Morning range {rng_pct}% {'(compressed — coil intact)' if compressed else '(range expanded)'}")

    # net read
    if compressed and ce_wall and pe_wall:
        lines.append("NET: expiry coil. Break of either wall = the trade. No entry mid-band.")
    elif not compressed:
        lines.append("NET: range already expanded — gamma-coil setup weaker today.")
    return "\n".join(lines)

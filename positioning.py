"""
SOS POSITIONING — the desks' view, built in.
1. Daily futures OI quadrant (day-over-day):  price↑+OI↑ LONG BUILDUP · price↑+OI↓ SHORT COVERING
   price↓+OI↑ SHORT BUILDUP · price↓+OI↓ LONG UNWINDING
2. Multi-day trend (last 5 sessions) so "short covering since Friday" is visible.
3. Live intraday quadrant: price & OI change since open, evaluated continuously.
"""

def quadrant(price_chg_pct, oi_chg_pct, min_move=0.05):
    """Classic futures OI quadrant. Returns (label, bias, emoji)."""
    if abs(price_chg_pct) < min_move and abs(oi_chg_pct) < 0.3:
        return ("FLAT", "NEUTRAL", "⚪")
    if price_chg_pct >= 0 and oi_chg_pct >= 0:
        return ("LONG BUILDUP", "BULLISH", "🟢")
    if price_chg_pct >= 0 and oi_chg_pct < 0:
        return ("SHORT COVERING", "BULLISH", "🟢")
    if price_chg_pct < 0 and oi_chg_pct >= 0:
        return ("SHORT BUILDUP", "BEARISH", "🔴")
    return ("LONG UNWINDING", "BEARISH", "🔴")


def daily_brief(daily_candles, symbol):
    """
    daily_candles: list of dicts {date, close, oi} (chronological, >=2, ideally 6).
    Returns brief text block for this symbol with day-over-day quadrants + net view.
    """
    if not daily_candles or len(daily_candles) < 2:
        return f"{symbol}: not enough daily data."

    rows = []
    labels = []
    for prev, cur in zip(daily_candles, daily_candles[1:]):
        if not prev.get("close") or not prev.get("oi"):
            continue
        p_chg = (cur["close"] - prev["close"]) / prev["close"] * 100
        o_chg = (cur["oi"] - prev["oi"]) / prev["oi"] * 100 if prev["oi"] else 0
        label, bias, dot = quadrant(p_chg, o_chg)
        d = cur["date"].strftime("%d-%b") if hasattr(cur["date"], "strftime") else str(cur["date"])[:10]
        rows.append(f"{d}: {dot} {label}  (px {p_chg:+.1f}% · OI {o_chg:+.1f}%)")
        labels.append((label, bias))

    if not labels:
        return f"{symbol}: no usable OI data."

    # net view: latest day dominates; streak strengthens it
    last_label, last_bias = labels[-1]
    streak = 1
    for l, b in reversed(labels[:-1]):
        if b == last_bias and b != "NEUTRAL":
            streak += 1
        else:
            break

    dot = "🟢" if last_bias == "BULLISH" else "🔴" if last_bias == "BEARISH" else "⚪"
    if last_bias == "NEUTRAL":
        view = f"VIEW: ⚪ no clear positional bias."
    elif streak >= 2:
        view = (f"VIEW: {dot} {last_bias} — {last_label} running {streak} sessions. "
                f"Carry this bias; {'dips buyable' if last_bias == 'BULLISH' else 'rallies sellable'} until quadrant flips.")
    else:
        view = f"VIEW: {dot} {last_bias} lean — {last_label} (1 session, unconfirmed)."

    return f"— {symbol} FUTURES —\n" + "\n".join(rows[-5:]) + f"\n{view}"


def intraday_quadrant(open_price, cur_price, open_oi, cur_oi):
    """Live since-open quadrant. Returns (label, bias, dot, p_chg, o_chg) or None."""
    if not open_price or not open_oi:
        return None
    p_chg = (cur_price - open_price) / open_price * 100
    o_chg = (cur_oi - open_oi) / open_oi * 100 if open_oi else 0
    label, bias, dot = quadrant(p_chg, o_chg, min_move=0.15)
    return (label, bias, dot, round(p_chg, 2), round(o_chg, 2))

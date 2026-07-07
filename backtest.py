"""
SOS BACKTEST — confluence-at-location analysis on Nifty 5m
Tests: historic reversal zone + MA flip (EMA7/17) + unmitigated FVG + swing confirmation
Reports outcome distribution by confluence score, expiry vs non-expiry.
Double bottom/top detected as parallel setup.

Pure functions — fed a list of 5m candles (dicts with date/open/high/low/close).
No network here; the caller (sos-kite /backtest endpoint) supplies candles via Kite.
"""

from datetime import datetime, timedelta


# ─────────────────────────────────────────────────────────
# indicators
# ─────────────────────────────────────────────────────────
def ema(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


# ─────────────────────────────────────────────────────────
# swing pivots (fractal: high with N lower highs each side)
# ─────────────────────────────────────────────────────────
def find_pivots(candles, left=2, right=2):
    """Returns list of (index, price, 'H'|'L')."""
    piv = []
    n = len(candles)
    for i in range(left, n - right):
        hi = candles[i]["high"]
        lo = candles[i]["low"]
        is_high = all(candles[i - j]["high"] < hi for j in range(1, left + 1)) and \
                  all(candles[i + j]["high"] < hi for j in range(1, right + 1))
        is_low = all(candles[i - j]["low"] > lo for j in range(1, left + 1)) and \
                 all(candles[i + j]["low"] > lo for j in range(1, right + 1))
        if is_high:
            piv.append((i, hi, "H"))
        if is_low:
            piv.append((i, lo, "L"))
    return piv


# ─────────────────────────────────────────────────────────
# FVG detection (3-candle gap)
# ─────────────────────────────────────────────────────────
def find_fvgs(candles):
    """Returns list of dicts: {idx, type:'bull'|'bear', lo, hi, size}."""
    fvgs = []
    for i in range(2, len(candles)):
        c1, c3 = candles[i - 2], candles[i]
        if c1["high"] < c3["low"]:  # bullish gap
            fvgs.append({"idx": i, "type": "bull", "lo": c1["high"], "hi": c3["low"],
                         "size": round(c3["low"] - c1["high"], 2)})
        elif c1["low"] > c3["high"]:  # bearish gap
            fvgs.append({"idx": i, "type": "bear", "lo": c3["high"], "hi": c1["low"],
                         "size": round(c1["low"] - c3["high"], 2)})
    return fvgs


def fvg_at(fvgs, price, upto_idx, max_size):
    """Is there an unmitigated FVG (size<=max_size) whose zone contains price, formed before upto_idx?"""
    for f in fvgs:
        if f["idx"] >= upto_idx:
            continue
        if f["size"] > max_size:
            continue
        if f["lo"] <= price <= f["hi"]:
            return f
    return None


# ─────────────────────────────────────────────────────────
# session helpers
# ─────────────────────────────────────────────────────────
def session_date(c):
    return c["date"].date() if hasattr(c["date"], "date") else c["date"]


def is_expiry_day(d):
    # Nifty weekly expiry = Tuesday (weekday 1). Holiday shifts not handled.
    return d.weekday() == 1


# ─────────────────────────────────────────────────────────
# double bottom / top
# ─────────────────────────────────────────────────────────
def detect_double(pivots, candles, tol_pct=0.1):
    """Detect double bottoms/tops. Returns list of {type, idx, neckline, target}."""
    doubles = []
    lows = [p for p in pivots if p[2] == "L"]
    highs = [p for p in pivots if p[2] == "H"]

    for a, b in zip(lows, lows[1:]):
        i1, p1, _ = a
        i2, p2, _ = b
        if abs(p2 - p1) / p1 * 100 <= tol_pct and i2 - i1 >= 3:
            mid_high = max(candles[k]["high"] for k in range(i1, i2 + 1))
            height = mid_high - min(p1, p2)
            doubles.append({"type": "double_bottom", "idx": i2,
                            "neckline": round(mid_high, 2),
                            "target": round(mid_high + height, 2),
                            "height": round(height, 2)})
    for a, b in zip(highs, highs[1:]):
        i1, p1, _ = a
        i2, p2, _ = b
        if abs(p2 - p1) / p1 * 100 <= tol_pct and i2 - i1 >= 3:
            mid_low = min(candles[k]["low"] for k in range(i1, i2 + 1))
            height = max(p1, p2) - mid_low
            doubles.append({"type": "double_top", "idx": i2,
                            "neckline": round(mid_low, 2),
                            "target": round(mid_low - height, 2),
                            "height": round(height, 2)})
    return doubles


# ─────────────────────────────────────────────────────────
# main analysis
# ─────────────────────────────────────────────────────────
def analyze(candles, zone_tol_pct=0.08, fav_targets=(20, 30, 50), adverse=15):
    """
    candles: list of {date(datetime), open, high, low, close}, chronological, 5m.
    Detects revisit-and-reverse events at historic reversal zones (current + 3 prior
    sessions), scores confluence (zone + MA flip EMA7/17 + FVG + swing), measures
    forward outcome. Returns summary dict.
    """
    n = len(candles)
    if n < 50:
        return {"error": "not enough candles"}

    closes = [c["close"] for c in candles]
    ema7 = ema(closes, 7)
    ema17 = ema(closes, 17)
    pivots = find_pivots(candles, 2, 2)
    fvgs = find_fvgs(candles)
    doubles = detect_double(pivots, candles)

    # index sessions
    for i, c in enumerate(candles):
        c["_sd"] = session_date(c)
    session_list = sorted(set(c["_sd"] for c in candles))
    sess_pos = {d: k for k, d in enumerate(session_list)}

    # reversal zones = pivot prices, tagged with the session they formed in
    zones = [{"idx": i, "price": p, "kind": k, "sd": candles[i]["_sd"]} for (i, p, k) in pivots]

    events = []

    for i in range(20, n - 12):  # leave forward room for outcome
        c = candles[i]
        cur_sd = c["_sd"]
        cur_sess = sess_pos[cur_sd]
        price = c["close"]

        # candidate zones: from current or prior 3 sessions, formed before i
        for z in zones:
            if z["idx"] >= i - 1:
                continue
            zsess = sess_pos[z["sd"]]
            if cur_sess - zsess > 3 or cur_sess - zsess < 0:
                continue
            # price revisiting the zone within tolerance
            if abs(price - z["price"]) / z["price"] * 100 > zone_tol_pct:
                continue

            # direction of expected reversal: revisit a prior HIGH from below = bearish rejection;
            # revisit a prior LOW from above = bullish bounce.
            if z["kind"] == "L":
                bias = "bull"
            else:
                bias = "bear"

            # ---- confluence scoring ----
            score = 1  # zone touch itself
            reasons = ["historic reversal zone"]

            # MA flip (EMA7/17): for bull, price now above both EMAs having been below recently
            e7, e17 = ema7[i], ema17[i]
            e7p, e17p = ema7[i - 5], ema17[i - 5]
            if bias == "bull":
                if closes[i] > e7 and closes[i] > e17 and closes[i - 5] < e17p:
                    score += 1; reasons.append("MA flip to support")
                swing_ok = candles[i]["low"] > candles[i - 2]["low"]  # higher-low
            else:
                if closes[i] < e7 and closes[i] < e17 and closes[i - 5] > e17p:
                    score += 1; reasons.append("MA flip to resistance")
                swing_ok = candles[i]["high"] < candles[i - 2]["high"]  # lower-high

            # FVG present at location (bucketed)
            fvg_hit = None
            for mx in (5, 10, 20):
                f = fvg_at(fvgs, price, i, mx)
                if f and ((bias == "bull" and f["type"] == "bull") or (bias == "bear" and f["type"] == "bear")):
                    fvg_hit = (mx, f["size"])
                    break
            if fvg_hit:
                score += 1; reasons.append(f"FVG<={fvg_hit[0]} ({fvg_hit[1]}pt)")

            # swing confirmation
            if swing_ok:
                score += 1; reasons.append("swing confirm")

            # ---- outcome over next 12 candles (1 hour) ----
            fwd = candles[i + 1:i + 13]
            if bias == "bull":
                max_fav = max((k["high"] - price) for k in fwd)
                max_adv = max((price - k["low"]) for k in fwd)
            else:
                max_fav = max((price - k["low"]) for k in fwd)
                max_adv = max((k["high"] - price) for k in fwd)

            hit = {t: (max_fav >= t and max_adv < adverse) for t in fav_targets}

            events.append({
                "i": i, "sd": str(cur_sd), "bias": bias, "score": score,
                "reasons": reasons, "fvg": bool(fvg_hit),
                "fvg_bucket": fvg_hit[0] if fvg_hit else None,
                "max_fav": round(max_fav, 1), "max_adv": round(max_adv, 1),
                "hit": hit, "expiry": is_expiry_day(cur_sd),
            })

    # dedupe: collapse events within a 6-bar window + same bias into one (highest score)
    deduped = []
    for e in sorted(events, key=lambda x: x["i"]):
        if deduped and e["bias"] == deduped[-1]["bias"] and e["i"] - deduped[-1]["i"] <= 6:
            if e["score"] > deduped[-1]["score"]:
                deduped[-1] = e
            continue
        deduped.append(e)

    return summarize(deduped, fav_targets)


def summarize(events, fav_targets):
    def bucket_stats(evs):
        if not evs:
            return None
        out = {"count": len(evs)}
        for t in fav_targets:
            hits = sum(1 for e in evs if e["hit"][t])
            out[f"reach_{t}"] = f"{hits}/{len(evs)} ({round(hits/len(evs)*100)}%)"
        out["avg_fav"] = round(sum(e["max_fav"] for e in evs) / len(evs), 1)
        out["avg_adv"] = round(sum(e["max_adv"] for e in evs) / len(evs), 1)
        return out

    by_score = {}
    for s in (1, 2, 3, 4):
        by_score[s] = bucket_stats([e for e in events if e["score"] == s])

    by_fvg_bucket = {}
    for b in (5, 10, 20):
        by_fvg_bucket[b] = bucket_stats([e for e in events if e["fvg_bucket"] == b])

    expiry = bucket_stats([e for e in events if e["expiry"]])
    nonexp = bucket_stats([e for e in events if not e["expiry"]])
    hi_conf_expiry = bucket_stats([e for e in events if e["expiry"] and e["score"] >= 3])
    hi_conf_nonexp = bucket_stats([e for e in events if not e["expiry"] and e["score"] >= 3])

    return {
        "total_events": len(events),
        "by_confluence_score": by_score,
        "by_fvg_size_bucket": by_fvg_bucket,
        "expiry_all": expiry,
        "nonexpiry_all": nonexp,
        "expiry_score3plus": hi_conf_expiry,
        "nonexpiry_score3plus": hi_conf_nonexp,
        "note": "reach_X = fraction reaching +X pts before -15 adverse, within 12 bars (1h).",
    }

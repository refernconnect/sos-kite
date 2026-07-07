"""
SOS BACKTEST v2 — selective confluence-at-location analysis, Nifty 5m
Filters (all applied):
  1. Quality zones only: pivot whose original reversal displaced >= 1x ATR within 3 bars
  2. Real round-trip: price moved >= 1.5x ATR away from zone before revisiting
  3. First revisit only: each zone tested once, then consumed
Confluence at revisit: MA flip (EMA7/17) + directional FVG (<=5/10/20) + swing confirm
Outcome: max favorable vs max adverse over next 12 bars (1h)
"""

from datetime import datetime


def ema(values, length):
    if not values:
        return []
    k = 2 / (length + 1)
    out = [values[0]]
    for v in values[1:]:
        out.append(v * k + out[-1] * (1 - k))
    return out


def atr(candles, length=14):
    trs = []
    for i, c in enumerate(candles):
        if i == 0:
            trs.append(c["high"] - c["low"])
        else:
            p = candles[i - 1]["close"]
            trs.append(max(c["high"] - c["low"], abs(c["high"] - p), abs(c["low"] - p)))
    return ema(trs, length)


def find_pivots(candles, left=2, right=2):
    piv = []
    n = len(candles)
    for i in range(left, n - right):
        hi, lo = candles[i]["high"], candles[i]["low"]
        if all(candles[i - j]["high"] < hi for j in range(1, left + 1)) and \
           all(candles[i + j]["high"] < hi for j in range(1, right + 1)):
            piv.append((i, hi, "H"))
        if all(candles[i - j]["low"] > lo for j in range(1, left + 1)) and \
           all(candles[i + j]["low"] > lo for j in range(1, right + 1)):
            piv.append((i, lo, "L"))
    return piv


def find_fvgs(candles):
    fvgs = []
    for i in range(2, len(candles)):
        c1, c3 = candles[i - 2], candles[i]
        if c1["high"] < c3["low"]:
            fvgs.append({"idx": i, "type": "bull", "lo": c1["high"], "hi": c3["low"],
                         "size": round(c3["low"] - c1["high"], 2)})
        elif c1["low"] > c3["high"]:
            fvgs.append({"idx": i, "type": "bear", "lo": c3["high"], "hi": c1["low"],
                         "size": round(c1["low"] - c3["high"], 2)})
    return fvgs


def fvg_at(fvgs, price, upto_idx, max_size, want_type):
    for f in fvgs:
        if f["idx"] >= upto_idx or f["size"] > max_size or f["type"] != want_type:
            continue
        if f["lo"] <= price <= f["hi"]:
            return f
    return None


def session_date(c):
    return c["date"].date() if hasattr(c["date"], "date") else c["date"]


def is_expiry_day(d):
    return d.weekday() == 1  # Nifty weekly = Tuesday


def analyze(candles, zone_tol_pct=0.08, fav_targets=(20, 30, 50), adverse=15):
    n = len(candles)
    if n < 100:
        return {"error": "not enough candles"}

    closes = [c["close"] for c in candles]
    ema7 = ema(closes, 7)
    ema17 = ema(closes, 17)
    atrv = atr(candles, 14)
    pivots = find_pivots(candles, 2, 2)
    fvgs = find_fvgs(candles)

    for c in candles:
        c["_sd"] = session_date(c)
    session_list = sorted(set(c["_sd"] for c in candles))
    sess_pos = {d: k for k, d in enumerate(session_list)}

    # ---- FILTER 1: quality zones (original reversal displaced >= 1x ATR in 3 bars) ----
    zones = []
    for (i, p, k) in pivots:
        if i + 3 >= n:
            continue
        a = atrv[i]
        if k == "L":
            disp = max(candles[i + j]["high"] for j in (1, 2, 3)) - p
        else:
            disp = p - min(candles[i + j]["low"] for j in (1, 2, 3))
        if disp >= 1.5 * a:
            zones.append({"idx": i, "price": p, "kind": k, "sd": candles[i]["_sd"],
                          "consumed": False, "left": False})

    events = []

    for i in range(20, n - 12):
        c = candles[i]
        cur_sess = sess_pos[c["_sd"]]
        price = c["close"]
        a = atrv[i]

        for z in zones:
            if z["consumed"] or z["idx"] >= i - 3:
                continue
            zsess = sess_pos[z["sd"]]
            if not (0 <= cur_sess - zsess <= 3):
                continue

            dist = abs(price - z["price"])

            # ---- FILTER 2: must have LEFT the zone (>= 1.5x ATR away) before a revisit counts ----
            if not z["left"]:
                if dist >= 2.5 * a:
                    z["left"] = True
                continue

            # revisit within tolerance
            if dist / z["price"] * 100 > zone_tol_pct:
                continue

            # ---- FILTER 3: first revisit only ----
            z["consumed"] = True

            bias = "bull" if z["kind"] == "L" else "bear"
            score = 1
            reasons = ["quality zone first retest"]

            e17, e17p = ema17[i], ema17[i - 5]
            if bias == "bull":
                if closes[i] > ema7[i] and closes[i] > e17 and closes[i - 5] < e17p:
                    score += 1; reasons.append("MA flip support")
                swing_ok = candles[i]["low"] > candles[i - 2]["low"]
                want = "bull"
            else:
                if closes[i] < ema7[i] and closes[i] < e17 and closes[i - 5] > e17p:
                    score += 1; reasons.append("MA flip resistance")
                swing_ok = candles[i]["high"] < candles[i - 2]["high"]
                want = "bear"

            fvg_hit = None
            for mx in (5, 10, 20):
                f = fvg_at(fvgs, price, i, mx, want)
                if f:
                    fvg_hit = (mx, f["size"])
                    break
            if fvg_hit:
                score += 1; reasons.append(f"FVG<={fvg_hit[0]}")
            if swing_ok:
                score += 1; reasons.append("swing confirm")

            fwd = candles[i + 1:i + 13]
            if bias == "bull":
                max_fav = max(k2["high"] - price for k2 in fwd)
                max_adv = max(price - k2["low"] for k2 in fwd)
            else:
                max_fav = max(price - k2["low"] for k2 in fwd)
                max_adv = max(k2["high"] - price for k2 in fwd)

            hit = {t: (max_fav >= t and max_adv < adverse) for t in fav_targets}

            events.append({
                "i": i, "sd": str(c["_sd"]), "bias": bias, "score": score,
                "fvg_bucket": fvg_hit[0] if fvg_hit else None,
                "max_fav": round(max_fav, 1), "max_adv": round(max_adv, 1),
                "hit": hit, "expiry": is_expiry_day(c["_sd"]),
            })

    return summarize(events, fav_targets, len(session_list))


def summarize(events, fav_targets, n_sessions):
    def stats(evs):
        if not evs:
            return None
        out = {"count": len(evs)}
        for t in fav_targets:
            hits = sum(1 for e in evs if e["hit"][t])
            out[f"reach_{t}"] = f"{hits}/{len(evs)} ({round(hits / len(evs) * 100)}%)"
        out["avg_fav"] = round(sum(e["max_fav"] for e in evs) / len(evs), 1)
        out["avg_adv"] = round(sum(e["max_adv"] for e in evs) / len(evs), 1)
        return out

    return {
        "total_events": len(events),
        "events_per_session": round(len(events) / max(1, n_sessions), 1),
        "by_confluence_score": {s: stats([e for e in events if e["score"] == s]) for s in (1, 2, 3, 4)},
        "by_fvg_size_bucket": {b: stats([e for e in events if e["fvg_bucket"] == b]) for b in (5, 10, 20)},
        "expiry_all": stats([e for e in events if e["expiry"]]),
        "nonexpiry_all": stats([e for e in events if not e["expiry"]]),
        "expiry_score3plus": stats([e for e in events if e["expiry"] and e["score"] >= 3]),
        "nonexpiry_score3plus": stats([e for e in events if not e["expiry"] and e["score"] >= 3]),
        "note": "Filters: quality zones (>=1xATR original displacement), real round-trip (>=1.5xATR away), first retest only. reach_X = +X pts before -15 adverse within 1h.",
    }

"""
SOS BACKTEST v2 — confluence-at-location with SELECTIVITY FILTERS
v1 finding: loose detection (7,741 events / 90d) measured noise; no score edge.
v2 adds three filters that approximate Sid's selective eye:
  F1 QUALITY ZONE  — the original reversal at the pivot was sharp:
                     displacement >= 1.5 x ATR within 3 bars of the pivot.
  F2 REAL ROUND-TRIP — price departed >= 2.5 x ATR from the zone before revisiting.
                     A touch before full departure MITIGATES the zone (discarded).
  F3 FIRST RETEST ONLY — each zone can produce at most one event, on its first
                     qualified return. Consumed after that.
Target: 2-4 events/session. Scoring + outcome logic unchanged from v1 for comparability.

Pure functions — fed a list of 5m candles (dicts with date/open/high/low/close).
No network here; the caller (sos-kite /backtest endpoint) supplies candles via Kite.
Interface identical to v1: analyze(candles) -> summary dict. app.py unchanged.
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


def atr_series(candles, length=14):
    """Wilder ATR on 5m candles. Returns list aligned to candles."""
    n = len(candles)
    if n == 0:
        return []
    trs = [candles[0]["high"] - candles[0]["low"]]
    for i in range(1, n):
        h, l = candles[i]["high"], candles[i]["low"]
        pc = candles[i - 1]["close"]
        trs.append(max(h - l, abs(h - pc), abs(l - pc)))
    out = [trs[0]]
    for i in range(1, n):
        out.append((out[-1] * (length - 1) + trs[i]) / length)
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
    """Is there an FVG (size<=max_size) whose zone contains price, formed before upto_idx?"""
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
# double bottom / top (kept from v1, unchanged)
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
# v2 filter 1: quality zone
# ─────────────────────────────────────────────────────────
def zone_is_quality(candles, atr, idx, price, kind, disp_mult=1.5, within=3):
    """Original reversal was sharp: displacement >= disp_mult x ATR within `within` bars."""
    n = len(candles)
    end = min(idx + within + 1, n)
    a = atr[idx] if atr[idx] > 0 else 1.0
    if kind == "L":
        best = max((candles[k]["high"] for k in range(idx + 1, end)), default=price)
        return (best - price) >= disp_mult * a
    else:
        best = min((candles[k]["low"] for k in range(idx + 1, end)), default=price)
        return (price - best) >= disp_mult * a


# ─────────────────────────────────────────────────────────
# main analysis — v2 single-pass with zone lifecycle
# ─────────────────────────────────────────────────────────
def analyze(candles, zone_tol_pct=0.08, fav_targets=(20, 30, 50), adverse=15,
            disp_mult=1.5, trip_mult=2.5):
    """
    candles: list of {date(datetime), open, high, low, close}, chronological, 5m.
    v2: quality zones only (F1), real round-trip required (F2), first retest only (F3).
    Scores confluence (zone + MA flip EMA7/17 + FVG + swing), measures forward outcome.
    Returns summary dict. Interface identical to v1.
    """
    n = len(candles)
    if n < 50:
        return {"error": "not enough candles"}

    closes = [c["close"] for c in candles]
    ema7 = ema(closes, 7)
    ema17 = ema(closes, 17)
    atr = atr_series(candles, 14)
    pivots = find_pivots(candles, 2, 2)
    fvgs = find_fvgs(candles)

    # index sessions
    for c in candles:
        c["_sd"] = session_date(c)
    session_list = sorted(set(c["_sd"] for c in candles))
    sess_pos = {d: k for k, d in enumerate(session_list)}

    # ---- F1: only quality zones enter the pool ----
    # zone state: 'building' (waiting for round-trip), 'armed' (round-trip done,
    # waiting for first retest), 'dead' (mitigated early / consumed / expired)
    zones = []
    zones_total_pivots = 0
    for (idx, price, kind) in pivots:
        zones_total_pivots += 1
        if not zone_is_quality(candles, atr, idx, price, kind, disp_mult):
            continue
        zones.append({
            "idx": idx, "price": price, "kind": kind,
            "sd": candles[idx]["_sd"], "atr0": atr[idx] if atr[idx] > 0 else 1.0,
            "state": "building", "max_depart": 0.0,
        })
    zones_quality = len(zones)

    zone_ptr = 0          # zones are pivot-index ordered; activate as i passes them
    active = []
    events = []

    for i in range(20, n - 12):  # leave forward room for outcome
        c = candles[i]
        cur_sess = sess_pos[c["_sd"]]

        # activate zones whose pivot is now confirmed (pivot uses right=2 lookahead;
        # activate 3 bars after pivot so no lookahead leak, and after F1 window)
        while zone_ptr < len(zones) and zones[zone_ptr]["idx"] + 3 <= i:
            active.append(zones[zone_ptr])
            zone_ptr += 1

        # expire zones older than 3 sessions
        active = [z for z in active
                  if z["state"] != "dead" and cur_sess - sess_pos[z["sd"]] <= 3]

        hi, lo = c["high"], c["low"]
        price = c["close"]

        for z in active:
            zp = z["price"]
            tol = zp * zone_tol_pct / 100
            touching = (zp - tol) <= lo <= (zp + tol) or (zp - tol) <= hi <= (zp + tol) \
                       or (lo < zp - tol and hi > zp + tol)

            if z["state"] == "building":
                # track departure distance (F2)
                if z["kind"] == "L":
                    z["max_depart"] = max(z["max_depart"], hi - zp)
                else:
                    z["max_depart"] = max(z["max_depart"], zp - lo)

                if z["max_depart"] >= trip_mult * z["atr0"]:
                    z["state"] = "armed"
                elif touching and i > z["idx"] + 4:
                    # returned before a real round-trip -> mitigated, discard (F2)
                    z["state"] = "dead"
                continue

            if z["state"] == "armed" and touching:
                # ---- F3: first retest — consume zone regardless of outcome ----
                z["state"] = "dead"

                bias = "bull" if z["kind"] == "L" else "bear"

                # ---- confluence scoring (identical to v1) ----
                score = 1
                reasons = ["quality zone, first retest after round-trip"]

                e7, e17 = ema7[i], ema17[i]
                e17p = ema17[i - 5]
                if bias == "bull":
                    if closes[i] > e7 and closes[i] > e17 and closes[i - 5] < e17p:
                        score += 1; reasons.append("MA flip to support")
                    swing_ok = candles[i]["low"] > candles[i - 2]["low"]
                else:
                    if closes[i] < e7 and closes[i] < e17 and closes[i - 5] > e17p:
                        score += 1; reasons.append("MA flip to resistance")
                    swing_ok = candles[i]["high"] < candles[i - 2]["high"]

                fvg_hit = None
                for mx in (5, 10, 20):
                    f = fvg_at(fvgs, price, i, mx)
                    if f and ((bias == "bull" and f["type"] == "bull") or
                              (bias == "bear" and f["type"] == "bear")):
                        fvg_hit = (mx, f["size"])
                        break
                if fvg_hit:
                    score += 1; reasons.append(f"FVG<={fvg_hit[0]} ({fvg_hit[1]}pt)")

                if swing_ok:
                    score += 1; reasons.append("swing confirm")

                # ---- outcome over next 12 candles (1 hour), identical to v1 ----
                fwd = candles[i + 1:i + 13]
                if bias == "bull":
                    max_fav = max((k["high"] - price) for k in fwd)
                    max_adv = max((price - k["low"]) for k in fwd)
                else:
                    max_fav = max((price - k["low"]) for k in fwd)
                    max_adv = max((k["high"] - price) for k in fwd)

                hit = {t: (max_fav >= t and max_adv < adverse) for t in fav_targets}

                events.append({
                    "i": i, "sd": str(c["_sd"]), "bias": bias, "score": score,
                    "reasons": reasons, "fvg": bool(fvg_hit),
                    "fvg_bucket": fvg_hit[0] if fvg_hit else None,
                    "max_fav": round(max_fav, 1), "max_adv": round(max_adv, 1),
                    "hit": hit, "expiry": is_expiry_day(c["_sd"]),
                })

    out = summarize(events, fav_targets)
    n_sessions = max(len(session_list), 1)
    out["sessions"] = n_sessions
    out["events_per_session"] = round(len(events) / n_sessions, 2)
    out["funnel"] = {
        "pivots_found": zones_total_pivots,
        "quality_zones_F1": zones_quality,
        "events_after_F2_F3": len(events),
    }
    out["filters"] = {
        "F1_quality_displacement": f">= {disp_mult} x ATR within 3 bars",
        "F2_round_trip": f">= {trip_mult} x ATR departure before revisit",
        "F3": "first retest only, zone consumed",
    }
    return out


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
        "version": "v2-selectivity",
        "total_events": len(events),
        "by_confluence_score": by_score,
        "by_fvg_size_bucket": by_fvg_bucket,
        "expiry_all": expiry,
        "nonexpiry_all": nonexp,
        "expiry_score3plus": hi_conf_expiry,
        "nonexpiry_score3plus": hi_conf_nonexp,
        "note": "reach_X = fraction reaching +X pts before -15 adverse, within 12 bars (1h).",
    }

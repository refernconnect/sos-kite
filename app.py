"""
SOS KITE — Tick-level gamma detection engine
Semi-auto daily login · WebSocket ATM±3 strikes · convexity ignition · Telegram push
LOGGING + ALERTS ONLY. Places no orders.
"""

import os
import json
import time
import threading
from collections import deque
from datetime import datetime, timezone, timedelta, time as dtime

import requests
from flask import Flask, request, redirect, jsonify, render_template_string
from kiteconnect import KiteConnect, KiteTicker
import positioning as pos

app = Flask(__name__)
IST = timezone(timedelta(hours=5, minutes=30))

# ─── ENV CONFIG ───
API_KEY    = os.environ.get("KITE_API_KEY", "")
API_SECRET = os.environ.get("KITE_API_SECRET", "")
TG_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TG_CHAT    = os.environ.get("TELEGRAM_CHAT_ID", "")
BRIDGE_URL = os.environ.get("BRIDGE_URL", "https://sos-bridge-production.up.railway.app")

# ─── GAMMA THRESHOLDS ───
WINDOW_SEC     = 60    # rolling comparison window
EVAL_EVERY     = 5     # evaluate every N seconds
CONV_MIN       = 3.0   # premium % move ≥ 3x spot % move
MIN_SPOT_PCT   = 0.03  # ignore if spot moved < 0.03% in window
MAX_DTE        = 45    # classify events any DTE (blast itself gated to DTE0 inside logic)
COOLDOWN_SEC   = 600   # one alert per side per instrument per 10 min

TOKEN_FILE = "/tmp/kite_token.json"

kite = KiteConnect(api_key=API_KEY) if API_KEY else None

state = {
    "access_token": None,
    "login_time": None,
    "ws_connected": False,
    "subscribed": [],
    "spot": {},          # index_token -> ltp
    "instruments": {},   # token -> {symbol, strike, type, underlying, expiry, dte}
    "last_alert": {},    # (underlying, side) -> ts
    "gamma_log": [],     # recent events
    "day_range": {},     # idx_token -> {hi, lo} morning range
    "structure": {},     # underlying -> {ce_wall, pe_wall, last_event, updated} running map
    "futures": {},       # underlying -> {token, open_price, open_oi, last_quad}
    "brief_sent": None,  # date of last auto morning brief
    "first_candle": {},  # token -> {ph, pl} premium 9:15-9:20 range; idx_token -> spot range
    "od_fired": {},      # (underlying, type) -> date, one opening-drive per side per day
    "votes": {},         # underlying -> list of (ts, signed_weight, event_type, strike)
    "net_push": {},      # underlying -> {"bias": str, "ts": float}
    "error": None,
}
lock = threading.Lock()

# index tokens
NIFTY_TOKEN = 256265      # NSE:NIFTY 50
BANKNIFTY_TOKEN = 260105  # NSE:NIFTY BANK
SENSEX_TOKEN = 265        # BSE:SENSEX

# tick history: token -> deque of (ts, price)
hist = {}


def load_token():
    try:
        with open(TOKEN_FILE) as f:
            d = json.load(f)
        if d.get("date") == datetime.now(IST).strftime("%Y-%m-%d"):
            return d.get("access_token")
    except Exception:
        pass
    return None


def save_token(tok):
    with open(TOKEN_FILE, "w") as f:
        json.dump({"access_token": tok, "date": datetime.now(IST).strftime("%Y-%m-%d")}, f)


def tg_send(msg):
    if not TG_TOKEN or not TG_CHAT:
        print(f"TG not configured: {msg}")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
            json={"chat_id": TG_CHAT, "text": msg, "parse_mode": "HTML"},
            timeout=8,
        )
    except Exception as e:
        print(f"TG send failed: {e}")


def bridge_log(payload):
    try:
        requests.post(f"{BRIDGE_URL}/webhook", json={"message": payload}, timeout=8)
    except Exception as e:
        print(f"Bridge log failed: {e}")


def resolve_instruments():
    """Pick ATM±3 CE/PE for NIFTY + BANKNIFTY (NFO) and SENSEX (BFO),
    nearest expiry each. Requires spot prices first."""
    try:
        nfo = kite.instruments("NFO")
    except Exception as e:
        with lock:
            state["error"] = f"NFO instruments fetch failed: {e}"
        return []
    try:
        bfo = kite.instruments("BFO")
    except Exception as e:
        bfo = []
        with lock:
            state["error"] = f"BFO fetch failed (Sensex skipped): {e}"

    now = datetime.now(IST).date()
    tokens = []

    for underlying, idx_token, step, source in (
        ("NIFTY", NIFTY_TOKEN, 50, nfo),
        ("BANKNIFTY", BANKNIFTY_TOKEN, 100, nfo),
        ("SENSEX", SENSEX_TOKEN, 100, bfo),
    ):
        spot = state["spot"].get(idx_token)
        if not spot or not source:
            continue
        atm = round(spot / step) * step

        opts = [i for i in source
                if i["name"] == underlying and i["instrument_type"] in ("CE", "PE")
                and i["expiry"] and i["expiry"] >= now]
        if not opts:
            continue
        nearest_exp = min(o["expiry"] for o in opts)
        dte = (nearest_exp - now).days

        for o in opts:
            if o["expiry"] != nearest_exp:
                continue
            if abs(o["strike"] - atm) <= 3 * step:
                tok = o["instrument_token"]
                tokens.append(tok)
                with lock:
                    state["instruments"][tok] = {
                        "symbol": o["tradingsymbol"],
                        "strike": o["strike"],
                        "type": o["instrument_type"],
                        "underlying": underlying,
                        "idx_token": idx_token,
                        "dte": dte,
                    }

        # nearest-month FUTURES for the live positioning quadrant
        futs = [i for i in source
                if i["name"] == underlying and i["instrument_type"] == "FUT"
                and i["expiry"] and i["expiry"] >= now]
        if futs:
            nearest_fut = min(futs, key=lambda x: x["expiry"])
            ftok = nearest_fut["instrument_token"]
            tokens.append(ftok)

            # Seed since-open baselines from ground truth, not first tick:
            # open_price = day's actual open (quote OHLC), open_oi = yesterday's closing OI.
            # This makes the live quadrant correct even after a mid-day re-login/deploy.
            seed_open, seed_oi = None, None
            try:
                exch = nearest_fut.get("exchange", "NFO")
                qkey = f"{exch}:{nearest_fut['tradingsymbol']}"
                qd = kite.quote([qkey]).get(qkey)
                if qd:
                    seed_open = (qd.get("ohlc") or {}).get("open") or None
            except Exception as e:
                print(f"open-price seed failed {underlying}: {e}")
            try:
                d0 = datetime.now(IST)
                today_str = d0.strftime("%Y-%m-%d")
                hd = kite.historical_data(
                    ftok, (d0 - timedelta(days=7)).strftime("%Y-%m-%d"),
                    today_str, "day", oi=True)
                prev = [c for c in hd if str(c["date"])[:10] < today_str]
                if prev:
                    seed_oi = prev[-1].get("oi") or None
            except Exception as e:
                print(f"open-oi seed failed {underlying}: {e}")

            with lock:
                state["futures"][underlying] = {
                    "token": ftok, "symbol": nearest_fut["tradingsymbol"],
                    "open_price": seed_open, "open_oi": seed_oi, "last_quad": None,
                }
    return tokens


def on_ticks(ws, ticks):
    ts = time.time()
    now_ist = datetime.now(IST)
    for t in ticks:
        tok = t["instrument_token"]
        price = t.get("last_price", 0)
        if not price:
            continue
        oi = t.get("oi", 0)  # present in FULL mode for options

        if tok in (NIFTY_TOKEN, BANKNIFTY_TOKEN, SENSEX_TOKEN):
            with lock:
                state["spot"][tok] = price
                # track morning range (9:15 to 13:45) per index
                dr = state["day_range"].setdefault(tok, {"hi": price, "lo": price})
                if now_ist.time() <= dtime(13, 45):
                    dr["hi"] = max(dr["hi"], price)
                    dr["lo"] = min(dr["lo"], price)

        # futures open snapshot (first tick of day) for since-open quadrant
        with lock:
            for u, f in state["futures"].items():
                if f["token"] == tok:
                    if f["open_price"] is None and price:
                        f["open_price"] = price
                    if f["open_oi"] is None and oi:
                        f["open_oi"] = oi
                    f["last_price"] = price
                    f["last_oi"] = oi
                    break

        # first-candle range capture (09:15-09:20) for opening-drive breakout
        t_now = now_ist.time()
        if dtime(9, 15) <= t_now < dtime(9, 20):
            with lock:
                fc = state["first_candle"].setdefault(tok, {"hi": price, "lo": price})
                fc["hi"] = max(fc["hi"], price)
                fc["lo"] = min(fc["lo"], price)

        if tok not in hist:
            hist[tok] = deque(maxlen=600)
        hist[tok].append((ts, price, oi))


def on_connect(ws, response):
    with lock:
        state["ws_connected"] = True
    # subscribe indices first (spot), option tokens after resolution
    ws.subscribe([NIFTY_TOKEN, BANKNIFTY_TOKEN, SENSEX_TOKEN])
    ws.set_mode(ws.MODE_LTP, [NIFTY_TOKEN, BANKNIFTY_TOKEN, SENSEX_TOKEN])

    def sub_options():
        time.sleep(5)  # wait for first spot ticks
        toks = resolve_instruments()
        if toks:
            ws.subscribe(toks)
            ws.set_mode(ws.MODE_FULL, toks)  # FULL = includes OI, needed for squeeze/writing detection
            with lock:
                state["subscribed"] = toks
            tg_send(f"SOS KITE live — tracking {len(toks)} strikes FULL mode (ATM±3, Nifty+BankNifty+Sensex)")
    threading.Thread(target=sub_options, daemon=True).start()


def on_close(ws, code, reason):
    with lock:
        state["ws_connected"] = False


def window_vals(tok, now_ts, window_sec):
    """Return (old_price, new_price, old_oi, new_oi) over the window."""
    dq = hist.get(tok)
    if not dq or len(dq) < 2:
        return None
    cutoff = now_ts - window_sec
    old = None
    for rec in dq:
        if rec[0] >= cutoff:
            old = rec
            break
    if old is None:
        return None
    new = dq[-1]
    return (old[1], new[1], old[2], new[2])


def gamma_loop():
    """Every EVAL_EVERY sec: classify each ATM-region strike for blast/covering/writing."""
    import gamma_engine as ge
    while True:
        time.sleep(EVAL_EVERY)
        now_ts = time.time()
        now_ist = datetime.now(IST)
        with lock:
            insts = dict(state["instruments"])
            subscribed = list(state["subscribed"])
            spots = dict(state["spot"])
            dayr = dict(state["day_range"])

        # ── OPENING DRIVE scan (09:20-10:00): premium breaks first-candle high, spot aligned ──
        if dtime(9, 20) <= now_ist.time() <= dtime(10, 0):
            today = now_ist.strftime("%Y-%m-%d")
            with lock:
                fcs = dict(state["first_candle"])
                od_fired = dict(state["od_fired"])
            for tok in subscribed:
                meta = insts.get(tok)
                if not meta:
                    continue
                key = (meta["underlying"], meta["type"])
                if od_fired.get(key) == today:
                    continue
                fc = fcs.get(tok)
                idx_fc = fcs.get(meta["idx_token"])
                dq = hist.get(tok)
                if not fc or not idx_fc or not dq:
                    continue
                cur_prem = dq[-1][1]
                cur_spot = spots.get(meta["idx_token"], 0)
                od = pos.opening_drive_check(fc["hi"], cur_prem, idx_fc["hi"], idx_fc["lo"],
                                             cur_spot, meta["type"])
                if od:
                    entry, t1, t2, stp = od
                    with lock:
                        state["od_fired"][key] = today
                    biasdot = "🟢" if meta["type"] == "CE" else "🔴"
                    tg_send(f"🚀 <b>OPENING DRIVE</b> — {biasdot} <b>{'BULLISH' if meta['type']=='CE' else 'BEARISH'}</b>\n"
                            f"{meta['underlying']} {meta['type']} {meta['strike']:.0f}\n"
                            f"Premium broke first-candle high {fc['hi']:.1f} · spot confirming\n"
                            f"<b>BUY {meta['strike']:.0f} {meta['type']} @ ~{entry}\n"
                            f"⏱ RESTING ORDERS NOW:\n"
                            f"• SELL limit {t1} (+35%) books half\n"
                            f"• SELL limit {t2} (+70%) books rest\n"
                            f"• STOP {stp} (−15%)</b>\n"
                            f"Set & step back.")
                    bridge_log(f"OPENING DRIVE {meta['underlying']} {meta['type']} {meta['strike']:.0f} entry {entry}")

        for tok in subscribed:
            meta = insts.get(tok)
            if not meta or meta["dte"] > MAX_DTE:
                continue

            wv = window_vals(tok, now_ts, WINDOW_SEC)
            sv = window_vals(meta["idx_token"], now_ts, WINDOW_SEC)
            if not wv or not sv:
                continue
            prem_old, prem_new, oi_old, oi_new = wv
            spot_old, spot_new = sv[0], sv[1]

            spot_dir = 1 if spot_new > spot_old else -1 if spot_new < spot_old else 0

            # compression + gate context (for classic blast labelling)
            idx = meta["idx_token"]
            dr = dayr.get(idx, {})
            ref = spots.get(idx)
            compressed, rng_pct = ge.compression_state(dr.get("hi", 0), dr.get("lo", 0), ref) if ref else (False, 0)
            gated = ge.in_gate(now_ist.time(), meta["dte"])
            broke = ge.spot_broke_range(spot_new, dr.get("hi", 0), dr.get("lo", 0), ref) if ref else 0

            result = ge.classify(prem_old, prem_new, oi_old, oi_new, spot_dir, meta["type"],
                                  strike=meta["strike"], spot=spot_new)
            if not result:
                continue
            event_type, bias, detail = result

            # For GAMMA BLAST specifically, require the coil+gate+release context
            if event_type == "GAMMA BLAST":
                if not (compressed and gated and broke != 0):
                    # premium accelerating but not the classic expiry-coil blast -> downgrade label
                    event_type = "PREMIUM SURGE"

            key = (meta["underlying"], meta["type"], event_type)
            with lock:
                last = state["last_alert"].get(key, 0)
            if now_ts - last < COOLDOWN_SEC:
                continue
            with lock:
                state["last_alert"][key] = now_ts

            icon = {"GAMMA BLAST": "⚡", "SHORT COVERING": "🔥", "PREMIUM SURGE": "📈",
                    "WRITING PRESSURE": "🧱", "FRESH BUYING": "🟢", "UNWINDING": "🔄"}.get(event_type, "•")

            # update running structure map: walls from WRITING PRESSURE
            with lock:
                st = state["structure"].setdefault(meta["underlying"],
                        {"ce_wall": None, "pe_wall": None, "last_event": None, "updated": None})
                if event_type == "WRITING PRESSURE":
                    if meta["type"] == "CE":
                        st["ce_wall"] = meta["strike"]
                    else:
                        st["pe_wall"] = meta["strike"]
                st["last_event"] = event_type
                st["updated"] = now_ist.strftime("%H:%M")
                struct_snapshot = dict(st)

            read, watch = ge.event_guidance(event_type, bias, meta["type"], meta["strike"], spot_new)
            situation = ge.build_situation(struct_snapshot, spot_new, dr, compressed, rng_pct)
            step = 100 if meta["underlying"] in ("BANKNIFTY", "SENSEX") else 50
            plan = ge.trade_plan(event_type, bias, meta["type"], meta["strike"], spot_new, struct_snapshot, step, spot_hint=prem_new)

            biasdot = "🟢" if bias == "BULLISH" else "🔴"

            # ── ONLY GAMMA BLAST pushes immediately (rare, actionable, with plan) ──
            if event_type == "GAMMA BLAST":
                msg = (f"{icon} <b>{event_type}</b> — {biasdot} <b>{bias}</b>\n"
                       f"{meta['underlying']} {meta['type']} {meta['strike']:.0f} · {meta['symbol']}\n"
                       f"{detail}\n"
                       f"LTP {prem_new:.1f} (from {prem_old:.1f}) · spot {spot_new:.1f} · DTE {meta['dte']}\n"
                       f"\n▸ {read}\n▸ <b>{watch}</b>\n"
                       f"\n<b>{plan}</b>\n"
                       f"\n<i>{situation}</i>")
                tg_send(msg)

            # ── everything else becomes a weighted VOTE toward net bias ──
            weights = {"SHORT COVERING": 3.0, "PREMIUM SURGE": 2.0,
                       "UNWINDING": 1.0, "FRESH BUYING": 0.5}
            w = weights.get(event_type, 0.0)
            if w > 0:
                signed = w if bias == "BULLISH" else -w
                with lock:
                    v = state["votes"].setdefault(meta["underlying"], [])
                    v.append((now_ts, signed, event_type, meta["strike"]))
                    state["votes"][meta["underlying"]] = [x for x in v if now_ts - x[0] <= 900]

                    votes_now = state["votes"][meta["underlying"]]
                    net = sum(x[1] for x in votes_now)
                    np_ = state["net_push"].get(meta["underlying"], {"bias": None, "ts": 0})

                net_bias = "BULLISH" if net >= 4 else "BEARISH" if net <= -4 else None
                if net_bias and (net_bias != np_["bias"] or now_ts - np_["ts"] > 1800):
                    with lock:
                        state["net_push"][meta["underlying"]] = {"bias": net_bias, "ts": now_ts}
                    dominant = {}
                    for _, sw, et, stk in votes_now:
                        dominant[et] = dominant.get(et, 0) + abs(sw)
                    top = max(dominant, key=dominant.get) if dominant else ""
                    ndot = "🟢" if net_bias == "BULLISH" else "🔴"
                    tg_send(f"{ndot} <b>NET BIAS — {meta['underlying']}: {net_bias}</b> (score {net:+.1f})\n"
                            f"Driven by {top.lower()} over last 15 min · spot {spot_new:.1f}\n"
                            f"\n<i>{situation}</i>")
            bridge_log(f"{event_type} {bias} {meta['underlying']} {meta['type']} {meta['strike']:.0f} :: {detail}")
            entry = {
                "time": now_ist.strftime("%H:%M:%S"),
                "type": event_type, "bias": bias, "symbol": meta["symbol"],
                "detail": detail, "ltp": round(prem_new, 1),
            }
            with lock:
                state["gamma_log"].append(entry)
                state["gamma_log"] = state["gamma_log"][-50:]


ticker_started_once = False


def start_ticker(access_token):
    global ticker_started_once
    ticker_started_once = True
    kws = KiteTicker(API_KEY, access_token)
    kws.on_ticks = on_ticks
    kws.on_connect = on_connect
    kws.on_close = on_close
    kws.connect(threaded=True)


# ─── ROUTES ───
@app.route("/")
def home():
    with lock:
        s = {
            "logged_in": state["access_token"] is not None,
            "login_time": state["login_time"],
            "ws": state["ws_connected"],
            "n_subscribed": len(state["subscribed"]),
            "spot": {("NIFTY" if k == NIFTY_TOKEN else "BANKNIFTY" if k == BANKNIFTY_TOKEN else "SENSEX"): v for k, v in state["spot"].items()},
            "gamma_log": list(reversed(state["gamma_log"])),
            "error": state["error"],
        }
    return render_template_string(HOME_HTML, s=s)


@app.route("/kite/login")
def kite_login():
    if not kite:
        return "KITE_API_KEY not set in environment", 500
    return redirect(kite.login_url())


@app.route("/kite/callback")
def kite_callback():
    req_token = request.args.get("request_token")
    if not req_token:
        return "No request_token in callback", 400
    try:
        data = kite.generate_session(req_token, api_secret=API_SECRET)
        tok = data["access_token"]
        kite.set_access_token(tok)
        save_token(tok)
        with lock:
            state["access_token"] = tok
            state["login_time"] = datetime.now(IST).strftime("%H:%M:%S")
            state["error"] = None
        if ticker_started_once:
            # twisted reactor cannot restart in-process; token is saved,
            # so exit and let Railway restart us — boot() restores token
            # and starts a clean ticker automatically.
            tg_send("SOS KITE — login OK, restarting stream engine (~10s)")
            def _bye():
                time.sleep(1.5)
                os._exit(1)
            threading.Thread(target=_bye, daemon=True).start()
            return redirect("/")
        start_ticker(tok)
        tg_send("SOS KITE — login OK, connecting to tick stream")
        return redirect("/")
    except Exception as e:
        return f"Token exchange failed: {e}", 500


@app.route("/health")
def health():
    return "ok"
@app.route("/token")
def token_share():
    """Share today's Kite access token with sibling services.

    Consumed by sos-stock-radar/token_sync.py and the CAS backfill below.
    Accepts the secret via X-Token-Secret header (preferred) or ?secret=
    (kept for backward compatibility with existing token_sync.py).
    """
    secret = os.environ.get("TOKEN_SHARE_SECRET", "")
    if not secret:
        return jsonify({"error": "TOKEN_SHARE_SECRET not configured"}), 503
    supplied = request.headers.get("X-Token-Secret") or request.args.get("secret")
    if supplied != secret:
        return jsonify({"error": "unauthorized"}), 403
    with lock:
        tok = state.get("access_token")
    if not tok:
        tok = load_token()
    if not tok:
        return jsonify({"token": None, "error": "no token - morning login not done"}), 200
    return jsonify({"token": tok, "api_key": API_KEY})


# ─── CAS BACKFILL (hypothesis 1: does the close mean-revert into the next open?) ───

NIFTY50_SYMBOLS = [
    "RELIANCE","HDFCBANK","ICICIBANK","BHARTIARTL","INFY","TCS","SBIN","LT",
    "ITC","AXISBANK","KOTAKBANK","HINDUNILVR","BAJFINANCE","M&M","MARUTI",
    "SUNPHARMA","NTPC","HCLTECH","TATAMOTORS","ULTRACEMCO","TITAN","ASIANPAINT",
    "POWERGRID","ADANIENT","TATASTEEL","BAJAJFINSV","ONGC","COALINDIA","NESTLEIND",
    "JSWSTEEL","WIPRO","GRASIM","ADANIPORTS","TECHM","HINDALCO","CIPLA","DRREDDY",
    "INDUSINDBK","BAJAJ-AUTO","APOLLOHOSP","EICHERMOT","BPCL","DIVISLAB","TATACONSUM",
    "HEROMOTOCO","BRITANNIA","SBILIFE","HDFCLIFE","SHRIRAMFIN","TRENT",
]

CAS_START_DATE = "2026-08-03"   # CAS went live


def _cas_day_stats(minute_bars, daily_bars):
    """Per date: reference VWAP (15:00-15:14) from minute bars, close from daily bars."""
    from collections import defaultdict
    by_date = defaultdict(list)
    last_bar_time = {}
    for b in minute_bars:
        d = b["date"]
        key = d.strftime("%Y-%m-%d")
        by_date[key].append(b)
        t = d.strftime("%H:%M")
        if key not in last_bar_time or t > last_bar_time[key]:
            last_bar_time[key] = t

    closes, opens = {}, {}
    for b in daily_bars:
        key = b["date"].strftime("%Y-%m-%d")
        closes[key] = b["close"]
        opens[key] = b["open"]

    out = {}
    for key, bars in by_date.items():
        win = [b for b in bars if "15:00" <= b["date"].strftime("%H:%M") <= "15:14"]
        if not win or key not in closes:
            continue
        vol = sum(b.get("volume") or 0 for b in win)
        if vol > 0:
            vwap = sum(((b["high"] + b["low"] + b["close"]) / 3.0) * (b.get("volume") or 0)
                       for b in win) / vol
        else:
            vwap = sum(b["close"] for b in win) / len(win)
        out[key] = {
            "ref_vwap": round(vwap, 2),
            "close": closes[key],
            "next_open": None,
            "last_minute_bar": last_bar_time.get(key),
        }

    keys = sorted(out.keys())
    for i, k in enumerate(keys[:-1]):
        out[k]["next_open"] = opens.get(keys[i + 1])
    return out


@app.route("/cas_backfill")
def cas_backfill_route():
    """Hypothesis 1 study.  Query: ?limit=50&start=2026-08-03

    For every Nifty-50 stock-day since CAS launch, compares the official close
    against the 15:00-15:14 reference VWAP, then against the NEXT session's open.
    Tests whether the auction print fades (mean-reverts) or persists.
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    limit = int(request.args.get("limit", 50))
    start = request.args.get("start", CAS_START_DATE)
    to_d = datetime.now(IST)
    from_d = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=IST)

    try:
        nse = kite.instruments("NSE")
    except Exception as e:
        return jsonify({"error": "instruments() failed: %s" % e}), 500

    tokmap = {}
    for r in nse:
        if r.get("segment") == "NSE" and r.get("instrument_type") == "EQ":
            tokmap[r["tradingsymbol"]] = r["instrument_token"]

    rows, skipped = [], []
    for sym in NIFTY50_SYMBOLS[:limit]:
        tok = tokmap.get(sym)
        if not tok:
            skipped.append({"symbol": sym, "reason": "no instrument token"})
            continue
        try:
            a = from_d.strftime("%Y-%m-%d %H:%M:%S")
            b = to_d.strftime("%Y-%m-%d %H:%M:%S")
            mins = kite.historical_data(tok, a, b, "minute")
            days = kite.historical_data(tok, a, b, "day")
        except Exception as e:
            skipped.append({"symbol": sym, "reason": str(e)[:120]})
            time.sleep(0.4)
            continue

        for date_key, s in _cas_day_stats(mins, days).items():
            if not s["next_open"] or not s["ref_vwap"]:
                continue
            delta = (s["close"] - s["ref_vwap"]) / s["ref_vwap"] * 100.0
            nxt = (s["next_open"] - s["close"]) / s["close"] * 100.0
            rows.append({
                "symbol": sym, "date": date_key,
                "ref_vwap": s["ref_vwap"], "close": s["close"],
                "delta_pct": round(delta, 3),
                "next_open_pct": round(nxt, 3),
                "last_minute_bar": s["last_minute_bar"],
            })
        time.sleep(0.4)

    if not rows:
        return jsonify({"error": "no rows built", "skipped": skipped[:20]}), 500

    n = len(rows)
    mean_d = sum(r["delta_pct"] for r in rows) / n
    mean_n = sum(r["next_open_pct"] for r in rows) / n
    cov = sum((r["delta_pct"] - mean_d) * (r["next_open_pct"] - mean_n) for r in rows)
    vd = sum((r["delta_pct"] - mean_d) ** 2 for r in rows) ** 0.5
    vn = sum((r["next_open_pct"] - mean_n) ** 2 for r in rows) ** 0.5
    corr = cov / (vd * vn) if vd and vn else 0.0

    nonzero = [r for r in rows if abs(r["delta_pct"]) > 0.01]
    fades = sum(1 for r in nonzero if r["delta_pct"] * r["next_open_pct"] < 0)
    big = [r for r in rows if abs(r["delta_pct"]) >= 0.5]
    big_fades = sum(1 for r in big if r["delta_pct"] * r["next_open_pct"] < 0)

    bar_times = {}
    for r in rows:
        bar_times[r["last_minute_bar"]] = bar_times.get(r["last_minute_bar"], 0) + 1

    return jsonify({
        "observations": n,
        "symbols": len(set(r["symbol"] for r in rows)),
        "sessions": len(set(r["date"] for r in rows)),
        "mean_delta_pct": round(mean_d, 4),
        "mean_next_open_pct": round(mean_n, 4),
        "correlation_delta_vs_next_open": round(corr, 4),
        "fade_rate_all": round(fades / len(nonzero) * 100, 2) if nonzero else None,
        "fade_rate_big_moves": round(big_fades / len(big) * 100, 2) if big else None,
        "big_move_count": len(big),
        "last_minute_bar_distribution": bar_times,
        "skipped": skipped[:20],
        "sample": rows[:15],
    })

@app.route("/backtest")
def backtest_route():
    """Pull Nifty 5m history via Kite and run confluence analysis.
    Query: ?days=90  (how many calendar days back, default 90)"""
    import backtest as bt
    if not state.get("access_token"):
        return jsonify({"error": "not logged in — do morning login first"}), 400

    days = int(request.args.get("days", 90))
    try:
        to_d = datetime.now(IST)
        from_d = to_d - timedelta(days=days)
        # Kite historical: NIFTY 50 index token 256265, 5minute.
        # API caps intraday pulls ~100 days/request; chunk if needed.
        all_candles = []
        chunk_start = from_d
        while chunk_start < to_d:
            chunk_end = min(chunk_start + timedelta(days=60), to_d)
            data = kite.historical_data(
                NIFTY_TOKEN,
                chunk_start.strftime("%Y-%m-%d %H:%M:%S"),
                chunk_end.strftime("%Y-%m-%d %H:%M:%S"),
                "5minute",
            )
            for d in data:
                all_candles.append({
                    "date": d["date"],
                    "open": d["open"], "high": d["high"],
                    "low": d["low"], "close": d["close"],
                })
            chunk_start = chunk_end + timedelta(days=1)

        if not all_candles:
            return jsonify({"error": "no candles returned"}), 500

        result = bt.analyze(all_candles)
        result["candles_analyzed"] = len(all_candles)
        result["range"] = f"{from_d.date()} to {to_d.date()}"
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


HOME_HTML = """
<!DOCTYPE html>
<html>
<head>
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>SOS KITE</title>
<style>
body { background:#1A1816; color:#e8e2d8; font-family:monospace; font-size:13px; padding:12px; }
h1 { color:#C4A882; font-size:15px; letter-spacing:1px; }
.card { background:#2a2622; padding:10px; margin:8px 0; border-radius:2px; border-left:3px solid #C4A882; }
.ok { color:#4cc46a; } .bad { color:#d9534f; }
a.btn { display:inline-block; background:#C4A882; color:#1A1816; padding:10px 18px; text-decoration:none; font-weight:700; border-radius:2px; margin-top:6px; }
.g { border-left-color:#ff9500; }
</style>
</head>
<body>
<h1>SOS KITE — tick gamma engine</h1>
<div class="card">
Login: <span class="{{ 'ok' if s.logged_in else 'bad' }}">{{ 'ACTIVE since ' + s.login_time if s.logged_in else 'NOT LOGGED IN' }}</span><br>
WebSocket: <span class="{{ 'ok' if s.ws else 'bad' }}">{{ 'CONNECTED' if s.ws else 'DOWN' }}</span> ·
Strikes tracked: {{ s.n_subscribed }}<br>
{% for k, v in s.spot.items() %}{{ k }}: {{ '%.1f'|format(v) }} · {% endfor %}
{% if s.error %}<br><span class="bad">{{ s.error }}</span>{% endif %}
</div>
{% if not s.logged_in %}
<a class="btn" href="/kite/login">MORNING LOGIN — tap to start day</a>
{% else %}
<a class="btn" href="/kite/login" style="background:#3a3630;color:#C4A882;">RE-LOGIN (new day / stream stuck)</a>
{% endif %}
<h1 style="margin-top:14px">Events today</h1>
{% for g in s.gamma_log %}
<div class="card g">{{ g.time }} — <b>{{ g.type }}</b> {{ g.bias }} · {{ g.symbol }} · {{ g.detail }} · LTP {{ g.ltp }}</div>
{% endfor %}
{% if not s.gamma_log %}<div class="card">None yet.</div>{% endif %}
</body>
</html>
"""



def build_full_brief():
    """Daily futures OI quadrant brief (the desks' view) for all three underlyings."""
    if not state.get("access_token"):
        return None, "not logged in"
    blocks = []
    to_d = datetime.now(IST)
    from_d = to_d - timedelta(days=12)
    with lock:
        futs = dict(state["futures"])
    if not futs:
        return None, "futures not resolved yet (login + wait for ticker)"
    for underlying, f in futs.items():
        try:
            data = kite.historical_data(
                f["token"],
                from_d.strftime("%Y-%m-%d"),
                to_d.strftime("%Y-%m-%d"),
                "day", oi=True,
            )
            candles = [{"date": d["date"], "close": d["close"], "oi": d.get("oi", 0)} for d in data]
            blocks.append(pos.daily_brief(candles, underlying))
        except Exception as e:
            blocks.append(f"— {underlying} — brief failed: {e}")

        # live since-open line
        q = pos.intraday_quadrant(f.get("open_price"), f.get("last_price"),
                                  f.get("open_oi"), f.get("last_oi")) if f.get("last_price") else None
        if q:
            label, bias, dot, p, o = q
            blocks.append(f"  today live: {dot} {label} (px {p:+.2f}% · OI {o:+.2f}%)")
    txt = "📋 POSITIONING BRIEF — " + datetime.now(IST).strftime("%d %b %H:%M") + "\n\n" + "\n\n".join(blocks)
    return txt, None


@app.route("/brief")
def brief_route():
    txt, err = build_full_brief()
    if err:
        return jsonify({"error": err}), 400
    return "<pre style='background:#1A1816;color:#e8e2d8;padding:14px;font-size:13px'>" + txt + "</pre>"


def positioning_loop():
    """Morning auto-brief (~09:05) + live quadrant flip alerts (30-min cooldown)."""
    while True:
        time.sleep(60)
        now = datetime.now(IST)
        try:
            # morning brief once per day after 09:05, if logged in
            if now.time() >= dtime(9, 5) and now.time() <= dtime(15, 30):
                with lock:
                    sent = state.get("brief_sent")
                today = now.strftime("%Y-%m-%d")
                if sent != today and state.get("access_token"):
                    txt, err = build_full_brief()
                    if txt:
                        tg_send(txt)
                        with lock:
                            state["brief_sent"] = today

            # live quadrant flip detection
            with lock:
                futs = dict(state["futures"])
            for underlying, f in futs.items():
                if not f.get("last_price") or not f.get("open_price"):
                    continue
                q = pos.intraday_quadrant(f["open_price"], f["last_price"], f["open_oi"], f["last_oi"])
                if not q:
                    continue
                label, bias, dot, p, o = q
                if label in ("FLAT",):
                    continue
                prev_quad = f.get("last_quad")
                if prev_quad != label:
                    with lock:
                        state["futures"][underlying]["last_quad"] = label
                    if prev_quad is not None:  # skip the first classification of the day
                        tg_send(f"{dot} FUTURES QUADRANT FLIP — {underlying}\n"
                                f"Now: {label} ({bias})\n"
                                f"Since open: px {p:+.2f}% · OI {o:+.2f}%\n"
                                f"▸ The positional view just changed — reassess open bias.")
        except Exception as e:
            print(f"positioning loop error: {e}")

# ─── STARTUP ───
def boot():
    tok = load_token()
    if tok and kite:
        try:
            kite.set_access_token(tok)
            with lock:
                state["access_token"] = tok
                state["login_time"] = "restored"
            start_ticker(tok)
        except Exception as e:
            print(f"Token restore failed: {e}")

boot()
threading.Thread(target=gamma_loop, daemon=True).start()
threading.Thread(target=positioning_loop, daemon=True).start()


@app.route("/cas_edge")
def cas_edge_route():
    """Is the CAS fade actually tradeable? Expectancy per trade, net of cost.

    Query: ?limit=15&start=2026-07-01&cost_bps=20&regime=post
      regime: post = CAS sessions only, pre = pre-CAS control, all = both
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    limit = int(request.args.get("limit", 15))
    start = request.args.get("start", CAS_START_DATE)
    cost_bps = float(request.args.get("cost_bps", 20))
    regime = request.args.get("regime", "post")
    want_bar = {"post": "15:14", "pre": "15:29"}.get(regime)

    to_d = datetime.now(IST)
    from_d = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=IST)

    try:
        nse = kite.instruments("NSE")
    except Exception as e:
        return jsonify({"error": "instruments() failed: %s" % e}), 500
    tokmap = {r["tradingsymbol"]: r["instrument_token"] for r in nse
              if r.get("segment") == "NSE" and r.get("instrument_type") == "EQ"}

    rows = []
    for sym in NIFTY50_SYMBOLS[:limit]:
        tok = tokmap.get(sym)
        if not tok:
            continue
        try:
            a = from_d.strftime("%Y-%m-%d %H:%M:%S")
            b = to_d.strftime("%Y-%m-%d %H:%M:%S")
            mins = kite.historical_data(tok, a, b, "minute")
            days = kite.historical_data(tok, a, b, "day")
        except Exception:
            time.sleep(0.4)
            continue
        for dk, s in _cas_day_stats(mins, days).items():
            if not s["next_open"] or not s["ref_vwap"]:
                continue
            if want_bar and s["last_minute_bar"] != want_bar:
                continue
            delta = (s["close"] - s["ref_vwap"]) / s["ref_vwap"] * 100.0
            nxt = (s["next_open"] - s["close"]) / s["close"] * 100.0
            side = 1 if delta > 0 else -1          # +1 = close printed high -> fade short
            capture = -side * nxt * 100.0          # bps earned by fading
            rows.append({"symbol": sym, "date": dk, "delta_pct": round(delta, 3),
                         "next_open_pct": round(nxt, 3), "side": side,
                         "capture_bps": round(capture, 1)})
        time.sleep(0.4)

    if not rows:
        return jsonify({"error": "no rows", "regime": regime}), 500

    def stats(sel):
        if not sel:
            return None
        caps = sorted(r["capture_bps"] for r in sel)
        n = len(caps)
        mean = sum(caps) / n
        med = caps[n // 2] if n % 2 else (caps[n // 2 - 1] + caps[n // 2]) / 2.0
        wins = sum(1 for c in caps if c > 0)
        sd = (sum((c - mean) ** 2 for c in caps) / n) ** 0.5
        net = mean - cost_bps
        return {
            "trades": n,
            "win_rate_pct": round(wins / n * 100, 1),
            "mean_capture_bps": round(mean, 1),
            "median_capture_bps": round(med, 1),
            "stdev_bps": round(sd, 1),
            "net_expectancy_bps": round(net, 1),
            "total_net_bps": round(net * n, 0),
            "best_bps": caps[-1],
            "worst_bps": caps[0],
        }

    out = {
        "regime": regime,
        "cost_bps_assumed": cost_bps,
        "sessions": len(set(r["date"] for r in rows)),
        "symbols": len(set(r["symbol"] for r in rows)),
        "all_stock_days": stats(rows),
        "by_threshold": {},
        "by_direction_at_0.5": {},
    }
    for th in (0.3, 0.5, 0.75, 1.0):
        out["by_threshold"]["abs_delta_gte_%.2f_pct" % th] = stats(
            [r for r in rows if abs(r["delta_pct"]) >= th])
    sel5 = [r for r in rows if abs(r["delta_pct"]) >= 0.5]
    out["by_direction_at_0.5"]["close_printed_HIGH_fade_short"] = stats(
        [r for r in sel5 if r["side"] == 1])
    out["by_direction_at_0.5"]["close_printed_LOW_fade_long"] = stats(
        [r for r in sel5 if r["side"] == -1])
    out["worst_10_trades"] = sorted(sel5, key=lambda r: r["capture_bps"])[:10]
    return jsonify(out)

@app.route("/cas_rank")
def cas_rank_route():
    """Per-symbol CAS dislocation stats, for picking a shortlist.

    Query: ?offset=0&limit=12&start=2026-08-03&th=0.75&cost_bps=10
    Chunk with offset to stay under the request timeout.
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    offset = int(request.args.get("offset", 0))
    limit = int(request.args.get("limit", 12))
    start = request.args.get("start", CAS_START_DATE)
    th = float(request.args.get("th", 0.75))
    cost_bps = float(request.args.get("cost_bps", 10))

    # universe: CAS-eligible symbols from NSE, ordered by traded value
    universe, uni_src = [], "nse"
    try:
        s = requests.Session()
        s.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/124.0.0.0 Safari/537.36",
            "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate",
        })
        s.get("https://www.nseindia.com", timeout=12)
        s.get("https://www.nseindia.com/market-data/closing-auction-session", timeout=12)
        r = s.get("https://www.nseindia.com/api/NextApi/apiClient/casApi"
                  "?functionName=getCASData",
                  headers={"Referer": "https://www.nseindia.com/market-data/"
                                      "closing-auction-session",
                           "X-Requested-With": "XMLHttpRequest"}, timeout=15)
        data = r.json().get("data") or []
        data.sort(key=lambda d: d.get("finalValue") or 0, reverse=True)
        universe = [d["symbol"] for d in data if d.get("symbol")]
    except Exception as e:
        uni_src = "fallback:%s" % str(e)[:60]
    if not universe:
        universe = NIFTY50_SYMBOLS
        uni_src = "fallback_nifty50"

    chunk = universe[offset:offset + limit]

    try:
        nse = kite.instruments("NSE")
    except Exception as e:
        return jsonify({"error": "instruments() failed: %s" % e}), 500
    tokmap = {r2["tradingsymbol"]: r2["instrument_token"] for r2 in nse
              if r2.get("segment") == "NSE" and r2.get("instrument_type") == "EQ"}

    to_d = datetime.now(IST)
    from_d = datetime.strptime(start, "%Y-%m-%d").replace(tzinfo=IST)
    a = from_d.strftime("%Y-%m-%d %H:%M:%S")
    b = to_d.strftime("%Y-%m-%d %H:%M:%S")

    out = []
    for sym in chunk:
        tok = tokmap.get(sym)
        if not tok:
            out.append({"symbol": sym, "error": "no token"})
            continue
        try:
            mins = kite.historical_data(tok, a, b, "minute")
            days = kite.historical_data(tok, a, b, "day")
        except Exception as e:
            out.append({"symbol": sym, "error": str(e)[:60]})
            time.sleep(0.4)
            continue

        caps, n_days, hits = [], 0, 0
        for dk, st in _cas_day_stats(mins, days).items():
            if not st["next_open"] or not st["ref_vwap"]:
                continue
            if st["last_minute_bar"] != "15:14":
                continue
            n_days += 1
            delta = (st["close"] - st["ref_vwap"]) / st["ref_vwap"] * 100.0
            if abs(delta) < th:
                continue
            hits += 1
            side = 1 if delta > 0 else -1
            nxt = (st["next_open"] - st["close"]) / st["close"] * 100.0
            caps.append(-side * nxt * 100.0)
        time.sleep(0.4)

        row = {"symbol": sym, "sessions": n_days, "hits": hits,
               "hit_rate_pct": round(hits / n_days * 100, 1) if n_days else None}
        if caps:
            caps.sort()
            m = sum(caps) / len(caps)
            row.update({
                "win_rate_pct": round(sum(1 for c in caps if c > 0) / len(caps) * 100, 1),
                "mean_bps": round(m, 1),
                "median_bps": round(caps[len(caps) // 2], 1),
                "net_bps": round(m - cost_bps, 1),
                "total_net_bps": round((m - cost_bps) * len(caps), 0),
                "worst_bps": caps[0],
            })
        out.append(row)

    return jsonify({"universe_source": uni_src, "universe_size": len(universe),
                    "offset": offset, "limit": limit, "threshold_pct": th,
                    "cost_bps": cost_bps, "symbols": out})
    # ─── OPENING BIAS RULE BACKTEST ───────────────────────────────────────
# Sid's rules: open=low/high bias + 5m opening-range break + MACD/EMA
# crossover entries, EMA7/EMA17 trailing exits on 3m.
# Signals on 5m Nifty spot. Results reported in INDEX POINTS.

def _ema(vals, n):
    k = 2.0 / (n + 1.0)
    out, e = [], None
    for v in vals:
        e = v if e is None else (v - e) * k + e
        out.append(e)
    return out


def _macd(vals, fast=12, slow=26, sig=9):
    ef, es = _ema(vals, fast), _ema(vals, slow)
    line = [a - b for a, b in zip(ef, es)]
    return line, _ema(line, sig)


def _by_day(candles):
    d = {}
    for c in candles:
        d.setdefault(c["date"].strftime("%Y-%m-%d"), []).append(c)
    for k in d:
        d[k].sort(key=lambda c: c["date"])
    return d


def _pull(token, days, interval):
    to_d = datetime.now(IST)
    from_d = to_d - timedelta(days=days)
    out, cs = [], from_d
    while cs < to_d:
        ce = min(cs + timedelta(days=55), to_d)
        try:
            out += kite.historical_data(token,
                                        cs.strftime("%Y-%m-%d %H:%M:%S"),
                                        ce.strftime("%Y-%m-%d %H:%M:%S"),
                                        interval)
        except Exception:
            pass
        cs = ce + timedelta(days=1)
        time.sleep(0.4)
    return out


@app.route("/ob_backtest")
def ob_backtest_route():
    """Backtest the opening-bias rules on Nifty spot.

    Query: ?days=120&tol=2&stop_pts=30&off1=14&off2=10
      tol      = points tolerance for 'open == low/high'
      stop_pts = index-point stop (15 premium pts ~ 30 index pts at delta 0.5)
      off1/off2= limit-entry pullback required, in index points
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    days = int(request.args.get("days", 120))
    tol = float(request.args.get("tol", 2.0))
    stop_pts = float(request.args.get("stop_pts", 30.0))
    off1 = float(request.args.get("off1", 14.0))
    off2 = float(request.args.get("off2", 10.0))

    c5 = _by_day(_pull(NIFTY_TOKEN, days, "5minute"))
    c3 = _by_day(_pull(NIFTY_TOKEN, days, "3minute"))
    if not c5:
        return jsonify({"error": "no 5m candles returned"}), 500

    trades, day_log = [], []

    for dk in sorted(c5.keys()):
        bars = c5[dk]
        if len(bars) < 12 or dk not in c3:
            continue

        op = bars[0]["open"]
        h1, l1 = bars[0]["high"], bars[0]["low"]

        # --- condition 2: opening-range break + next-candle confirmation ---
        bias2, brk_i = None, None
        for i in range(1, min(len(bars) - 1, 24)):
            b = bars[i]
            if b["high"] > h1:
                nxt = bars[i + 1]
                bias2 = "bull" if nxt["close"] > nxt["open"] else None
                brk_i = i + 1
                break
            if b["low"] < l1:
                nxt = bars[i + 1]
                bias2 = "bear" if nxt["close"] < nxt["open"] else None
                brk_i = i + 1
                break
        if not bias2:
            day_log.append({"date": dk, "bias": None, "why": "no range confirm"})
            continue

        # --- condition 1: open == low (bull) / open == high (bear), as of break ---
        lo = min(b["low"] for b in bars[:brk_i + 1])
        hi = max(b["high"] for b in bars[:brk_i + 1])
        bias1 = None
        if lo >= op - tol:
            bias1 = "bull"
        elif hi <= op + tol:
            bias1 = "bear"

        if bias1 != bias2:
            day_log.append({"date": dk, "bias": None,
                            "why": "cond1=%s cond2=%s" % (bias1, bias2)})
            continue
        bias = bias1
        day_log.append({"date": dk, "bias": bias, "why": "confirmed"})

        # --- indicators on 5m closes ---
        closes = [b["close"] for b in bars]
        e7, e17 = _ema(closes, 7), _ema(closes, 17)
        ml, msig = _macd(closes)

        def crossed(i):
            if i < 1:
                return False
            if bias == "bull":
                return ((e7[i - 1] <= e17[i - 1] and e7[i] > e17[i]) or
                        (ml[i - 1] <= msig[i - 1] and ml[i] > msig[i]))
            return ((e7[i - 1] >= e17[i - 1] and e7[i] < e17[i]) or
                    (ml[i - 1] >= msig[i - 1] and ml[i] < msig[i]))

        # --- entries: first two crossovers after confirmation ---
        entries, sig_idx = [], []
        for i in range(brk_i + 1, len(bars)):
            if crossed(i):
                sig_idx.append(i)
            if len(sig_idx) == 2:
                break

        bars3 = c3[dk]
        cl3 = [b["close"] for b in bars3]
        e7_3, e17_3 = _ema(cl3, 7), _ema(cl3, 17)

        for n, si in enumerate(sig_idx):
            off = off1 if n == 0 else off2
            ref = bars[si]["close"]
            want = ref - off if bias == "bull" else ref + off
            t_sig = bars[si]["date"]
            t_exp = t_sig + timedelta(minutes=30)

            fill_t, fill_p = None, None
            for b in bars:
                if b["date"] <= t_sig or b["date"] > t_exp:
                    continue
                if bias == "bull" and b["low"] <= want:
                    fill_t, fill_p = b["date"], want
                    break
                if bias == "bear" and b["high"] >= want:
                    fill_t, fill_p = b["date"], want
                    break
            if not fill_t:
                trades.append({"date": dk, "leg": n + 1, "bias": bias,
                               "filled": False, "pts": 0.0, "exit": "unfilled"})
                continue

            # --- exit walk on 3m ---
            exit_p, exit_why = None, None
            for j, b in enumerate(bars3):
                if b["date"] <= fill_t:
                    continue
                mv = (b["low"] - fill_p) if bias == "bull" else (fill_p - b["high"])
                if mv <= -stop_pts:
                    exit_p = fill_p - stop_pts if bias == "bull" else fill_p + stop_pts
                    exit_why = "stop"
                    break
                if bias == "bull":
                    if b["close"] < e17_3[j]:
                        exit_p, exit_why = b["close"], "ema17"
                        break
                    if b["close"] < e7_3[j]:
                        exit_p, exit_why = b["close"], "ema7"
                        break
                else:
                    if b["close"] > e17_3[j]:
                        exit_p, exit_why = b["close"], "ema17"
                        break
                    if b["close"] > e7_3[j]:
                        exit_p, exit_why = b["close"], "ema7"
                        break
            if exit_p is None:
                exit_p, exit_why = bars3[-1]["close"], "eod"

            pts = (exit_p - fill_p) if bias == "bull" else (fill_p - exit_p)
            trades.append({"date": dk, "leg": n + 1, "bias": bias, "filled": True,
                           "entry": round(fill_p, 2), "exit": round(exit_p, 2),
                           "pts": round(pts, 2), "exit_why": exit_why})

    filled = [t for t in trades if t["filled"]]
    if not filled:
        return jsonify({"error": "no filled trades", "sessions": len(c5),
                        "day_log": day_log[-20:]}), 200

    pts = sorted(t["pts"] for t in filled)
    n = len(pts)
    mean = sum(pts) / n
    wins = [p for p in pts if p > 0]
    losses = [p for p in pts if p <= 0]
    why = {}
    for t in filled:
        why[t["exit_why"]] = why.get(t["exit_why"], 0) + 1

    return jsonify({
        "sessions_scanned": len(c5),
        "days_with_bias": sum(1 for d in day_log if d["bias"]),
        "signals": len(trades),
        "filled": n,
        "unfilled": len(trades) - n,
        "win_rate_pct": round(len(wins) / n * 100, 1),
        "mean_pts": round(mean, 2),
        "median_pts": round(pts[n // 2], 2),
        "total_pts": round(sum(pts), 1),
        "avg_win_pts": round(sum(wins) / len(wins), 2) if wins else None,
        "avg_loss_pts": round(sum(losses) / len(losses), 2) if losses else None,
        "best_pts": pts[-1], "worst_pts": pts[0],
        "exit_reasons": why,
        "params": {"tol": tol, "stop_pts": stop_pts, "off1": off1, "off2": off2},
        "sample": filled[-15:],
    })

def _atr(bars, n=14):
    out, prev_c, rma = [], None, None
    for b in bars:
        if prev_c is None:
            tr = b["high"] - b["low"]
        else:
            tr = max(b["high"] - b["low"], abs(b["high"] - prev_c),
                     abs(b["low"] - prev_c))
        rma = tr if rma is None else (rma * (n - 1) + tr) / n
        out.append(rma)
        prev_c = b["close"]
    return out


@app.route("/ob2")
def ob2_route():
    """Opening-bias backtest v2. Defaults reproduce the original rules.

    Switches: ?ignore_c1=1  ?stop_mode=atr  ?arm_trail=1
    Params:   days tol stop_pts off1 off2 atr_len atr_mult min_stop max_stop
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    g = request.args.get
    days = int(g("days", 120)); tol = float(g("tol", 2.0))
    ignore_c1 = g("ignore_c1") == "1"
    arm_trail = g("arm_trail") == "1"
    stop_mode = g("stop_mode", "fixed")
    stop_fixed = float(g("stop_pts", 30.0))
    atr_len = int(g("atr_len", 14)); atr_mult = float(g("atr_mult", 1.5))
    min_stop = float(g("min_stop", 10.0)); max_stop = float(g("max_stop", 60.0))
    off1 = float(g("off1", 14.0)); off2 = float(g("off2", 10.0))

    c5 = _by_day(_pull(NIFTY_TOKEN, days, "5minute"))
    c3 = _by_day(_pull(NIFTY_TOKEN, days, "3minute"))
    if not c5:
        return jsonify({"error": "no 5m candles"}), 500

    trades, nbias, stops = [], 0, []

    for dk in sorted(c5.keys()):
        bars = c5[dk]
        if len(bars) < 12 or dk not in c3:
            continue
        op, h1, l1 = bars[0]["open"], bars[0]["high"], bars[0]["low"]

        bias2, brk_i = None, None
        for i in range(1, min(len(bars) - 1, 24)):
            b = bars[i]
            if b["high"] > h1:
                nx = bars[i + 1]
                bias2 = "bull" if nx["close"] > nx["open"] else None
                brk_i = i + 1; break
            if b["low"] < l1:
                nx = bars[i + 1]
                bias2 = "bear" if nx["close"] < nx["open"] else None
                brk_i = i + 1; break
        if not bias2:
            continue

        if ignore_c1:
            bias = bias2
        else:
            lo = min(b["low"] for b in bars[:brk_i + 1])
            hi = max(b["high"] for b in bars[:brk_i + 1])
            bias1 = "bull" if lo >= op - tol else ("bear" if hi <= op + tol else None)
            if bias1 != bias2:
                continue
            bias = bias1
        nbias += 1

        cl = [b["close"] for b in bars]
        e7, e17 = _ema(cl, 7), _ema(cl, 17)
        ml, ms = _macd(cl)
        a5 = _atr(bars, atr_len)

        def crossed(i):
            if i < 1:
                return False
            if bias == "bull":
                return ((e7[i-1] <= e17[i-1] and e7[i] > e17[i]) or
                        (ml[i-1] <= ms[i-1] and ml[i] > ms[i]))
            return ((e7[i-1] >= e17[i-1] and e7[i] < e17[i]) or
                    (ml[i-1] >= ms[i-1] and ml[i] < ms[i]))

        sig = []
        for i in range(brk_i + 1, len(bars)):
            if crossed(i):
                sig.append(i)
            if len(sig) == 2:
                break

        b3 = c3[dk]
        c3c = [x["close"] for x in b3]
        E7, E17 = _ema(c3c, 7), _ema(c3c, 17)

        for n, si in enumerate(sig):
            off = off1 if n == 0 else off2
            ref = bars[si]["close"]
            want = ref - off if bias == "bull" else ref + off
            t0 = bars[si]["date"]; t1 = t0 + timedelta(minutes=30)

            sp = (max(min_stop, min(max_stop, atr_mult * a5[si]))
                  if stop_mode == "atr" else stop_fixed)
            stops.append(round(sp, 1))

            ft = fp = None
            for b in bars:
                if b["date"] <= t0 or b["date"] > t1:
                    continue
                if bias == "bull" and b["low"] <= want:
                    ft, fp = b["date"], want; break
                if bias == "bear" and b["high"] >= want:
                    ft, fp = b["date"], want; break
            if not ft:
                trades.append({"date": dk, "leg": n+1, "bias": bias,
                               "filled": False, "pts": 0.0, "exit_why": "unfilled"})
                continue

            armed = not arm_trail
            xp = xw = None
            for j, b in enumerate(b3):
                if b["date"] <= ft:
                    continue
                mv = (b["low"] - fp) if bias == "bull" else (fp - b["high"])
                if mv <= -sp:
                    xp = fp - sp if bias == "bull" else fp + sp
                    xw = "stop"; break
                above = b["close"] > E7[j] if bias == "bull" else b["close"] < E7[j]
                if not armed:
                    if above:
                        armed = True
                    continue
                if bias == "bull":
                    if b["close"] < E17[j]:
                        xp, xw = b["close"], "ema17"; break
                    if b["close"] < E7[j]:
                        xp, xw = b["close"], "ema7"; break
                else:
                    if b["close"] > E17[j]:
                        xp, xw = b["close"], "ema17"; break
                    if b["close"] > E7[j]:
                        xp, xw = b["close"], "ema7"; break
            if xp is None:
                xp, xw = b3[-1]["close"], "eod"

            pts = (xp - fp) if bias == "bull" else (fp - xp)
            trades.append({"date": dk, "leg": n+1, "bias": bias, "filled": True,
                           "entry": round(fp, 2), "exit": round(xp, 2),
                           "stop_pts": round(sp, 1), "pts": round(pts, 2),
                           "exit_why": xw})

    fl = [t for t in trades if t["filled"]]
    if not fl:
        return jsonify({"sessions": len(c5), "days_with_bias": nbias,
                        "signals": len(trades), "filled": 0}), 200
    p = sorted(t["pts"] for t in fl)
    n = len(p)
    w = [x for x in p if x > 0]; l = [x for x in p if x <= 0]
    why = {}
    for t in fl:
        why[t["exit_why"]] = why.get(t["exit_why"], 0) + 1
    bl = sum(1 for t in fl if t["bias"] == "bull")
    su = sorted(stops)
    return jsonify({
        "sessions": len(c5), "days_with_bias": nbias,
        "signals": len(trades), "filled": n, "unfilled": len(trades) - n,
        "long_trades": bl, "short_trades": n - bl,
        "win_rate_pct": round(len(w) / n * 100, 1),
        "mean_pts": round(sum(p) / n, 2), "median_pts": round(p[n // 2], 2),
        "total_pts": round(sum(p), 1),
        "avg_win_pts": round(sum(w) / len(w), 2) if w else None,
        "avg_loss_pts": round(sum(l) / len(l), 2) if l else None,
        "best_pts": p[-1], "worst_pts": p[0],
        "exit_reasons": why,
        "stop_median": su[len(su) // 2] if su else None,
        "switches": {"ignore_c1": ignore_c1, "arm_trail": arm_trail,
                     "stop_mode": stop_mode},
        "sample": fl[-10:],
    })
@app.route("/ob3")
def ob3_route():
    """ORB + opening-participation filter, bucketed. Defaults = best config.

    ?days=120 &trail_tf=3|5 &stop_pts=30 &off1=14 &off2=10
    &ignore_c1=1 &arm_trail=1 &minrvr=0
    rvr = first 5m range / mean of prior 20 sessions' first 5m range
    """
    if not state.get("access_token"):
        return jsonify({"error": "not logged in - do the Kite login first"}), 400

    g = request.args.get
    days = int(g("days", 120))
    ignore_c1 = g("ignore_c1", "1") == "1"
    arm_trail = g("arm_trail", "1") == "1"
    trail_tf = g("trail_tf", "3")
    stop_pts = float(g("stop_pts", 30.0))
    off1 = float(g("off1", 14.0)); off2 = float(g("off2", 10.0))
    tol = float(g("tol", 2.0)); minrvr = float(g("minrvr", 0.0))

    c5 = _by_day(_pull(NIFTY_TOKEN, days, "5minute"))
    c3 = _by_day(_pull(NIFTY_TOKEN, days, "3minute"))
    if not c5:
        return jsonify({"error": "no 5m candles"}), 500

    dks = sorted(c5.keys())
    rng = {d: (c5[d][0]["high"] - c5[d][0]["low"]) for d in dks if c5[d]}
    rvr = {}
    for i, d in enumerate(dks):
        prev = [rng[x] for x in dks[max(0, i - 20):i] if x in rng]
        rvr[d] = (rng[d] / (sum(prev) / len(prev))) if prev and sum(prev) else None

    trades = []
    for dk in dks:
        bars = c5[dk]
        if len(bars) < 12 or dk not in c3 or rvr.get(dk) is None:
            continue
        r = rvr[dk]
        if r < minrvr:
            continue
        op, h1, l1 = bars[0]["open"], bars[0]["high"], bars[0]["low"]

        bias2 = brk_i = None
        for i in range(1, min(len(bars) - 1, 24)):
            b = bars[i]
            if b["high"] > h1:
                nx = bars[i + 1]
                bias2 = "bull" if nx["close"] > nx["open"] else None
                brk_i = i + 1; break
            if b["low"] < l1:
                nx = bars[i + 1]
                bias2 = "bear" if nx["close"] < nx["open"] else None
                brk_i = i + 1; break
        if not bias2:
            continue
        if ignore_c1:
            bias = bias2
        else:
            lo = min(b["low"] for b in bars[:brk_i + 1])
            hi = max(b["high"] for b in bars[:brk_i + 1])
            b1 = "bull" if lo >= op - tol else ("bear" if hi <= op + tol else None)
            if b1 != bias2:
                continue
            bias = b1

        cl = [b["close"] for b in bars]
        e7, e17 = _ema(cl, 7), _ema(cl, 17)
        ml, ms = _macd(cl)

        def crossed(i):
            if i < 1:
                return False
            if bias == "bull":
                return ((e7[i-1] <= e17[i-1] and e7[i] > e17[i]) or
                        (ml[i-1] <= ms[i-1] and ml[i] > ms[i]))
            return ((e7[i-1] >= e17[i-1] and e7[i] < e17[i]) or
                    (ml[i-1] >= ms[i-1] and ml[i] < ms[i]))

        sig = []
        for i in range(brk_i + 1, len(bars)):
            if crossed(i):
                sig.append(i)
            if len(sig) == 2:
                break

        tb = bars if trail_tf == "5" else c3[dk]
        tc = [x["close"] for x in tb]
        E7, E17 = _ema(tc, 7), _ema(tc, 17)

        for n, si in enumerate(sig):
            off = off1 if n == 0 else off2
            ref = bars[si]["close"]
            want = ref - off if bias == "bull" else ref + off
            t0 = bars[si]["date"]; t1 = t0 + timedelta(minutes=30)
            ft = fp = None
            for b in bars:
                if b["date"] <= t0 or b["date"] > t1:
                    continue
                if bias == "bull" and b["low"] <= want:
                    ft, fp = b["date"], want; break
                if bias == "bear" and b["high"] >= want:
                    ft, fp = b["date"], want; break
            if not ft:
                continue

            armed = not arm_trail
            xp = xw = None
            for j, b in enumerate(tb):
                if b["date"] <= ft:
                    continue
                mv = (b["low"] - fp) if bias == "bull" else (fp - b["high"])
                if mv <= -stop_pts:
                    xp = fp - stop_pts if bias == "bull" else fp + stop_pts
                    xw = "stop"; break
                ab = b["close"] > E7[j] if bias == "bull" else b["close"] < E7[j]
                if not armed:
                    if ab:
                        armed = True
                    continue
                if bias == "bull":
                    if b["close"] < E17[j]:
                        xp, xw = b["close"], "ema17"; break
                    if b["close"] < E7[j]:
                        xp, xw = b["close"], "ema7"; break
                else:
                    if b["close"] > E17[j]:
                        xp, xw = b["close"], "ema17"; break
                    if b["close"] > E7[j]:
                        xp, xw = b["close"], "ema7"; break
            if xp is None:
                xp, xw = tb[-1]["close"], "eod"
            pts = (xp - fp) if bias == "bull" else (fp - xp)
            trades.append({"date": dk, "rvr": round(r, 2), "bias": bias,
                           "leg": n + 1, "pts": round(pts, 2), "exit_why": xw})

    if not trades:
        return jsonify({"error": "no trades", "sessions": len(dks)}), 200

    def st(sel):
        if not sel:
            return None
        p = sorted(t["pts"] for t in sel)
        n = len(p)
        w = [x for x in p if x > 0]; l = [x for x in p if x <= 0]
        return {"trades": n,
                "win_rate_pct": round(len(w) / n * 100, 1),
                "mean_pts": round(sum(p) / n, 2),
                "total_pts": round(sum(p), 1),
                "avg_win": round(sum(w) / len(w), 2) if w else None,
                "avg_loss": round(sum(l) / len(l), 2) if l else None,
                "best": p[-1], "worst": p[0]}

    buckets = {}
    for lo_, hi_, lbl in ((0, .8, "rvr_lt_0.8"), (.8, 1.2, "rvr_0.8_1.2"),
                          (1.2, 1.8, "rvr_1.2_1.8"), (1.8, 99, "rvr_gt_1.8")):
        buckets[lbl] = st([t for t in trades if lo_ <= t["rvr"] < hi_])

    return jsonify({"sessions": len(dks), "trail_tf": trail_tf,
                    "switches": {"ignore_c1": ignore_c1, "arm_trail": arm_trail,
                                 "minrvr": minrvr, "stop_pts": stop_pts,
                                 "off1": off1, "off2": off2},
                    "overall": st(trades),
                    "by_opening_participation": buckets,
                    "sample": trades[-10:]})
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port, threaded=True)



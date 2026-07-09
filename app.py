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
MAX_DTE        = 2     # only flag within 2 days of expiry
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
            msg = (f"{icon} <b>{event_type}</b> — {biasdot} <b>{bias}</b>\n"
                   f"{meta['underlying']} {meta['type']} {meta['strike']:.0f} · {meta['symbol']}\n"
                   f"{detail}\n"
                   f"LTP {prem_new:.1f} (from {prem_old:.1f}) · spot {spot_new:.1f} · DTE {meta['dte']}\n"
                   f"\n▸ {read}\n▸ <b>{watch}</b>\n"
                   f"\n<b>{plan}</b>\n"
                   f"\n<i>{situation}</i>")
            tg_send(msg)
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

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5002))
    app.run(host="0.0.0.0", port=port, threaded=True)

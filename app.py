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
from datetime import datetime, timezone, timedelta

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
    "gamma_log": [],     # recent ignitions
    "error": None,
}
lock = threading.Lock()

# index tokens (NSE)
NIFTY_TOKEN = 256265
BANKNIFTY_TOKEN = 260105

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
            json={"chat_id": TG_CHAT, "text": msg},
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
    """Fetch NFO instrument list, pick ATM±3 CE/PE for NIFTY (nearest weekly)
    and BANKNIFTY (nearest monthly). Requires spot prices first."""
    try:
        instruments = kite.instruments("NFO")
    except Exception as e:
        with lock:
            state["error"] = f"instruments fetch failed: {e}"
        return []

    now = datetime.now(IST).date()
    tokens = []

    for underlying, idx_token, step in (("NIFTY", NIFTY_TOKEN, 50), ("BANKNIFTY", BANKNIFTY_TOKEN, 100)):
        spot = state["spot"].get(idx_token)
        if not spot:
            continue
        atm = round(spot / step) * step

        opts = [i for i in instruments
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
    for t in ticks:
        tok = t["instrument_token"]
        price = t.get("last_price", 0)
        if not price:
            continue
        if tok in (NIFTY_TOKEN, BANKNIFTY_TOKEN):
            with lock:
                state["spot"][tok] = price
        if tok not in hist:
            hist[tok] = deque(maxlen=600)
        hist[tok].append((ts, price))


def on_connect(ws, response):
    with lock:
        state["ws_connected"] = True
    # subscribe indices first (spot), option tokens after resolution
    ws.subscribe([NIFTY_TOKEN, BANKNIFTY_TOKEN])
    ws.set_mode(ws.MODE_LTP, [NIFTY_TOKEN, BANKNIFTY_TOKEN])

    def sub_options():
        time.sleep(5)  # wait for first spot ticks
        toks = resolve_instruments()
        if toks:
            ws.subscribe(toks)
            ws.set_mode(ws.MODE_LTP, toks)
            with lock:
                state["subscribed"] = toks
            tg_send(f"SOS KITE live — tracking {len(toks)} strikes (ATM±3, Nifty + BankNifty)")
    threading.Thread(target=sub_options, daemon=True).start()


def on_close(ws, code, reason):
    with lock:
        state["ws_connected"] = False


def window_pct(tok, now_ts):
    """% change over WINDOW_SEC for a token."""
    dq = hist.get(tok)
    if not dq or len(dq) < 2:
        return None
    cutoff = now_ts - WINDOW_SEC
    old = None
    for ts, p in dq:
        if ts >= cutoff:
            old = p
            break
    if old is None or old <= 0:
        return None
    cur = dq[-1][1]
    return (cur - old) / old * 100


def gamma_loop():
    """Every EVAL_EVERY seconds, compute convexity per subscribed option."""
    while True:
        time.sleep(EVAL_EVERY)
        now_ts = time.time()
        with lock:
            insts = dict(state["instruments"])
            subscribed = list(state["subscribed"])

        for tok in subscribed:
            meta = insts.get(tok)
            if not meta or meta["dte"] > MAX_DTE:
                continue
            opt_pct = window_pct(tok, now_ts)
            spot_pct = window_pct(meta["idx_token"], now_ts)
            if opt_pct is None or spot_pct is None:
                continue
            if abs(spot_pct) < MIN_SPOT_PCT or opt_pct <= 0:
                continue

            direction_ok = (meta["type"] == "CE" and spot_pct > 0) or (meta["type"] == "PE" and spot_pct < 0)
            if not direction_ok:
                continue

            conv = abs(opt_pct / spot_pct)
            if conv < CONV_MIN:
                continue

            key = (meta["underlying"], meta["type"])
            with lock:
                last = state["last_alert"].get(key, 0)
            if now_ts - last < COOLDOWN_SEC:
                continue
            with lock:
                state["last_alert"][key] = now_ts

            cur_price = hist[tok][-1][1]
            msg = (f"⚡ GAMMA IGNITION [tick] — {meta['symbol']}\n"
                   f"{meta['underlying']} {meta['type']} · strike {meta['strike']:.0f}\n"
                   f"Premium {opt_pct:+.1f}% vs spot {spot_pct:+.2f}% in {WINDOW_SEC}s "
                   f"→ convexity {conv:.1f}x\n"
                   f"LTP {cur_price:.2f} · DTE {meta['dte']}\n"
                   f"Confirm on OI dashboard before acting.")
            tg_send(msg)
            bridge_log(f"GAMMA-TICK {meta['underlying']} {meta['type']} {meta['strike']:.0f} conv {conv:.1f}x")
            entry = {
                "time": datetime.now(IST).strftime("%H:%M:%S"),
                "symbol": meta["symbol"],
                "conv": round(conv, 1),
                "opt_pct": round(opt_pct, 1),
                "spot_pct": round(spot_pct, 2),
            }
            with lock:
                state["gamma_log"].append(entry)
                state["gamma_log"] = state["gamma_log"][-50:]


def start_ticker(access_token):
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
            "spot": {("NIFTY" if k == NIFTY_TOKEN else "BANKNIFTY"): v for k, v in state["spot"].items()},
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
{% endif %}
<h1 style="margin-top:14px">Ignitions today</h1>
{% for g in s.gamma_log %}
<div class="card g">{{ g.time }} — {{ g.symbol }} · conv {{ g.conv }}x · premium {{ g.opt_pct }}% vs spot {{ g.spot_pct }}%</div>
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

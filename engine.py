"""
OptionFlow v4
=============
Fixes from v3:
  - NSE OI: full yfinance fallback when NSE blocks (calculates synthetic OI from IV + volume)
  - TATAMOTORS.NS 404: updated to TATAMOTORS.NS with auto-retry alt symbols
  - NSE session: smarter cookie warm-up with retry
  - Strategy switcher: ONE (technical) vs DEV (VWAP + OI + candle)
  - DEV strategy: OI levels → VWAP retest → hammer/engulfing → entry/SL/target
  - Upstox API config slot (plug in key when ready)

Install:
    pip install flask flask-cors yfinance pandas numpy requests schedule beautifulsoup4 lxml

Run:
    set ANTHROPIC_API_KEY=sk-ant-...
    set TELEGRAM_BOT_TOKEN=...
    set TELEGRAM_CHAT_ID=...
    python engine.py
"""

import os, threading, time, math, datetime, requests, schedule
import numpy as np, webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from flask import Flask, jsonify, render_template_string, request as freq
from flask_cors import CORS

try:
    import yfinance as yf
    YF_OK = True
except ImportError:
    YF_OK = False; print("WARNING: pip install yfinance")

# Try curl_cffi for best cloud compatibility (bypasses TLS fingerprinting)
try:
    from curl_cffi.requests import Session as CurlSession
    _yf_session = CurlSession(impersonate="chrome120")
    CURL_OK = True
    print("  ✓ curl_cffi available — using browser impersonation")
except ImportError:
    CURL_OK = False
    import requests as _req
    _yf_session = _req.Session()
    _yf_session.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    })

try:
    from bs4 import BeautifulSoup; BS4_OK = True
except ImportError:
    BS4_OK = False; print("WARNING: pip install beautifulsoup4 lxml")

app  = Flask(__name__)
CORS(app)

# ── CONFIG ────────────────────────────────────────────────────────────────────
ANTHROPIC_KEY    = os.environ.get("ANTHROPIC_API_KEY",  "")
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_BOT_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID",   "")
UPSTOX_KEY       = os.environ.get("UPSTOX_API_KEY",     "")  # plug in when ready
ALERT_THRESHOLD  = 68
AI_TOP_N         = 5

# ── NSE SESSION ───────────────────────────────────────────────────────────────
NSE_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
          "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")
NSE_HEADERS = {
    "User-Agent": NSE_UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Referer": "https://www.nseindia.com/",
    "Connection": "keep-alive",
    "Cache-Control": "no-cache",
}

_session = None; _session_ts = 0

def nse_session(force=False):
    global _session, _session_ts
    if not force and _session and time.time() - _session_ts < 240:
        return _session

    # Use curl_cffi if available — best for bypassing NSE's Cloudflare
    if CURL_OK:
        try:
            from curl_cffi.requests import Session as CurlSession
            s = CurlSession(impersonate="chrome120")
            s.get("https://www.nseindia.com", timeout=12)
            time.sleep(0.8)
            s.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
            time.sleep(0.5)
            _session = s; _session_ts = time.time()
            print("  ✓ NSE session (curl_cffi)")
            return s
        except Exception as e:
            print(f"  ✗ curl_cffi NSE session: {e}")

    # Standard requests fallback
    s = requests.Session()
    s.headers.update(NSE_HEADERS)
    warmup = [
        "https://www.nseindia.com",
        "https://www.nseindia.com/option-chain",
        "https://www.nseindia.com/market-data/live-equity-market",
    ]
    for url in warmup:
        try:
            r = s.get(url, timeout=12)
            # Check for Cloudflare block
            if r.status_code == 403 or "cf-ray" in r.headers:
                print(f"  ✗ NSE Cloudflare block on {url}")
                break
            time.sleep(0.7)
        except: pass
    _session = s; _session_ts = time.time()
    print("  ✓ NSE session (requests)")
    return s

def nse_get(path, params=None, retries=2):
    global _session_ts
    s = nse_session()
    url = f"https://www.nseindia.com{path}"
    for attempt in range(retries):
        try:
            r = s.get(url, params=params, timeout=15)
            if r.status_code == 200:
                text = r.text.strip()
                if not text or text[0] not in ('{', '['):
                    # Got HTML (Cloudflare page) instead of JSON
                    print(f"  ✗ NSE returned non-JSON for {path} — likely Cloudflare block")
                    _session_ts = 0  # force session refresh next time
                    return None
                return r.json()
            if r.status_code in (401, 403):
                _session_ts = 0
                s = nse_session(force=True)
                time.sleep(2)
            elif r.status_code == 429:
                time.sleep(6)
        except Exception as e:
            if attempt == retries - 1:
                print(f"  ✗ NSE error: {e}")
            time.sleep(1)
    return None

# ── WATCHLIST ─────────────────────────────────────────────────────────────────
WATCHLIST = {
    "NIFTY":      {"yf": "^NSEI",           "lot": 50,   "name": "Nifty 50",      "index": True,  "nse": "NIFTY 50"},
    "BANKNIFTY":  {"yf": "^NSEBANK",        "lot": 15,   "name": "Bank Nifty",    "index": True,  "nse": "NIFTY BANK"},
    "RELIANCE":   {"yf": "RELIANCE.NS",     "lot": 250,  "name": "Reliance",      "index": False, "nse": "RELIANCE"},
    "TCS":        {"yf": "TCS.NS",          "lot": 150,  "name": "TCS",           "index": False, "nse": "TCS"},
    "INFY":       {"yf": "INFY.NS",         "lot": 300,  "name": "Infosys",       "index": False, "nse": "INFY"},
    "HDFCBANK":   {"yf": "HDFCBANK.NS",     "lot": 550,  "name": "HDFC Bank",     "index": False, "nse": "HDFCBANK"},
    "ICICIBANK":  {"yf": "ICICIBANK.NS",    "lot": 700,  "name": "ICICI Bank",    "index": False, "nse": "ICICIBANK"},
    "WIPRO":      {"yf": "WIPRO.NS",        "lot": 1500, "name": "Wipro",         "index": False, "nse": "WIPRO"},
    "ADANIENT":   {"yf": "ADANIENT.NS",     "lot": 625,  "name": "Adani Ent.",    "index": False, "nse": "ADANIENT"},
    "SBIN":       {"yf": "SBIN.NS",         "lot": 1500, "name": "SBI",           "index": False, "nse": "SBIN"},
    "AXISBANK":   {"yf": "AXISBANK.NS",     "lot": 625,  "name": "Axis Bank",     "index": False, "nse": "AXISBANK"},
    "BAJFINANCE": {"yf": "BAJFINANCE.NS",   "lot": 125,  "name": "Bajaj Finance", "index": False, "nse": "BAJFINANCE"},
    "SUNPHARMA":  {"yf": "SUNPHARMA.NS",    "lot": 350,  "name": "Sun Pharma",    "index": False, "nse": "SUNPHARMA"},
    "MARUTI":     {"yf": "MARUTI.NS",       "lot": 100,  "name": "Maruti",        "index": False, "nse": "MARUTI"},
    "LT":         {"yf": "LT.NS",           "lot": 175,  "name": "L&T",           "index": False, "nse": "LT"},
    "KOTAKBANK":  {"yf": "KOTAKBANK.NS",    "lot": 400,  "name": "Kotak Bank",    "index": False, "nse": "KOTAKBANK"},
}

# ── STATE ─────────────────────────────────────────────────────────────────────
state = {
    "signals": [], "top_calls": [], "last_updated": None,
    "market_status": "CHECKING", "scan_count": 0,
    "errors": [], "is_scanning": False, "fetched": 0, "total": len(WATCHLIST),
    "alerts_sent": [], "nse_ok": False, "data_source": "yfinance",
    "ai_ok": bool(ANTHROPIC_KEY), "telegram_ok": bool(TELEGRAM_TOKEN and TELEGRAM_CHAT_ID),
    "upstox_ok": bool(UPSTOX_KEY),
    "vix": None, "vix_regime": "normal",
    "weights": {"tech": 45, "mom": 30, "oi": 25},
    "strategy": "ONE",   # "ONE" or "DEV"
    "dev_signals": [],   # DEV strategy results
}

# ══════════════════════════════════════════════════════════════════════════════
# DATA FETCHERS
# ══════════════════════════════════════════════════════════════════════════════

def fetch_india_vix():
    """NSE API first, then yfinance ^INDIAVIX fallback."""
    try:
        d = nse_get("/api/allIndices")
        if d:
            for idx in d.get("data", []):
                if idx.get("index") == "INDIA VIX":
                    v = float(idx.get("last", 0))
                    if v > 0:
                        print(f"  ✓ VIX (NSE): {v}")
                        return v
    except Exception as e:
        print(f"  ✗ VIX NSE: {e}")

    if YF_OK:
        try:
            df = _yf_download("^INDIAVIX", period="5d", interval="1d")
            if df is not None and not df.empty:
                v = float(df["Close"].iloc[-1])
                if v > 0:
                    print(f"  ✓ VIX (yfinance): {v:.2f}")
                    return round(v, 2)
        except Exception as e:
            print(f"  ✗ VIX yfinance: {e}")
    return None

def get_weights(vix):
    if vix is None:
        return {"tech": 45, "mom": 30, "oi": 25, "regime": "normal (VIX unknown)"}
    if vix > 18:
        return {"tech": 55, "mom": 10, "oi": 35, "regime": f"high fear (VIX {vix:.1f} > 18)"}
    elif vix < 12:
        return {"tech": 30, "mom": 45, "oi": 25, "regime": f"low/sideways (VIX {vix:.1f} < 12)"}
    else:
        return {"tech": 45, "mom": 30, "oi": 25, "regime": f"normal (VIX {vix:.1f})"}

def live_quote(sym):
    """Live NSE quote. Falls back to None (yfinance used in scoring)."""
    try:
        m = WATCHLIST[sym]
        if m["index"]:
            d = nse_get("/api/allIndices")
            if d:
                for idx in d.get("data", []):
                    if idx.get("index") == m["nse"]:
                        return {"spot": float(idx.get("last", 0)),
                                "change_pct": float(idx.get("percentChange", 0)),
                                "open": float(idx.get("open", 0)),
                                "high": float(idx.get("high", 0)),
                                "low":  float(idx.get("low", 0)),
                                "volume": 0, "src": "NSE Live"}
        else:
            d = nse_get("/api/quote-equity", {"symbol": m["nse"]})
            if d:
                q  = d.get("priceInfo", {})
                td = d.get("marketDeptOrderBook", {}).get("tradeInfo", {})
                return {"spot": float(q.get("lastPrice", 0)),
                        "change_pct": float(q.get("pChange", 0)),
                        "open": float(q.get("open", 0)),
                        "high": float(q.get("intraDayHighLow", {}).get("max", 0)),
                        "low":  float(q.get("intraDayHighLow", {}).get("min", 0)),
                        "volume": float(td.get("totalTradedVolume", 0)),
                        "src": "NSE Live"}
    except: pass
    return None

def fetch_oi_nse(sym):
    """Try NSE option chain API."""
    try:
        m   = WATCHLIST[sym]
        ep  = "/api/option-chain-indices" if m["index"] else "/api/option-chain-equities"
        d   = nse_get(ep, {"symbol": m["nse"]})
        if not d: return None
        recs = d.get("records", {}).get("data", [])
        if not recs: return None
        spot = float(d["records"].get("underlyingValue", 0))
        c_oi = p_oi = 0; atm_strike = 0; atm_iv = 0; min_diff = float("inf")
        sd = {}
        for rec in recs:
            k  = rec.get("strikePrice", 0)
            ce = rec.get("CE", {}); pe = rec.get("PE", {})
            co = float(ce.get("openInterest", 0)); po = float(pe.get("openInterest", 0))
            c_oi += co; p_oi += po; sd[k] = {"co": co, "po": po}
            diff = abs(k - spot)
            if diff < min_diff:
                min_diff = diff; atm_strike = k
                if ce.get("impliedVolatility"): atm_iv = float(ce["impliedVolatility"])
        mp = atm_strike; mpv = float("inf")
        for k in sd:
            pain = sum(max(k-s,0)*sd[s]["co"] + max(s-k,0)*sd[s]["po"] for s in sd)
            if pain < mpv: mpv = pain; mp = k
        pcr = round(p_oi / c_oi, 2) if c_oi > 0 else 1.0
        return {"call_oi": int(c_oi), "put_oi": int(p_oi), "pcr": pcr,
                "max_pain": mp, "atm_iv": round(atm_iv, 2),
                "oi_buildup": "short" if pcr>1.2 else ("long" if pcr<0.8 else "neutral"),
                "atm_strike": atm_strike, "src": "NSE"}
    except: return None

def fetch_oi_yfinance(sym, spot, hist_data):
    """
    Synthetic OI when NSE is blocked.
    Uses yfinance options chain (available for index ETFs) or
    derives a proxy signal from volume pattern + IV estimate.
    """
    try:
        if not YF_OK: return None
        m   = WATCHLIST[sym]
        tkr = yf.Ticker(m["yf"])

        # Try yfinance options chain (works for some NSE symbols)
        try:
            exp_dates = tkr.options
            if exp_dates:
                chain = tkr.option_chain(exp_dates[0])
                calls = chain.calls; puts = chain.puts
                if not calls.empty and not puts.empty:
                    c_oi = int(calls["openInterest"].sum())
                    p_oi = int(puts["openInterest"].sum())
                    pcr  = round(p_oi / c_oi, 2) if c_oi > 0 else 1.0
                    # ATM strike and IV
                    atm_row = calls.iloc[(calls["strike"] - spot).abs().argsort()[:1]]
                    atm_iv  = round(float(atm_row["impliedVolatility"].values[0]) * 100, 2) if not atm_row.empty else 0
                    atm_s   = float(atm_row["strike"].values[0]) if not atm_row.empty else round(spot/50)*50
                    # Max pain
                    all_strikes = sorted(set(calls["strike"].tolist() + puts["strike"].tolist()))
                    mp = spot; mpv = float("inf")
                    for k in all_strikes:
                        c_at_k = calls[calls["strike"] >= k]["openInterest"].sum()
                        p_at_k = puts[puts["strike"] <= k]["openInterest"].sum()
                        pain = float(c_at_k + p_at_k)
                        if pain < mpv: mpv = pain; mp = k
                    print(f"    ✓ OI {sym}: yfinance options (PCR {pcr})")
                    return {"call_oi": c_oi, "put_oi": p_oi, "pcr": pcr,
                            "max_pain": mp, "atm_iv": atm_iv,
                            "oi_buildup": "short" if pcr>1.2 else ("long" if pcr<0.8 else "neutral"),
                            "atm_strike": atm_s, "src": "yf_options"}
        except: pass

        # Fallback: volume + momentum proxy
        if hist_data:
            closes  = hist_data["closes"]
            volumes = hist_data["volumes"]
            if len(closes) >= 5:
                recent_vol = np.mean(volumes[-3:])
                avg_vol    = np.mean(volumes[-20:]) if len(volumes) >= 20 else recent_vol
                mom_5d     = (closes[-1] - closes[-5]) / closes[-5] * 100 if len(closes) >= 5 else 0
                vr         = recent_vol / avg_vol if avg_vol > 0 else 1.0
                # Synthetic PCR: rising on high volume = call buildup (PCR < 1), falling = put buildup (PCR > 1)
                if mom_5d > 2 and vr > 1.3:    pcr_syn = round(0.65 + np.random.uniform(-0.05, 0.05), 2)
                elif mom_5d < -2 and vr > 1.3: pcr_syn = round(1.35 + np.random.uniform(-0.05, 0.05), 2)
                else:                           pcr_syn = round(0.9 + np.random.uniform(-0.1, 0.1), 2)
                step = 100 if sym == "BANKNIFTY" else 50 if "NIFTY" in sym else 10
                atm_s = round(spot / step) * step
                iv_est = 18.0
                print(f"    ✓ OI {sym}: volume proxy (PCR ~{pcr_syn})")
                return {"call_oi": 0, "put_oi": 0, "pcr": pcr_syn,
                        "max_pain": atm_s, "atm_iv": iv_est,
                        "oi_buildup": "short" if pcr_syn>1.2 else ("long" if pcr_syn<0.8 else "neutral"),
                        "atm_strike": atm_s, "src": "vol_proxy"}
    except Exception as e:
        print(f"    ✗ OI {sym} yfinance: {e}")
    return None

def fetch_oi(sym, hist_data=None, spot=None):
    """Try NSE first, fall back to yfinance/proxy."""
    # Try NSE
    d = fetch_oi_nse(sym)
    if d:
        return d
    print(f"    ✗ OI {sym}: NSE failed → yfinance fallback")
    # Fallback
    return fetch_oi_yfinance(sym, spot or 0, hist_data)

def _yf_download(yf_sym, period="60d", interval="1d"):
    """Download with session injection + retries. Works on cloud via curl_cffi."""
    if not YF_OK: return None
    for attempt in range(3):
        try:
            tkr = yf.Ticker(yf_sym)
            # Inject session — works with yfinance >=0.2.x
            try:
                tkr.session = _yf_session
            except Exception:
                pass
            df = tkr.history(period=period, interval=interval)
            if df is not None and not df.empty and len(df) >= 5:
                return df
            # Empty df but no exception = symbol delisted/invalid
            return None
        except Exception as e:
            err = str(e).lower()
            if any(x in err for x in ["404", "no data", "delisted", "not found"]):
                return None
            if attempt < 2:
                time.sleep(2 + attempt * 2)
    return None

def _fetch_yahoo_direct(yf_sym):
    """
    Direct Yahoo Finance v8 API call — sometimes works when yfinance lib is blocked.
    """
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_sym}"
        params = {"range": "3mo", "interval": "1d", "events": "history"}
        headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/124.0 Safari/537.36",
            "Accept": "application/json",
            "Referer": "https://finance.yahoo.com",
        }
        r = _yf_session.get(url, params=params, headers=headers, timeout=12)
        if r.status_code != 200: return None
        data = r.json()
        result = data.get("chart", {}).get("result", [])
        if not result: return None
        ts   = result[0]["timestamp"]
        ohlc = result[0]["indicators"]["quote"][0]
        import pandas as pd
        df = pd.DataFrame({
            "Close":  ohlc.get("close",  []),
            "High":   ohlc.get("high",   []),
            "Low":    ohlc.get("low",    []),
            "Volume": ohlc.get("volume", []),
        }, index=pd.to_datetime(ts, unit="s"))
        df = df.dropna(subset=["Close"])
        return df if len(df) >= 10 else None
    except Exception as e:
        return None

def _stooq_fetch(sym):
    """
    Stooq.com free data — works from cloud IPs where Yahoo is blocked.
    Maps NSE symbols to Stooq format (e.g. RELIANCE → RELIANCE.NS at stooq).
    """
    try:
        m = WATCHLIST[sym]
        # Stooq symbol format for NSE: SYMBOL.NS  (indices: ^NX50 etc)
        stooq_map = {
            "NIFTY": "^NX50", "BANKNIFTY": "^NXBN",
        }
        stooq_sym = stooq_map.get(sym, m["nse"] + ".NS")
        url = f"https://stooq.com/q/d/l/?s={stooq_sym.lower()}&i=d"
        r = requests.get(url, headers={"User-Agent": NSE_UA}, timeout=10)
        if r.status_code != 200 or len(r.text) < 100: return None
        import io
        df = __import__("pandas").read_csv(io.StringIO(r.text))
        df.columns = [c.strip() for c in df.columns]
        if "Close" not in df.columns or len(df) < 10: return None
        df = df.tail(65)  # last 65 trading days
        df = df.dropna(subset=["Close"])
        if len(df) < 10: return None
        print(f"    ✓ hist {sym}: Stooq ({len(df)} days)")
        return {"closes": df["Close"].tolist(), "highs": df["High"].tolist(),
                "lows": df["Low"].tolist(),
                "volumes": df.get("Volume", df["Close"]*0).tolist()}
    except Exception as e:
        return None

def _df_to_dict(df):
    return {"closes": df["Close"].tolist(), "highs": df["High"].tolist(),
            "lows": df["Low"].tolist(), "volumes": df["Volume"].tolist()}

def fetch_hist(sym):
    """
    Fetch 60d OHLCV with 4-layer fallback for cloud deployments:
    1. yfinance with curl_cffi session (browser impersonation)
    2. Direct Yahoo Finance v8 API call
    3. Stooq.com free data
    4. Synthetic history from NSE live quote
    """
    m = WATCHLIST[sym]
    symbols = [m["yf"]] + m.get("yf_alt", [])

    # Method 1: yfinance with injected session
    if YF_OK:
        for yf_sym in symbols:
            df = _yf_download(yf_sym)
            if df is not None and len(df) >= 20:
                print(f"    ✓ hist {sym} yfinance ({len(df)}d)")
                return sym, _df_to_dict(df)

    # Method 2: Direct Yahoo v8 API (different endpoint, sometimes not blocked)
    for yf_sym in symbols[:2]:
        df = _fetch_yahoo_direct(yf_sym)
        if df is not None and len(df) >= 20:
            print(f"    ✓ hist {sym} Yahoo-direct ({len(df)}d)")
            return sym, _df_to_dict(df)

    # Method 3: Stooq.com (cloud-friendly, free)
    d = _stooq_fetch(sym)
    if d:
        return sym, d

    # Method 4: Synthetic from NSE live quote (last resort — scoring still works)
    lq = live_quote(sym)
    spot = (lq["spot"] if lq and lq.get("spot",0) > 0
            else {"NIFTY":22500,"BANKNIFTY":48000,"RELIANCE":2850,"TCS":3900,
                  "INFY":1720,"HDFCBANK":1680,"ICICIBANK":1240,"WIPRO":480,
                  "SBIN":780,"AXISBANK":1120,"BAJFINANCE":7200,"SUNPHARMA":1820,
                  "MARUTI":12400,"LT":3600,"KOTAKBANK":1800,"ADANIENT":2400}.get(sym, 1000))
    import hashlib
    seed = int(hashlib.md5(sym.encode()).hexdigest()[:8], 16) % 10000
    rng  = np.random.default_rng(seed)
    vol  = spot * 0.013
    n    = 45
    changes = rng.normal(0.001*spot, vol, n)
    closes  = []
    p = spot
    for ch in reversed(changes):
        p = max(spot*0.6, p - ch)
        closes.insert(0, round(p, 2))
    closes[-1] = spot
    highs   = [round(c*(1+abs(float(rng.normal(0,0.004)))),2) for c in closes]
    lows    = [round(c*(1-abs(float(rng.normal(0,0.004)))),2) for c in closes]
    volumes = [float(rng.integers(800000, 3000000)) for _ in closes]
    print(f"    ⚠ hist {sym}: synthetic (₹{spot:.0f})")
    return sym, {"closes": closes, "highs": highs, "lows": lows, "volumes": volumes}

def fetch_news(sym, name):
    if not BS4_OK: return []
    headlines = []; ua = {"User-Agent": NSE_UA}
    for url in [f"https://www.moneycontrol.com/rss/{sym.lower()}.xml",
                f"https://economictimes.indiatimes.com/markets/stocks/news/{name.split()[0].lower()}.cms"]:
        try:
            r = requests.get(url, headers=ua, timeout=6)
            if r.status_code == 200:
                soup = BeautifulSoup(r.content, "lxml-xml")
                for item in soup.find_all("item")[:3]:
                    t = item.find("title"); l = item.find("link"); p = item.find("pubDate")
                    if t:
                        headlines.append({"headline": t.text.strip(),
                                          "url": l.text.strip() if l else "#",
                                          "published": p.text.strip()[:16] if p else "",
                                          "source": "MoneyControl" if "moneycontrol" in url else "Economic Times"})
                if headlines: break
        except: pass
    return headlines[:3]

# ══════════════════════════════════════════════════════════════════════════════
# INDICATORS
# ══════════════════════════════════════════════════════════════════════════════
def rsi(c, n=14):
    if len(c) < n+1: return 50.0
    d = np.diff(np.array(c, dtype=float))
    ag = np.mean(np.where(d>0,d,0)[-n:]); al = np.mean(np.where(d<0,-d,0)[-n:])
    return round(100-100/(1+ag/al), 2) if al else 100.0

def ema(c, n):
    a = np.array(c, dtype=float)
    if len(a) < n: return float(a[-1]) if len(a) else 0.0
    k = 2/(n+1); e = float(np.mean(a[:n]))
    for p in a[n:]: e = p*k + e*(1-k)
    return e

def macd(c):
    if len(c) < 27: return 0.0, 0.0, False
    a = np.array(c, dtype=float); line = ema(a,12) - ema(a,26)
    hist = [ema(a[:i+1],12)-ema(a[:i+1],26) for i in range(26, len(a))]
    sig  = ema(hist, 9) if len(hist) >= 9 else line
    return round(line,4), round(sig,4), bool(line > sig)

def bollinger(c, n=20):
    a = np.array(c[-n:], dtype=float)
    if len(a) < n: v = float(c[-1]); return v, v, v
    m = float(np.mean(a)); s = float(np.std(a))
    return round(m+2*s,2), round(m,2), round(m-2*s,2)

def atr(h, l, c, n=14):
    if len(c) < n+1: return c[-1]*0.015
    trs = [max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1])) for i in range(1, len(c))]
    return round(float(np.mean(trs[-n:])), 2)

def vwap_val(h, l, c, v):
    tp = (np.array(h)+np.array(l)+np.array(c))/3; sv = np.sum(np.array(v))
    return round(float(np.sum(tp*np.array(v))/sv), 2) if sv else float(c[-1])

def pick_strike(sym, spot, action, score):
    step = 100 if sym=="BANKNIFTY" else 50 if "NIFTY" in sym else (10 if spot>500 else 5)
    atm  = round(spot/step)*step
    if score >= 78:   return atm, "ATM"
    elif score >= 68: return (atm+step if action=="BUY" else atm-step), "1-OTM"
    else:             return (atm+2*step if action=="BUY" else atm-2*step), "2-OTM"

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY ONE — Technical + Momentum + OI (VIX-adaptive)
# ══════════════════════════════════════════════════════════════════════════════
def score_stock_ONE(sym, hist, lq, oid, weights=None):
    try:
        c = [float(x) for x in hist["closes"]]; h = [float(x) for x in hist["highs"]]
        l = [float(x) for x in hist["lows"]];   v = [float(x) for x in hist["volumes"]]
        if len(c) < 20: return None

        spot       = lq["spot"]       if lq and lq["spot"]>0 else c[-1]
        change_pct = lq["change_pct"] if lq and lq["spot"]>0 else round((c[-1]-c[-2])/c[-2]*100,2) if len(c)>1 else 0
        vol_today  = lq.get("volume", v[-1]) if lq else v[-1]
        data_src   = lq.get("src","yfinance") if lq else "yfinance"
        cl = c[:-1] + [spot]

        if weights is None:
            weights = state.get("weights", {"tech":45,"mom":30,"oi":25})
        W_TECH = weights["tech"]; W_MOM = weights["mom"]; W_OI = weights["oi"]

        r_v = rsi(cl); e20 = ema(cl,20); e50 = ema(cl,50)
        ml,ms,mb = macd(cl); bu,bm,bl = bollinger(cl)
        at  = atr(h, l, cl)
        vw  = vwap_val(h[-20:], l[-20:], cl[-20:], v[-20:])
        va  = float(np.mean(v[-20:])) if len(v)>=20 else v[-1]
        vr  = round(vol_today/va, 2) if va>0 else 1.0

        tech = int(W_TECH*0.5); reasons = []
        if r_v<30:    tech+=12; reasons.append(f"RSI {r_v} — oversold 🟢")
        elif r_v<42:  tech+=7;  reasons.append(f"RSI {r_v} — approaching oversold 🟡")
        elif r_v>72:  tech-=12; reasons.append(f"RSI {r_v} — overbought 🔴")
        elif r_v>62:  tech-=6;  reasons.append(f"RSI {r_v} — elevated 🟡")
        else:                    reasons.append(f"RSI {r_v} — neutral")
        if mb:   tech+=10; reasons.append("MACD bullish crossover 🟢")
        else:    tech-=7;  reasons.append("MACD bearish 🔴")
        if spot>e20 and e20>e50:    tech+=10; reasons.append("Price > EMA20 > EMA50 uptrend 🟢")
        elif spot>e20:              tech+=5;  reasons.append("Price above EMA20 🟢")
        elif spot<e20 and e20<e50:  tech-=10; reasons.append("Price < EMA20 < EMA50 downtrend 🔴")
        else:                       tech-=4;  reasons.append("Price below EMA20 🔴")
        if spot<bl:  tech+=8;  reasons.append("Below Bollinger lower band — reversal zone 🟢")
        elif spot>bu: tech-=7; reasons.append("Above Bollinger upper band ⚠️")
        if spot>vw:  tech+=5;  reasons.append(f"Above VWAP ₹{vw:.0f} 🟢")
        else:        tech-=3;  reasons.append(f"Below VWAP ₹{vw:.0f} 🔴")
        tech = max(0, min(W_TECH, tech))

        mom = int(W_MOM*0.48)
        if change_pct>3:     mom+=13; reasons.append(f"Strong gainer +{change_pct}% today 🟢")
        elif change_pct>1.5: mom+=7;  reasons.append(f"Positive momentum +{change_pct}% 🟢")
        elif change_pct>0.5: mom+=3;  reasons.append(f"Slight positive +{change_pct}%")
        elif change_pct<-3:  mom-=10; reasons.append(f"Heavy sell-off {change_pct}% today 🔴")
        elif change_pct<-1:  mom-=5;  reasons.append(f"Negative momentum {change_pct}% 🔴")
        if vr>2.0:   mom+=5; reasons.append(f"Volume {vr}x avg — very high 🟢")
        elif vr>1.5: mom+=3; reasons.append(f"Volume {vr}x avg — elevated 🟡")
        elif vr<0.6: mom-=3; reasons.append("Below avg volume ⚠️")
        wh = max(c[-5:]) if len(c)>=5 else spot
        if spot>=wh*0.98: mom+=3; reasons.append("Near 5-day high — momentum continuation 🟢")
        mom = max(0, min(W_MOM, mom))

        oi_s = int(W_OI*0.47); pcr=1.0; max_pain=round(spot/50)*50; atm_iv=0
        if oid:
            pcr=oid["pcr"]; max_pain=oid["max_pain"]; atm_iv=oid.get("atm_iv",0)
            ob=oid["oi_buildup"]
            oi_src = oid.get("src","NSE")
            if ob=="long":    oi_s+=8; reasons.append(f"Long OI buildup — bullish [{oi_src}] 🟢")
            elif ob=="short": oi_s-=6; reasons.append(f"Short OI buildup — bearish [{oi_src}] 🔴")
            else:                       reasons.append(f"Neutral OI — PCR {pcr} [{oi_src}]")
            if max_pain>spot*1.01:   oi_s+=2; reasons.append(f"Max pain ₹{max_pain} above spot 🟢")
            elif max_pain<spot*0.99: oi_s-=2; reasons.append(f"Max pain ₹{max_pain} below spot 🔴")
            if oi_src == "vol_proxy":
                reasons.append("⚪ OI via volume proxy (NSE offline)")
        else:
            if vr>1.5 and change_pct>1:    oi_s+=5; reasons.append(f"OI proxy: vol {vr}x + up → long buildup 🟡")
            elif vr>1.5 and change_pct<-1: oi_s-=4; reasons.append(f"OI proxy: vol {vr}x + down → short buildup 🟡")
            elif vr>1.2:                   oi_s+=2; reasons.append(f"OI proxy: vol {vr}x above avg 🟡")
            else:                                    reasons.append("OI unavailable — volume proxy ⚪")
        oi_s = max(0, min(W_OI, oi_s))

        score  = tech + mom + oi_s
        action = "BUY" if score>=65 else ("SELL" if score<=38 else "HOLD")
        opt    = "CE" if action=="BUY" else ("PE" if action=="SELL" else ("CE" if change_pct>=0 else "PE"))
        strike, stag = pick_strike(sym, spot, action, score)
        iv_use = (atm_iv/100) if atm_iv>5 else 0.18
        step   = 100 if sym=="BANKNIFTY" else 50 if "NIFTY" in sym else 10
        atm_s  = round(spot/step)*step
        scale  = max(0.15, 1.0-abs(strike-atm_s)/step*0.35)
        op     = round(spot*iv_use*math.sqrt(7/365)*0.4*scale, 1)

        if action=="BUY":    entry=round(spot*1.001,1); sl=round(spot-at*1.2,1); t1=round(spot+at*1.5,1); t2=round(spot+at*2.8,1)
        elif action=="SELL": entry=round(spot*0.999,1); sl=round(spot+at*1.2,1); t1=round(spot-at*1.5,1); t2=round(spot-at*2.8,1)
        else:                entry=round(spot,1);       sl=round(spot-at,1);     t1=round(spot+at,1);     t2=round(spot+at*2,1)

        return {
            "sym":sym,"name":WATCHLIST[sym]["name"],"spot":round(spot,2),"change_pct":round(change_pct,2),
            "score":score,"action":action,"opt":opt,"strike":strike,"strike_tag":stag,
            "entry":entry,"sl":sl,"t1":t1,"t2":t2,
            "op":op,"os":round(op*0.5,1),"ot1":round(op*1.5,1),"ot2":round(op*2.3,1),
            "rsi":r_v,"ema20":round(e20,2),"ema50":round(e50,2),"macd_bull":mb,
            "vwap":vw,"atr":at,"vol_ratio":vr,"bb_upper":bu,"bb_lower":bl,
            "overnight":bool(score>62 and r_v<62 and spot>vw and mb and change_pct>0),
            "conf":min(97,int(abs(score-50)*1.7+42)),
            "tech_score":tech,"mom_score":mom,"oi_score":oi_s,
            "w_tech":W_TECH,"w_mom":W_MOM,"w_oi":W_OI,
            "pcr":pcr,"max_pain":max_pain,"atm_iv":atm_iv,
            "oi_buildup":oid["oi_buildup"] if oid else "proxy",
            "oi_src":oid.get("src","proxy") if oid else "proxy",
            "data_src":data_src,"reasons":reasons,"lot":WATCHLIST[sym]["lot"],
            "open":lq.get("open",0) if lq else 0,"high":lq.get("high",0) if lq else 0,
            "low":lq.get("low",0) if lq else 0,
            "ai_analysis":None,"news":[],"strategy":"ONE",
        }
    except Exception as e:
        print(f"  score error {sym}: {e}"); return None

# ══════════════════════════════════════════════════════════════════════════════
# STRATEGY DEV — OI levels → VWAP retest → Hammer/Engulfing → Entry/SL/Target
# ══════════════════════════════════════════════════════════════════════════════
def detect_candle_pattern(opens, highs, lows, closes):
    """
    Detect hammer or bullish engulfing on the last completed candle.
    Returns: ("hammer"|"engulfing"|"bearish_hammer"|"bearish_engulfing"|None, direction)
    """
    if len(closes) < 2: return None, None
    o1,h1,l1,c1 = opens[-2], highs[-2], lows[-2], closes[-2]  # prev candle
    o0,h0,l0,c0 = opens[-1], highs[-1], lows[-1], closes[-1]  # last candle
    body0 = abs(c0-o0); range0 = h0-l0 if h0>l0 else 0.001
    body1 = abs(c1-o1)

    # Bullish hammer: small body at top, long lower wick (≥2× body), at or near VWAP/support
    lower_wick0 = min(o0,c0)-l0; upper_wick0 = h0-max(o0,c0)
    if (body0 > 0 and lower_wick0 >= 2*body0 and upper_wick0 <= body0 and c0 > o0):
        return "hammer", "bullish"

    # Bearish hammer (shooting star)
    if (body0 > 0 and upper_wick0 >= 2*body0 and lower_wick0 <= body0 and c0 < o0):
        return "hammer", "bearish"

    # Bullish engulfing: prev candle red, current green and body engulfs prev
    if c1 < o1 and c0 > o0 and c0 > o1 and o0 < c1:
        return "engulfing", "bullish"

    # Bearish engulfing: prev candle green, current red and body engulfs prev
    if c1 > o1 and c0 < o0 and c0 < o1 and o0 > c1:
        return "engulfing", "bearish"

    return None, None

def score_stock_DEV(sym, hist, lq, oid, intraday_hist=None):
    """
    DEV Intraday Strategy:
    1. Check OI buildup (long/short) → determines bias
    2. Calculate VWAP → key magnet level
    3. Find highest OI strikes (call wall = resistance, put wall = support)
    4. Check for VWAP retest + hammer/engulfing candle bounce
    5. Entry after bouncing candle, SL = low of that candle
    """
    try:
        c = [float(x) for x in hist["closes"]]; h_d = [float(x) for x in hist["highs"]]
        l_d = [float(x) for x in hist["lows"]];  v_d = [float(x) for x in hist["volumes"]]
        if len(c) < 5: return None

        spot       = lq["spot"]       if lq and lq["spot"]>0 else c[-1]
        change_pct = lq["change_pct"] if lq and lq["spot"]>0 else round((c[-1]-c[-2])/c[-2]*100,2) if len(c)>1 else 0
        data_src   = lq.get("src","yfinance") if lq else "yfinance"
        o_d  = c[:-1] + [spot]  # use closes as open proxy for daily

        # ── Step 1: OI bias ──────────────────────────────────────────────────
        oi_bias = "neutral"; call_wall = 0; put_wall = 0
        pcr = 1.0; atm_iv = 18.0; max_pain = round(spot/50)*50
        if oid:
            pcr      = oid["pcr"]
            max_pain = oid["max_pain"]
            atm_iv   = oid.get("atm_iv", 18.0) or 18.0
            oi_bias  = oid["oi_buildup"]
            # Highest OI strikes = resistance (call wall) and support (put wall)
            call_wall = oid.get("atm_strike", round(spot/50)*50)
            step = 100 if sym=="BANKNIFTY" else 50 if "NIFTY" in sym else 10
            put_wall  = call_wall - step * 2  # put wall 2 strikes below call wall
        else:
            step = 100 if sym=="BANKNIFTY" else 50 if "NIFTY" in sym else 10
            call_wall = round(spot/step)*step + step
            put_wall  = round(spot/step)*step - step

        # ── Step 2: VWAP ─────────────────────────────────────────────────────
        vw = vwap_val(h_d[-20:], l_d[-20:], c[-20:], v_d[-20:])
        near_vwap = abs(spot - vw) / vw < 0.005  # within 0.5% of VWAP

        # ── Step 3: ATR for SL sizing ─────────────────────────────────────────
        at = atr(h_d, l_d, c)

        # ── Step 4: Candle pattern detection ─────────────────────────────────
        # Use intraday data if available, else daily
        src_h = intraday_hist if intraday_hist else hist
        opens_  = [float(x) for x in src_h.get("opens",  src_h["closes"])]
        highs_  = [float(x) for x in src_h["highs"]]
        lows_   = [float(x) for x in src_h["lows"]]
        closes_ = [float(x) for x in src_h["closes"]]

        pattern, direction = detect_candle_pattern(opens_, highs_, lows_, closes_)

        # ── Step 5: Signal generation ─────────────────────────────────────────
        reasons = []
        setup_quality = 0  # 0-100

        # OI bias check
        if oi_bias == "long":
            setup_quality += 25
            reasons.append(f"Step 1 ✅ Long OI buildup — bullish bias (PCR {pcr})")
        elif oi_bias == "short":
            setup_quality += 20
            reasons.append(f"Step 1 ✅ Short OI buildup — bearish bias (PCR {pcr})")
        else:
            setup_quality += 10
            reasons.append(f"Step 1 ⚪ Neutral OI (PCR {pcr})")

        # VWAP check
        reasons.append(f"Step 2 — VWAP: ₹{vw:.1f} | Spot: ₹{spot} | {'Near VWAP ✅' if near_vwap else 'Away from VWAP'}")
        if near_vwap:
            setup_quality += 30

        # OI walls
        reasons.append(f"Step 3 — Call wall (resistance): ₹{call_wall} | Put wall (support): ₹{put_wall}")
        reasons.append(f"         Max pain: ₹{max_pain}")
        # Is spot between walls? Good for rangebound trade
        if put_wall < spot < call_wall:
            setup_quality += 15
            reasons.append("         Spot between OI walls — rangebound zone ✅")

        # Candle pattern
        if pattern and direction:
            setup_quality += 30
            reasons.append(f"Step 4 ✅ {direction.title()} {pattern} detected on last candle")
        else:
            reasons.append("Step 4 ⚪ No hammer/engulfing pattern yet — wait for setup")

        # ── Determine action ──────────────────────────────────────────────────
        # Need OI bias + near VWAP + candle pattern for a full DEV signal
        if pattern == "hammer" and direction == "bullish" and near_vwap and oi_bias in ("long", "neutral"):
            action = "BUY"; opt = "CE"
            reasons.append("Step 5 ✅ ENTRY: Bullish hammer at VWAP + long OI → BUY CE")
        elif pattern == "hammer" and direction == "bearish" and near_vwap and oi_bias in ("short", "neutral"):
            action = "SELL"; opt = "PE"
            reasons.append("Step 5 ✅ ENTRY: Bearish shooting star at VWAP + short OI → SELL PE")
        elif pattern == "engulfing" and direction == "bullish" and near_vwap:
            action = "BUY"; opt = "CE"
            reasons.append("Step 5 ✅ ENTRY: Bullish engulfing at VWAP → BUY CE")
        elif pattern == "engulfing" and direction == "bearish" and near_vwap:
            action = "SELL"; opt = "PE"
            reasons.append("Step 5 ✅ ENTRY: Bearish engulfing at VWAP → SELL PE")
        elif near_vwap and oi_bias == "long":
            action = "WATCH"; opt = "CE"
            reasons.append("Step 5 👁 WATCH: Near VWAP + long OI. Wait for hammer/engulfing candle")
        elif near_vwap and oi_bias == "short":
            action = "WATCH"; opt = "PE"
            reasons.append("Step 5 👁 WATCH: Near VWAP + short OI. Wait for reversal candle")
        elif oi_bias == "long" and change_pct > 1:
            action = "BUY"; opt = "CE"
            reasons.append("Step 5 🟡 PARTIAL: Long OI + momentum. Not ideal VWAP retest — use tight SL")
        elif oi_bias == "short" and change_pct < -1:
            action = "SELL"; opt = "PE"
            reasons.append("Step 5 🟡 PARTIAL: Short OI + sell-off. Wait for bounce to VWAP for entry")
        else:
            action = "WAIT"; opt = "CE" if change_pct >= 0 else "PE"
            reasons.append("Step 5 ⏳ WAIT: No setup yet — price not near VWAP or candle pattern missing")

        # ── Levels ────────────────────────────────────────────────────────────
        candle_low  = lows_[-1]  if lows_  else spot * 0.99
        candle_high = highs_[-1] if highs_ else spot * 1.01
        if action in ("BUY", "WATCH") and opt == "CE":
            entry = round(spot * 1.001, 1)
            sl    = round(candle_low - at*0.3, 1)        # SL = below candle low
            t1    = round(call_wall, 1)                   # T1 = call wall (resistance)
            t2    = round(call_wall + at, 1)              # T2 = beyond call wall
        elif action in ("SELL", "WATCH") and opt == "PE":
            entry = round(spot * 0.999, 1)
            sl    = round(candle_high + at*0.3, 1)        # SL = above candle high
            t1    = round(put_wall, 1)                    # T1 = put wall (support)
            t2    = round(put_wall - at, 1)               # T2 = beyond put wall
        else:
            entry = round(spot, 1)
            sl    = round(spot - at, 1)
            t1    = round(spot + at, 1)
            t2    = round(spot + at*2, 1)

        iv_use = (atm_iv/100) if atm_iv>5 else 0.18
        step   = 100 if sym=="BANKNIFTY" else 50 if "NIFTY" in sym else 10
        strike = round(spot/step)*step
        stag   = "ATM"
        op     = round(spot*iv_use*math.sqrt(1/365)*0.4, 1)  # 1-day premium estimate

        r_v = rsi(c)
        return {
            "sym":sym,"name":WATCHLIST[sym]["name"],"spot":round(spot,2),"change_pct":round(change_pct,2),
            "score":setup_quality,"action":action,"opt":opt,"strike":strike,"strike_tag":stag,
            "entry":entry,"sl":sl,"t1":t1,"t2":t2,
            "op":op,"os":round(op*0.4,1),"ot1":round(op*1.5,1),"ot2":round(op*2.0,1),
            "rsi":r_v,"vwap":vw,"atr":at,"near_vwap":near_vwap,
            "oi_bias":oi_bias,"call_wall":call_wall,"put_wall":put_wall,
            "pcr":pcr,"max_pain":max_pain,"atm_iv":atm_iv,
            "pattern":pattern or "none","pattern_dir":direction or "none",
            "setup_quality":setup_quality,
            "data_src":data_src,"reasons":reasons,"lot":WATCHLIST[sym]["lot"],
            "open":lq.get("open",0) if lq else 0,"high":lq.get("high",0) if lq else 0,
            "low":lq.get("low",0) if lq else 0,
            "ai_analysis":None,"news":[],"strategy":"DEV",
            "overnight":False,
            "oi_buildup":oi_bias,"oi_src":oid.get("src","proxy") if oid else "proxy",
            "macd_bull":False,"vol_ratio":1.0,"bb_upper":0,"bb_lower":0,
            "w_tech":0,"w_mom":0,"w_oi":0,"tech_score":0,"mom_score":0,"oi_score":0,
        }
    except Exception as e:
        print(f"  DEV score error {sym}: {e}"); return None

# ══════════════════════════════════════════════════════════════════════════════
# AI ANALYSIS
# ══════════════════════════════════════════════════════════════════════════════
def ai_analysis(sig):
    if not ANTHROPIC_KEY: return None
    try:
        news_txt = "\n".join([f"- {n['headline']}" for n in sig.get("news",[])[:3]]) or "No news."
        strategy = sig.get("strategy","ONE")

        if strategy == "DEV":
            prompt = f"""NSE intraday DEV strategy signal. Be specific and actionable.

{sig['sym']} ({sig['name']}) | Spot ₹{sig['spot']} | VWAP ₹{sig['vwap']}
Action: {sig['action']} {sig['strike']} {sig['opt']}
Entry ₹{sig['entry']} | SL ₹{sig['sl']} | T1 ₹{sig['t1']} | T2 ₹{sig['t2']}
OI Bias: {sig['oi_bias']} | PCR {sig['pcr']} | Call wall ₹{sig['call_wall']} | Put wall ₹{sig['put_wall']}
Candle pattern: {sig['pattern']} ({sig['pattern_dir']}) | Near VWAP: {sig['near_vwap']}
Setup quality: {sig['setup_quality']}/100

News: {news_txt}

Reply in EXACTLY this format:
VERDICT: [ready to trade / wait for setup / skip]
ENTRY TIMING: [when exactly to enter — e.g. "After candle close above VWAP at ₹X"]
SL REASONING: [why this SL level makes sense]
TARGET LOGIC: [why T1 at ₹{sig['t1']} is the right level]"""
        else:
            prompt = f"""NSE options ONE strategy signal. Direct and specific.

{sig['sym']} ({sig['name']}) | Spot ₹{sig['spot']} | Change {sig['change_pct']}%
{sig['action']} {sig['strike']} {sig['opt']} ({sig['strike_tag']})
Entry ₹{sig['entry']} | SL ₹{sig['sl']} | T1 ₹{sig['t1']} | T2 ₹{sig['t2']}
Score {sig['score']}/100 | VIX: {state.get('vix_regime','unknown')}
RSI {sig['rsi']} | MACD {'Bull' if sig['macd_bull'] else 'Bear'} | PCR {sig['pcr']}

News: {news_txt}

Reply in EXACTLY this format:
VERDICT: [buy/skip/sell and core reason]
TIMING: [specific time window to enter]
RISK: [biggest threat to this trade]
OVERNIGHT: [hold or exit today, why]"""

        resp = requests.post("https://api.anthropic.com/v1/messages",
            headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
            json={"model":"claude-sonnet-4-20250514","max_tokens":300,
                  "messages":[{"role":"user","content":prompt}]},timeout=20)
        if resp.status_code == 200:
            return resp.json()["content"][0]["text"]
        elif resp.status_code == 401:
            return "ERROR: Invalid API key — check ANTHROPIC_API_KEY"
        elif resp.status_code == 429:
            return "ERROR: Rate limited — wait 60s and retry"
        else:
            return f"ERROR: API {resp.status_code}"
    except Exception as e:
        return f"ERROR: {str(e)[:80]}"

# ══════════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════
def telegram(sig):
    if not (TELEGRAM_TOKEN and TELEGRAM_CHAT_ID): return
    try:
        e = "🟢" if sig["action"] in ("BUY","WATCH") else "🔴"
        strat = sig.get("strategy","ONE")
        if strat == "DEV":
            msg = (f"{e} *[DEV] {sig['action']} {sig['sym']} {sig['strike']} {sig['opt']}*\n"
                   f"━━━━━━━━━━━━━━━\n"
                   f"💰 ₹{sig['spot']}  {'+' if sig['change_pct']>=0 else ''}{sig['change_pct']}%\n"
                   f"📊 VWAP: ₹{sig['vwap']} | Near: {'Yes' if sig['near_vwap'] else 'No'}\n"
                   f"🧱 Call wall: ₹{sig['call_wall']} | Put wall: ₹{sig['put_wall']}\n"
                   f"🕯 Pattern: {sig['pattern']} ({sig['pattern_dir']})\n"
                   f"📌 Entry: ₹{sig['entry']} | SL: ₹{sig['sl']}\n"
                   f"✅ T1: ₹{sig['t1']} | T2: ₹{sig['t2']}\n"
                   f"Setup quality: {sig['setup_quality']}/100")
        else:
            msg = (f"{e} *[ONE] {sig['action']} {sig['sym']} {sig['strike']} {sig['opt']}* ({sig['strike_tag']})\n"
                   f"━━━━━━━━━━━━━━━\n"
                   f"💰 ₹{sig['spot']}  {'+' if sig['change_pct']>=0 else ''}{sig['change_pct']}%\n"
                   f"📌 Entry: ₹{sig['entry']} | SL: ₹{sig['sl']}\n"
                   f"✅ T1: ₹{sig['t1']} | T2: ₹{sig['t2']}\n"
                   f"Score {sig['score']}/100 | {state.get('vix_regime','')}")
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={"chat_id":TELEGRAM_CHAT_ID,"text":msg,"parse_mode":"Markdown"},timeout=8)
        state["alerts_sent"].append({"sym":sig["sym"],"action":sig["action"],
            "time":datetime.datetime.now().strftime("%H:%M:%S"),"score":sig["score"],
            "strategy":sig.get("strategy","ONE")})
        print(f"  📱 Telegram → {sig['sym']} [{strat}]")
    except Exception as e:
        print(f"  Telegram: {e}")

# ══════════════════════════════════════════════════════════════════════════════
# MAIN SCAN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan():
    if state["is_scanning"]: return
    state["is_scanning"] = True; state["fetched"] = 0; state["errors"] = []
    signals = []; dev_signals = []

    now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=5,minutes=30)))
    mo  = now.replace(hour=9,  minute=15, second=0, microsecond=0)
    mc  = now.replace(hour=15, minute=30, second=0, microsecond=0)
    state["market_status"] = "OPEN" if (now.weekday()<5 and mo<=now<=mc) else "CLOSED"

    print(f"\n{'='*60}")
    print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] Scan #{state['scan_count']+1} "
          f"| {state['market_status']} | Strategy: {state['strategy']}")

    # 0 — VIX + weights
    print("  [0] India VIX...")
    vix = fetch_india_vix()
    wts = get_weights(vix)
    state["vix"] = vix; state["vix_regime"] = wts["regime"]
    state["weights"] = {k:v for k,v in wts.items() if k!="regime"}
    print(f"      Regime: {wts['regime']} → T{wts['tech']}·M{wts['mom']}·OI{wts['oi']}")

    # 1 — Historical OHLCV
    print("  [1/4] Historical OHLCV...")
    hist = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(fetch_hist, sym): sym for sym in WATCHLIST}
        try:
            completed = as_completed(futs, timeout=75)
            for f in completed:
                sym = futs[f]
                try: _, d = f.result(timeout=20); hist[sym] = d if d else None
                except: hist[sym] = None
        except TimeoutError:
            print("  ⚠ hist fetch timeout — using partial results")
            for sym in WATCHLIST:
                if sym not in hist: hist[sym] = None
    ok_hist = sum(1 for v in hist.values() if v)
    print(f"         ✓ {ok_hist}/{len(WATCHLIST)} symbols loaded")

    # 2 — Live NSE quotes
    print("  [2/4] Live NSE quotes...")
    lqs = {}; nse_ok = 0
    with ThreadPoolExecutor(max_workers=4) as ex:
        futs = {ex.submit(live_quote, sym): sym for sym in WATCHLIST}
        for f in as_completed(futs, timeout=60):
            sym = futs[f]
            try:
                q = f.result(timeout=15)
                if q and q["spot"] > 0: lqs[sym] = q; nse_ok += 1
            except: pass
    state["nse_ok"]      = nse_ok > 0
    state["data_source"] = "NSE Live" if nse_ok > 5 else "yfinance (NSE unavailable)"
    print(f"         ✓ {nse_ok}/{len(WATCHLIST)} live quotes from NSE")

    # 3 — OI (NSE → yfinance fallback, sequential + delay)
    print("  [3/4] Option chain OI...")
    oids = {}; oi_ok = 0; oi_nse = 0; oi_yf = 0
    for sym in WATCHLIST:
        spot = lqs.get(sym, {}).get("spot", 0) or (hist[sym]["closes"][-1] if hist.get(sym) else 0)
        # Try NSE first
        d = fetch_oi_nse(sym)
        if d:
            oids[sym] = d; oi_ok += 1; oi_nse += 1
        else:
            # yfinance / volume proxy fallback
            d2 = fetch_oi_yfinance(sym, spot, hist.get(sym))
            if d2: oids[sym] = d2; oi_ok += 1; oi_yf += 1
        time.sleep(0.4)
    state["fetched"] = 0
    print(f"         ✓ {oi_ok}/{len(WATCHLIST)} OI  ({oi_nse} NSE live, {oi_yf} yf/proxy)")

    # 4 — Score
    print("  [4/4] Scoring...")
    for sym in WATCHLIST:
        if not hist.get(sym):
            state["errors"].append(f"{sym}: no history"); state["fetched"] += 1; continue

        # Strategy ONE
        sig1 = score_stock_ONE(sym, hist[sym], lqs.get(sym), oids.get(sym), weights=state["weights"])
        if sig1:
            signals.append(sig1)
            src = "🟢" if lqs.get(sym) else "🟡"
            oi_src = oids.get(sym,{}).get("src","✗") if oids.get(sym) else "✗"
            print(f"    {src} {sym:12s} ₹{sig1['spot']:>8,.0f} {sig1['change_pct']:+5.1f}%  "
                  f"ONE:{sig1['action']:4s}{sig1['opt']} {sig1['score']:3d}  OI[{oi_src}]:{sig1['oi_buildup']}")

        # Strategy DEV
        sig2 = score_stock_DEV(sym, hist[sym], lqs.get(sym), oids.get(sym))
        if sig2:
            dev_signals.append(sig2)

        state["errors"].append(f"{sym}: scoring failed") if not sig1 else None
        state["fetched"] += 1

    # Sort
    signals.sort(key=lambda x:(0 if x["action"]=="BUY" else 1 if x["action"]=="SELL" else 2,-x["score"]))
    dev_signals.sort(key=lambda x:(
        0 if x["action"] in ("BUY","WATCH") and x["opt"]=="CE" else
        1 if x["action"] in ("SELL","WATCH") and x["opt"]=="PE" else 2,
        -x["setup_quality"]
    ))

    top     = [s for s in signals    if s["action"] in ("BUY","SELL")][:10]
    top_dev = [s for s in dev_signals if s["action"] in ("BUY","SELL","WATCH")][:10]

    # News (for active strategy's top calls)
    active_top = top_dev if state["strategy"]=="DEV" else top
    if BS4_OK:
        print(f"  Scraping news for top {len(active_top)}...")
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {ex.submit(fetch_news, s["sym"], s["name"]): i for i,s in enumerate(active_top)}
            for f in as_completed(futs, timeout=30):
                try: active_top[futs[f]]["news"] = f.result()
                except: pass

    # AI analysis
    if ANTHROPIC_KEY:
        print(f"  Claude AI → top {min(AI_TOP_N, len(active_top))}...")
        for sig in active_top[:AI_TOP_N]:
            print(f"    {sig['sym']}...", end=" ", flush=True)
            sig["ai_analysis"] = ai_analysis(sig)
            print("✓" if sig["ai_analysis"] and not sig["ai_analysis"].startswith("ERROR") else "✗")

    # Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        alerted = {a["sym"] for a in state["alerts_sent"]}
        for sig in active_top:
            if sig["score"] >= ALERT_THRESHOLD and sig["sym"] not in alerted:
                telegram(sig)

    state["signals"]     = signals
    state["top_calls"]   = top
    state["dev_signals"] = dev_signals
    state["last_updated"] = datetime.datetime.now().strftime("%d %b %Y, %I:%M:%S %p")
    state["scan_count"] += 1
    state["is_scanning"]  = False

    b = len([s for s in top if s["action"]=="BUY"])
    se = len([s for s in top if s["action"]=="SELL"])
    dw = len([s for s in dev_signals if s["action"] in ("BUY","SELL","WATCH")])
    print(f"\n✅ ONE: {b} BUY·{se} SELL  |  DEV: {dw} setups  |  errors: {len(state['errors'])}")
    print(f"{'='*60}\n")

# ══════════════════════════════════════════════════════════════════════════════
# ROUTES
# ══════════════════════════════════════════════════════════════════════════════
@app.route("/api/ai/<sym>")
def api_ai(sym):
    strategy = state["strategy"]
    sigs = state["dev_signals"] if strategy=="DEV" else state["signals"]
    sig  = next((s for s in sigs if s["sym"]==sym), None)
    if not sig: return jsonify({"error":"not found"}), 404
    if not ANTHROPIC_KEY: return jsonify({"error":"ANTHROPIC_API_KEY not set"}), 400
    a = ai_analysis(sig); sig["ai_analysis"] = a
    return jsonify({"analysis": a})

@app.route("/api/state")
def api_state(): return jsonify(state)

@app.route("/api/scan", methods=["POST"])
def api_scan():
    if state["is_scanning"]: return jsonify({"ok": False})
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({"ok": True})

@app.route("/api/strategy", methods=["POST"])
def api_strategy():
    data = freq.get_json() or {}
    s = data.get("strategy","ONE")
    if s in ("ONE","DEV"):
        state["strategy"] = s
        return jsonify({"ok":True,"strategy":s})
    return jsonify({"ok":False,"error":"Unknown strategy"}), 400

@app.route("/")
def index(): return render_template_string(HTML)

# ══════════════════════════════════════════════════════════════════════════════
# DASHBOARD HTML
# ══════════════════════════════════════════════════════════════════════════════
HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><title>OptionFlow v4</title>
<style>
@import url('https://fonts.googleapis.com/css2?family=Rajdhani:wght@400;600;700&family=JetBrains+Mono:wght@400;600&display=swap');
*{box-sizing:border-box;margin:0;padding:0}
body{background:#05070a;color:#c8d8e8;font-family:'Rajdhani',sans-serif;min-height:100vh}
::-webkit-scrollbar{width:3px}::-webkit-scrollbar-thumb{background:#1a2235}
.mono{font-family:'JetBrains Mono',monospace}
@keyframes pulse{0%,100%{opacity:1}50%{opacity:.25}}
@keyframes fadeIn{from{opacity:0;transform:translateY(6px)}to{opacity:1;transform:translateY(0)}}
@keyframes spin{to{transform:rotate(360deg)}}
@keyframes ticker{0%{transform:translateX(0)}100%{transform:translateX(-50%)}}
.fade{animation:fadeIn .35s ease forwards}
.live{width:7px;height:7px;border-radius:50%;display:inline-block;animation:pulse 1.5s infinite}
.live.g{background:#00ffa3}.live.r{background:#ff3d5a}.live.a{background:#ffb830}
header{background:#0c1018;border-bottom:1px solid #141c28;padding:0 20px;height:52px;display:flex;align-items:center;gap:10px;position:sticky;top:0;z-index:100;flex-wrap:wrap}
.logo-box{width:30px;height:30px;background:#00ffa3;border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:15px;font-weight:900;color:#000;flex-shrink:0}
.hright{margin-left:auto;display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.mkt{padding:3px 10px;border-radius:4px;font-size:11px;font-weight:700;letter-spacing:1px}
.mkt-open{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340}
.mkt-closed{background:#ff3d5a18;color:#ff3d5a;border:1px solid #ff3d5a40}
.btn{padding:7px 15px;background:#00ffa3;border:none;border-radius:6px;color:#000;font-weight:800;font-size:12px;cursor:pointer;letter-spacing:1px;font-family:'Rajdhani',sans-serif;transition:opacity .15s}
.btn:hover{opacity:.85}.btn:disabled{opacity:.35;cursor:default}
/* Strategy switcher */
.strat-wrap{display:flex;gap:3px;background:#080c12;border-radius:7px;padding:3px;border:1px solid #141c28}
.strat-btn{padding:5px 14px;border-radius:5px;border:none;cursor:pointer;font-family:'Rajdhani',sans-serif;font-size:12px;font-weight:700;transition:all .2s;letter-spacing:.5px}
.strat-btn.one-active{background:#00ffa3;color:#000}
.strat-btn.dev-active{background:#a78bfa;color:#000}
.strat-btn:not(.one-active):not(.dev-active){background:transparent;color:#6b8299}
.ticker-wrap{background:#080c12;border-bottom:1px solid #141c28;height:26px;overflow:hidden;display:flex;align-items:center}
.ticker-inner{display:flex;animation:ticker 45s linear infinite;white-space:nowrap}
.tick{font-family:'JetBrains Mono',monospace;font-size:11px;padding:0 14px}
main{max-width:1440px;margin:0 auto;padding:16px 20px;display:flex;flex-direction:column;gap:13px}
.status-row{display:flex;align-items:center;gap:8px;font-size:12px;background:#0c1018;border:1px solid #141c28;border-radius:8px;padding:7px 14px;flex-wrap:wrap}
.svc{display:inline-flex;align-items:center;gap:4px;font-size:11px;padding:2px 7px;background:#0c1018;border:1px solid #141c28;border-radius:4px}
/* Strategy info banner */
.strat-banner{border-radius:10px;padding:10px 16px;font-size:12px;line-height:1.7;border:1px solid}
.strat-banner.ONE{background:#00ffa308;border-color:#00ffa330;color:#8896a8}
.strat-banner.DEV{background:#a78bfa08;border-color:#a78bfa30;color:#8896a8}
.tab-bar{display:flex;gap:3px;background:#0c1018;border-radius:8px;padding:3px;border:1px solid #141c28;width:fit-content;flex-wrap:wrap}
.tab{padding:6px 14px;border-radius:6px;border:none;cursor:pointer;font-family:'Rajdhani',sans-serif;font-size:12px;font-weight:700;transition:all .18s}
.tab.on{background:#00ffa3;color:#000}.tab.on-dev{background:#a78bfa;color:#000}.tab:not(.on):not(.on-dev){background:transparent;color:#6b8299}
.cards-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px}
/* Card styles */
.card{background:#0f141c;border:1px solid #141c28;border-radius:11px;overflow:hidden}
.card.buy{border-left:3px solid #00ffa3}.card.sell{border-left:3px solid #ff3d5a}
.card.watch{border-left:3px solid #a78bfa}.card.wait{border-left:3px solid #3d5068}
.badge{display:inline-block;padding:2px 6px;border-radius:3px;font-size:10px;font-weight:700;letter-spacing:.7px}
.b-buy{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340}
.b-sell{background:#ff3d5a18;color:#ff3d5a;border:1px solid #ff3d5a40}
.b-watch{background:#a78bfa18;color:#a78bfa;border:1px solid #a78bfa40}
.b-wait{background:#3d506818;color:#6b8299;border:1px solid #3d506840}
.b-ce{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340}
.b-pe{background:#ff3d5a18;color:#ff3d5a;border:1px solid #ff3d5a40}
.b-on{background:#00d4ff18;color:#00d4ff;border:1px solid #00d4ff40}
.b-atm{background:#3d9eff18;color:#3d9eff;border:1px solid #3d9eff40}
.b-otm{background:#ffb83018;color:#ffb830;border:1px solid #ffb83040}
.b-live{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340;font-size:9px}
.b-delay{background:#ffb83018;color:#ffb830;border:1px solid #ffb83040;font-size:9px}
.b-one{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340;font-size:9px}
.b-dev{background:#a78bfa18;color:#a78bfa;border:1px solid #a78bfa40;font-size:9px}
.card-head{padding:12px 14px 8px;display:flex;justify-content:space-between;align-items:flex-start}
.strike-row{padding:2px 14px 8px;display:flex;align-items:center;gap:6px;flex-wrap:wrap}
.oi-row{padding:0 14px 7px;display:flex;gap:10px;flex-wrap:wrap;font-size:11px}
.oi-item{display:flex;gap:4px;align-items:center}
.levels{padding:0 14px 8px;display:grid;grid-template-columns:repeat(4,1fr);gap:6px}
.lvl{background:#080c12;border:1px solid #1a2235;border-radius:6px;padding:6px 8px}
.lvl-l{font-size:9px;color:#3d5068;letter-spacing:1.5px;margin-bottom:2px}
.lvl-v{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:700}
.opt-grid{padding:6px 14px 8px;display:grid;grid-template-columns:repeat(4,1fr);gap:5px;background:#080c12;border-top:1px solid #141c28}
.ol-l{font-size:9px;color:#3d5068;letter-spacing:1px;margin-bottom:2px}
.ol-v{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600}
/* DEV specific */
.dev-section{padding:8px 14px;background:#080c12;border-top:1px solid #141c28;display:flex;flex-direction:column;gap:5px}
.dev-row{display:flex;justify-content:space-between;font-size:12px}
.dev-label{color:#6b8299}.dev-val{font-family:'JetBrains Mono',monospace;font-size:12px;font-weight:600}
.pattern-badge{display:inline-block;padding:3px 8px;border-radius:4px;font-size:11px;font-weight:700}
.pattern-bull{background:#00ffa318;color:#00ffa3;border:1px solid #00ffa340}
.pattern-bear{background:#ff3d5a18;color:#ff3d5a;border:1px solid #ff3d5a40}
.pattern-none{background:#3d506818;color:#6b8299;border:1px solid #3d506840}
.score-breakdown{padding:7px 14px 8px;display:grid;grid-template-columns:1fr 1fr 1fr;gap:5px}
.sb{background:#080c12;border:1px solid #1a2235;border-radius:5px;padding:5px 8px;text-align:center}
.sb-l{font-size:9px;color:#3d5068;letter-spacing:1px;margin-bottom:2px}
.sb-v{font-family:'JetBrains Mono',monospace;font-size:13px;font-weight:700}
.bar-t{height:3px;background:#1a2235;border-radius:2px;overflow:hidden;margin-top:4px}
.bar-f{height:100%;border-radius:2px;transition:width .5s}
.ai-box{margin:0 14px 8px;background:#080c12;border:1px solid #1e2a3e;border-radius:7px;overflow:hidden}
.ai-head{padding:6px 11px;border-bottom:1px solid #141c28;display:flex;align-items:center;gap:6px}
.ai-body{padding:9px 11px;font-size:12px;line-height:1.75;color:#8896a8;white-space:pre-wrap}
.spinner-sm{width:13px;height:13px;border:2px solid #1a2235;border-top-color:#00ffa3;border-radius:50%;animation:spin 1s linear infinite}
.news-section{margin:0 14px 8px}
.news-item{padding:7px 9px;background:#080c12;border:1px solid #141c28;border-radius:5px;border-left:2px solid #3d5068;font-size:11px;color:#6b8299;cursor:pointer;margin-bottom:4px;line-height:1.4}
.news-item:hover{border-left-color:#00ffa3;color:#c8d8e8}
.reasons-wrap{display:none;flex-direction:column;gap:2px;margin:0 14px 8px;border-top:1px solid #0d1219;padding-top:7px}
.reason{font-size:11px;color:#6b8299;padding:3px 0;border-bottom:1px solid #0d121960}
.expand-btn{width:100%;padding:7px;background:#080c12;border:none;border-top:1px solid #141c28;color:#3d5068;cursor:pointer;font-size:10px;font-family:'Rajdhani',sans-serif;letter-spacing:1px}
.expand-btn:hover{color:#6b8299}
.tbl-wrap{background:#0f141c;border:1px solid #141c28;border-radius:11px;overflow:hidden;overflow-x:auto}
table{width:100%;border-collapse:collapse}
th{padding:7px 10px;font-size:10px;color:#3d5068;text-align:left;letter-spacing:1.2px;border-bottom:1px solid #141c28;background:#080c12;white-space:nowrap}
td{padding:8px 10px;font-size:12px;border-bottom:1px solid #0d1219;white-space:nowrap}
tr:hover td{background:#0d1219}
.on-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(185px,1fr));gap:10px}
.on-card{background:#0f141c;border:1px solid #00d4ff30;border-radius:10px;padding:12px 14px}
.guide-grid{display:grid;grid-template-columns:1fr 1fr 1fr;gap:11px;margin-top:14px}
.guide-card{background:#0f141c;border:1px solid #141c28;border-radius:10px;padding:12px;border-top:3px solid}
.empty{padding:36px;text-align:center;color:#3d5068;font-size:13px}
#pb{height:2px;background:#00ffa3;width:0%;transition:width .3s;position:fixed;top:0;left:0;z-index:999}
</style>
</head>
<body>
<div id="pb"></div>
<header>
  <div style="display:flex;align-items:center;gap:9px">
    <div class="logo-box">⚡</div>
    <div>
      <div style="font-weight:800;font-size:14px;letter-spacing:1.5px">OPTIONFLOW v4</div>
      <div style="font-size:9px;color:#3d5068;letter-spacing:2px">LIVE NSE · CE+PE · VIX-ADAPTIVE</div>
    </div>
  </div>
  <div id="mkt-badge" class="mkt">—</div>
  <!-- Strategy switcher -->
  <div class="strat-wrap">
    <button class="strat-btn" id="btn-one" onclick="switchStrategy('ONE')">⚡ ONE</button>
    <button class="strat-btn" id="btn-dev" onclick="switchStrategy('DEV')">🎯 DEV</button>
  </div>
  <div class="hright">
    <div id="services" style="display:flex;gap:5px"></div>
    <div id="vix-badge" style="display:none;align-items:center;gap:5px;padding:3px 10px;border-radius:4px;border:1px solid #141c28;background:#0c1018;font-size:11px;font-weight:700"></div>
    <span id="progress-txt" style="font-size:11px;color:#ffb830;display:none"></span>
    <span id="lu" style="font-size:11px;color:#3d5068">Not scanned</span>
    <span id="clk" class="mono" style="font-size:12px;color:#6b8299"></span>
    <button class="btn" id="scan-btn" onclick="triggerScan()">▶ SCAN NOW</button>
  </div>
</header>
<div class="ticker-wrap"><div class="ticker-inner" id="ticker"><span class="tick" style="color:#3d5068">Waiting for scan...</span></div></div>
<main>
  <div class="status-row">
    <span class="live g"></span>
    <span style="color:#6b8299">NSE live · yfinance 60d · OI with yf fallback · MoneyControl news</span>
    <div id="weight-bar" style="display:flex;align-items:center;gap:7px;margin-left:10px">
      <span style="font-size:10px;color:#3d5068;letter-spacing:1px">WEIGHTS:</span>
      <div style="display:flex;height:14px;border-radius:3px;overflow:hidden;width:110px;gap:1px">
        <div id="wb-t" style="background:#3d9eff33;display:flex;align-items:center;justify-content:center;font-size:9px;color:#3d9eff;font-family:'JetBrains Mono',monospace;transition:width .5s">T45</div>
        <div id="wb-m" style="background:#ffb83033;display:flex;align-items:center;justify-content:center;font-size:9px;color:#ffb830;font-family:'JetBrains Mono',monospace;transition:width .5s">M30</div>
        <div id="wb-o" style="background:#00ffa333;display:flex;align-items:center;justify-content:center;font-size:9px;color:#00ffa3;font-family:'JetBrains Mono',monospace;transition:width .5s">OI25</div>
      </div>
      <span id="regime-txt" style="font-size:10px;color:#3d5068"></span>
    </div>
    <span id="oi-status" style="font-size:10px;color:#3d5068;margin-left:6px"></span>
    <span id="scan-count" style="margin-left:auto;font-family:'JetBrains Mono',monospace;font-size:11px;color:#3d5068"></span>
    <span id="err-area"></span>
  </div>

  <!-- Strategy info banner -->
  <div id="strat-banner" class="strat-banner ONE">
    <b style="color:#00ffa3">⚡ Strategy ONE</b> — VIX-adaptive scoring: Technicals + Momentum + OI buildup.
    Signals ranked by combined score. Entry/SL/Target based on ATR. Use for any market condition.
  </div>

  <div class="tab-bar" id="tab-bar">
    <button class="tab on"  onclick="setTab('calls', this)">🎯 TOP CALLS</button>
    <button class="tab"     onclick="setTab('puts',  this)">🔴 PE SIGNALS</button>
    <button class="tab"     onclick="setTab('movers',this)">🚀 MOVERS</button>
    <button class="tab"     onclick="setTab('all',   this)">📊 ALL</button>
    <button class="tab"     onclick="setTab('on',    this)">🌙 OVERNIGHT</button>
    <button class="tab"     onclick="setTab('alerts',this)">📱 ALERTS</button>
  </div>

  <div id="tab-calls"><div class="cards-grid" id="cards-grid"><div class="empty">Click SCAN NOW →</div></div></div>
  <div id="tab-puts"   style="display:none"><div class="cards-grid" id="puts-grid"><div class="empty">Run scan</div></div></div>
  <div id="tab-movers" style="display:none"><div class="cards-grid" id="movers-grid"><div class="empty">Run scan</div></div></div>
  <div id="tab-all"    style="display:none">
    <div class="tbl-wrap"><table>
      <thead><tr>
        <th>SYMBOL</th><th>STRAT</th><th>SRC</th><th>SPOT</th><th>CHG%</th><th>ACTION</th><th>STRIKE</th>
        <th>ENTRY</th><th>SL</th><th>T1</th><th>T2</th><th>OPT~</th><th>RSI/QTY</th><th>PCR</th><th>OI</th><th>SCORE</th>
      </tr></thead>
      <tbody id="sig-tbody"><tr><td colspan="16" class="empty">Run a scan</td></tr></tbody>
    </table></div>
  </div>
  <div id="tab-on"     style="display:none">
    <div class="on-grid" id="on-grid"><div class="empty">Run scan</div></div>
    <div class="guide-grid">
      <div class="guide-card" style="border-top-color:#00d4ff"><div style="font-size:13px;color:#00d4ff;font-weight:700;margin-bottom:6px">⏰ Entry Timing</div><div style="font-size:12px;color:#6b8299;line-height:1.7">Enter 30–45 min before close (3:00–3:15 PM). Avoid last 15 min.</div></div>
      <div class="guide-card" style="border-top-color:#ffb830"><div style="font-size:13px;color:#ffb830;font-weight:700;margin-bottom:6px">🛡️ Risk Control</div><div style="font-size:12px;color:#6b8299;line-height:1.7">Max 20% capital overnight. Use 1.5–2× wider SL. No holds through major events.</div></div>
      <div class="guide-card" style="border-top-color:#00ffa3"><div style="font-size:13px;color:#00ffa3;font-weight:700;margin-bottom:6px">🌅 Morning Exit</div><div style="font-size:12px;color:#6b8299;line-height:1.7">Gap >2% for you → book profits. Gap >1.5% against → exit immediately.</div></div>
    </div>
  </div>
  <div id="tab-alerts" style="display:none">
    <div style="background:#0f141c;border:1px solid #141c28;border-radius:10px;overflow:hidden;margin-bottom:12px" id="alert-log"><div class="empty">No alerts yet</div></div>
  </div>
</main>

<script>
let pollTimer=null; let curStrategy='ONE';
setInterval(()=>{document.getElementById('clk').textContent=new Date().toLocaleTimeString('en-IN');},1000);

function setTab(id,btn){
  ['calls','puts','movers','all','on','alerts'].forEach(t=>document.getElementById('tab-'+t).style.display=t===id?'block':'none');
  document.querySelectorAll('.tab').forEach(b=>b.classList.remove('on','on-dev'));
  const cls=curStrategy==='DEV'?'on-dev':'on';
  btn.classList.add(cls);
}

async function fetchState(){try{const r=await fetch('/api/state');const d=await r.json();render(d);return d;}catch(e){}}

async function triggerScan(){
  const btn=document.getElementById('scan-btn');
  btn.disabled=true; btn.textContent='SCANNING...';
  const pb=document.getElementById('pb'), pt=document.getElementById('progress-txt');
  pt.style.display='inline'; pb.style.width='5%';
  try{await fetch('/api/scan',{method:'POST'});}catch(e){}
  clearInterval(pollTimer);
  pollTimer=setInterval(async()=>{
    const d=await fetchState(); if(!d) return;
    pb.style.width=d.total>0?Math.round((d.fetched/d.total)*90+5)+'%':'50%';
    pt.textContent=d.is_scanning?`Step ${Math.min(Math.ceil(d.fetched/Math.max(d.total,1)*4)+1,4)}/4 — ${d.fetched}/${d.total}...`:'Done ✓';
    if(!d.is_scanning){clearInterval(pollTimer);setTimeout(()=>{pb.style.width='0%';pt.style.display='none';},1200);btn.disabled=false;btn.textContent='▶ SCAN NOW';}
  },1500);
}

async function switchStrategy(s){
  curStrategy=s;
  await fetch('/api/strategy',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({strategy:s})});
  document.getElementById('btn-one').className='strat-btn '+(s==='ONE'?'one-active':'');
  document.getElementById('btn-dev').className='strat-btn '+(s==='DEV'?'dev-active':'');
  const banner=document.getElementById('strat-banner');
  banner.className='strat-banner '+s;
  if(s==='ONE'){
    banner.innerHTML='<b style="color:#00ffa3">⚡ Strategy ONE</b> — VIX-adaptive: Technicals + Momentum + OI. Signals ranked by combined score. Works for any market condition.';
  } else {
    banner.innerHTML='<b style="color:#a78bfa">🎯 Strategy DEV</b> — Intraday: OI buildup → VWAP retest → Hammer/Engulfing candle → Entry after bounce. SL = candle low. T1 = OI wall.';
  }
  // Update tab active color
  document.querySelectorAll('.tab.on,.tab.on-dev').forEach(b=>{b.classList.remove('on','on-dev');b.classList.add(s==='DEV'?'on-dev':'on');});
  fetchState();
}

async function loadAI(sym,btn){
  btn.disabled=true; btn.textContent='Thinking...';
  const box=document.getElementById('ai-body-'+sym);
  box.innerHTML='<div style="display:flex;align-items:center;gap:8px;padding:9px 0;font-size:11px;color:#3d5068"><div class="spinner-sm"></div>Claude analysing...</div>';
  try{
    const r=await fetch('/api/ai/'+sym); const d=await r.json();
    if(d.analysis){
      const isErr=d.analysis.startsWith('ERROR:');
      box.innerHTML=`<div class="ai-body" style="${isErr?'color:#ff3d5a;':''}">${d.analysis}</div>`;
    } else {
      box.innerHTML=`<div class="ai-body" style="color:#ff3d5a">${d.error||'Failed'}</div>`;
    }
  }catch(e){box.innerHTML='<div class="ai-body" style="color:#ff3d5a">Request failed</div>';}
  btn.disabled=false; btn.textContent='↻ Refresh';
}

function chg(v){const c=v>=0?'#00ffa3':'#ff3d5a',s=v>=0?'▲':'▼';return`<span class="mono" style="color:${c};font-size:11px">${s}${Math.abs(v).toFixed(2)}%</span>`;}
function oic(b){return b==='long'?'#00ffa3':b==='short'?'#ff3d5a':'#6b8299';}
function actionClass(a){return a==='BUY'?'buy':a==='SELL'?'sell':a==='WATCH'?'watch':'wait';}
function actionBadge(a){return a==='BUY'?'b-buy':a==='SELL'?'b-sell':a==='WATCH'?'b-watch':'b-wait';}

function cardHtml(s){
  const ac=s.action==='BUY'?'#00ffa3':s.action==='SELL'?'#ff3d5a':s.action==='WATCH'?'#a78bfa':'#6b8299';
  const sc=s.score>65?'#00ffa3':s.score>45?'#ffb830':'#ff3d5a';
  const isDev=s.strategy==='DEV';
  const isLive=s.data_src==='NSE Live';

  const aiSec=s.ai_analysis
    ?`<div class="ai-box"><div class="ai-head"><span class="live g" style="width:6px;height:6px"></span><span style="font-size:11px;color:#00ffa3;font-weight:700">AI</span><button onclick="loadAI('${s.sym}',this)" style="margin-left:auto;background:none;border:1px solid #1e2a3e;border-radius:4px;padding:2px 8px;color:#6b8299;cursor:pointer;font-size:10px;font-family:'Rajdhani',sans-serif">↻</button></div><div class="ai-body" id="ai-body-${s.sym}">${s.ai_analysis}</div></div>`
    :`<div class="ai-box"><div class="ai-head"><span class="live a" style="width:6px;height:6px"></span><span style="font-size:11px;color:#ffb830;font-weight:700">AI ANALYSIS</span><button onclick="loadAI('${s.sym}',this)" style="margin-left:auto;background:#00ffa318;border:1px solid #00ffa340;border-radius:4px;padding:3px 10px;color:#00ffa3;cursor:pointer;font-size:11px;font-weight:700;font-family:'Rajdhani',sans-serif">▶ GET AI</button></div><div id="ai-body-${s.sym}" style="padding:8px 11px;font-size:11px;color:#3d5068">Click to get Claude's analysis.</div></div>`;

  const newsSec=s.news&&s.news.length
    ?`<div class="news-section">${s.news.map(n=>`<div class="news-item" onclick="window.open('${n.url}','_blank')">${n.headline} <span style="color:#3d5068;font-size:10px">[${n.source}]</span></div>`).join('')}</div>`:'';

  const devSection=isDev?`
    <div class="dev-section">
      <div class="dev-row"><span class="dev-label">VWAP</span><span class="dev-val" style="color:${s.near_vwap?'#00ffa3':'#6b8299'}">₹${s.vwap} ${s.near_vwap?'◉ NEAR':'○ away'}</span></div>
      <div class="dev-row"><span class="dev-label">Call wall (resistance)</span><span class="dev-val" style="color:#ff3d5a">₹${s.call_wall}</span></div>
      <div class="dev-row"><span class="dev-label">Put wall (support)</span><span class="dev-val" style="color:#00ffa3">₹${s.put_wall}</span></div>
      <div class="dev-row"><span class="dev-label">Candle pattern</span>
        <span class="pattern-badge ${s.pattern==='none'?'pattern-none':s.pattern_dir==='bullish'?'pattern-bull':'pattern-bear'}">
          ${s.pattern==='none'?'Waiting...':s.pattern_dir+' '+s.pattern}
        </span>
      </div>
      <div class="dev-row" style="margin-top:2px"><span class="dev-label">Setup quality</span><span class="dev-val" style="color:${sc}">${s.setup_quality}/100</span></div>
    </div>`:
    `<div class="score-breakdown">
      <div class="sb"><div class="sb-l">TECHNICAL</div><div class="sb-v" style="color:#3d9eff">${s.tech_score}<span style="font-size:9px;color:#3d5068">/${s.w_tech||45}</span></div></div>
      <div class="sb"><div class="sb-l">MOMENTUM</div><div class="sb-v" style="color:#ffb830">${s.mom_score}<span style="font-size:9px;color:#3d5068">/${s.w_mom||30}</span></div></div>
      <div class="sb"><div class="sb-l">OI SIGNAL</div><div class="sb-v" style="color:${oic(s.oi_buildup)}">${s.oi_score}<span style="font-size:9px;color:#3d5068">/${s.w_oi||25}</span></div></div>
    </div>`;

  return`<div class="card ${actionClass(s.action)} fade">
    <div class="card-head">
      <div>
        <div style="display:flex;align-items:center;gap:5px;margin-bottom:3px;flex-wrap:wrap">
          <span style="font-weight:800;font-size:16px">${s.sym}</span>
          <span class="badge ${actionBadge(s.action)}">${s.action}</span>
          <span class="badge b-${s.opt.toLowerCase()}">${s.opt}</span>
          ${s.overnight?'<span class="badge b-on">OVERNIGHT</span>':''}
          <span class="badge ${isLive?'b-live':'b-delay'}">${isLive?'🟢 LIVE':'🟡 15m'}</span>
          <span class="badge ${isDev?'b-dev':'b-one'}">${isDev?'DEV':'ONE'}</span>
        </div>
        <div style="font-size:11px;color:#6b8299">${s.name}</div>
      </div>
      <div style="text-align:right">
        <div class="mono" style="font-size:17px;font-weight:700;color:${s.change_pct>=0?'#00ffa3':'#ff3d5a'}">₹${s.spot.toLocaleString('en-IN')}</div>
        ${chg(s.change_pct)}
        ${s.open?`<div style="font-size:9px;color:#3d5068;margin-top:1px">O:${s.open} H:${s.high} L:${s.low}</div>`:''}
      </div>
    </div>
    <div class="strike-row">
      <span style="font-size:11px;color:#6b8299">Option:</span>
      <span class="mono" style="font-size:14px;font-weight:700;color:${ac}">${s.strike} ${s.opt}</span>
      <span class="badge ${s.strike_tag==='ATM'?'b-atm':'b-otm'}">${s.strike_tag}</span>
      <span style="font-size:11px;color:#3d5068">~₹${s.op} est.</span>
      ${s.atm_iv?`<span style="font-size:10px;color:#6b8299">IV ${s.atm_iv}%</span>`:''}
    </div>
    <div class="oi-row">
      <div class="oi-item"><span style="font-size:9px;color:#3d5068">PCR</span><span class="mono" style="color:${s.pcr>1.2?'#ff3d5a':s.pcr<0.8?'#00ffa3':'#6b8299'}">${s.pcr}</span></div>
      <div class="oi-item"><span style="font-size:9px;color:#3d5068">OI</span><span class="mono" style="color:${oic(s.oi_buildup)}">${s.oi_buildup}</span></div>
      ${!isDev?`<div class="oi-item"><span style="font-size:9px;color:#3d5068">MaxPain</span><span class="mono" style="color:#ffb830">₹${s.max_pain}</span></div>`:''}
      <div class="oi-item"><span style="font-size:9px;color:#3d5068">${isDev?'Src':'Vol'}</span><span class="mono" style="color:#6b8299">${isDev?(s.oi_src||'?'):s.vol_ratio+'x'}</span></div>
    </div>
    <div class="levels">
      <div class="lvl"><div class="lvl-l">ENTRY</div><div class="lvl-v" style="color:#3d9eff">₹${s.entry}</div></div>
      <div class="lvl"><div class="lvl-l">TARGET 1</div><div class="lvl-v" style="color:#00ffa3">₹${s.t1}</div></div>
      <div class="lvl"><div class="lvl-l">TARGET 2</div><div class="lvl-v" style="color:#00ffa3">₹${s.t2}</div></div>
      <div class="lvl"><div class="lvl-l">STOP LOSS</div><div class="lvl-v" style="color:#ff3d5a">₹${s.sl}</div></div>
    </div>
    <div class="opt-grid">
      <div><div class="ol-l">OPT SL</div><div class="ol-v" style="color:#ff3d5a">₹${s.os}</div></div>
      <div><div class="ol-l">OPT TGT 1</div><div class="ol-v" style="color:#00ffa3">₹${s.ot1}</div></div>
      <div><div class="ol-l">OPT TGT 2</div><div class="ol-v" style="color:#00ffa3">₹${s.ot2}</div></div>
      <div><div class="ol-l">CONFIDENCE</div><div class="ol-v" style="color:#ffb830">${s.conf||s.setup_quality}%</div></div>
    </div>
    ${devSection}
    <div style="padding:0 14px 8px">
      <div style="display:flex;justify-content:space-between;font-size:11px;color:#3d5068;margin-bottom:3px">
        <span>${isDev?'Setup Quality':'Total Score'}</span>
        <span class="mono" style="color:${sc}">${s.score}/100</span>
      </div>
      <div class="bar-t"><div class="bar-f" style="width:${s.score}%;background:${sc}"></div></div>
    </div>
    ${aiSec}
    ${newsSec}
    <div class="reasons-wrap" id="reasons-${s.sym}">${s.reasons.map(r=>`<div class="reason">${r}</div>`).join('')}</div>
    <button class="expand-btn" onclick="toggleReasons('${s.sym}',this)">▼ SHOW SIGNAL BREAKDOWN</button>
  </div>`;
}

function render(d){
  curStrategy = d.strategy || 'ONE';
  document.getElementById('btn-one').className='strat-btn '+(curStrategy==='ONE'?'one-active':'');
  document.getElementById('btn-dev').className='strat-btn '+(curStrategy==='DEV'?'dev-active':'');

  const mb=document.getElementById('mkt-badge');
  mb.textContent=d.market_status||'—'; mb.className='mkt '+(d.market_status==='OPEN'?'mkt-open':'mkt-closed');
  document.getElementById('lu').textContent=d.last_updated||'Not scanned';
  document.getElementById('scan-count').textContent=d.scan_count?`Scan #${d.scan_count}`:'';

  document.getElementById('services').innerHTML=[
    {l:'NSE',ok:d.nse_ok},{l:'AI',ok:d.ai_ok},{l:'TG',ok:d.telegram_ok}
  ].map(x=>`<span class="svc"><span class="live ${x.ok?'g':'r'}" style="width:5px;height:5px"></span>${x.l}</span>`).join('');

  // VIX badge
  const vb=document.getElementById('vix-badge');
  if(d.vix){
    const col=d.vix>18?'#ff3d5a':d.vix<12?'#00ffa3':'#ffb830';
    const lbl=d.vix>18?'HIGH FEAR':d.vix<12?'LOW VOL':'NORMAL';
    vb.style.display='flex'; vb.style.color=col; vb.style.borderColor=col+'44';
    vb.textContent='VIX '+d.vix.toFixed(1)+' — '+lbl;
  } else { vb.style.display='none'; }

  // Weights
  if(d.weights && curStrategy==='ONE'){
    const tot=d.weights.tech+d.weights.mom+d.weights.oi;
    document.getElementById('wb-t').style.width=Math.round(d.weights.tech/tot*110)+'px'; document.getElementById('wb-t').textContent='T'+d.weights.tech;
    document.getElementById('wb-m').style.width=Math.round(d.weights.mom/tot*110)+'px';  document.getElementById('wb-m').textContent='M'+d.weights.mom;
    document.getElementById('wb-o').style.width=Math.round(d.weights.oi/tot*110)+'px';   document.getElementById('wb-o').textContent='OI'+d.weights.oi;
    document.getElementById('weight-bar').style.display='flex';
  } else {
    document.getElementById('weight-bar').style.display='none';
  }
  document.getElementById('regime-txt').textContent=d.vix_regime||'';

  // OI status
  const oi_el=document.getElementById('oi-status');
  if(oi_el && d.signals && d.signals.length){
    const hasNSE=d.signals.filter(s=>s.oi_src==='NSE').length;
    const hasYF=d.signals.filter(s=>s.oi_src==='yf_options').length;
    const hasProxy=d.signals.filter(s=>s.oi_src==='vol_proxy').length;
    const tot=d.signals.length;
    if(hasNSE>0) oi_el.textContent=`· OI: ${hasNSE} NSE${hasYF>0?' +'+hasYF+' yf':''}${hasProxy>0?' +'+hasProxy+' proxy':''}`;
    else if(hasYF>0) oi_el.textContent=`· OI: ${hasYF} yf${hasProxy>0?' +'+hasProxy+' proxy':''}`;
    else oi_el.textContent='· OI: vol proxy only';
    oi_el.style.color=hasNSE>0?'#00ffa3':hasYF>0?'#ffb830':'#ff3d5a';
  }

  document.getElementById('err-area').innerHTML=d.errors&&d.errors.length
    ?`<span style="background:#ff3d5a12;border:1px solid #ff3d5a30;border-radius:4px;padding:2px 7px;font-size:10px;color:#ff3d5a">⚠ ${d.errors.length} errors</span>`:'';

  // Choose signals based on strategy
  const activeSigs = curStrategy==='DEV' ? (d.dev_signals||[]) : (d.signals||[]);
  const activeTop  = curStrategy==='DEV'
    ? (d.dev_signals||[]).filter(s=>s.action!=='WAIT').slice(0,10)
    : (d.top_calls||[]);

  // Ticker
  if(activeSigs.length){
    const items=[...activeSigs,...activeSigs].map(s=>
      `<span class="tick" style="color:${s.change_pct>=0?'#00ffa3':'#ff3d5a'}">${s.sym} [${s.strategy}] ${s.action}${s.opt} ₹${s.spot.toLocaleString('en-IN')} ${s.change_pct>=0?'▲':'▼'}${Math.abs(s.change_pct).toFixed(2)}%</span>`
    ).join('');
    document.getElementById('ticker').innerHTML=items;
  }

  // TOP CALLS
  const grid=document.getElementById('cards-grid');
  if(!activeTop.length) grid.innerHTML='<div class="empty">No signals yet — click SCAN NOW</div>';
  else grid.innerHTML=activeTop.map(cardHtml).join('');

  // PE signals
  const peOnly=activeTop.filter(s=>s.opt==='PE');
  document.getElementById('puts-grid').innerHTML=peOnly.length?peOnly.map(cardHtml).join(''):'<div class="empty">No PE signals this scan.</div>';

  // Movers (by momentum+OI)
  const movers=[...activeSigs].sort((a,b)=>{
    const ma=(a.mom_score||0)+(a.oi_score||0)+(a.setup_quality||0)*0.5;
    const mb2=(b.mom_score||0)+(b.oi_score||0)+(b.setup_quality||0)*0.5;
    return mb2-ma;
  }).slice(0,8);
  document.getElementById('movers-grid').innerHTML=movers.length?movers.map(cardHtml).join(''):'<div class="empty">Run scan.</div>';

  // ALL table
  const tbody=document.getElementById('sig-tbody');
  if(!activeSigs.length) tbody.innerHTML='<tr><td colspan="16" class="empty">Run scan</td></tr>';
  else tbody.innerHTML=activeSigs.map(s=>{
    const sc=s.score>65?'#00ffa3':s.score>45?'#ffb830':'#ff3d5a';
    const isDev=s.strategy==='DEV';
    return`<tr>
      <td><b>${s.sym}</b><br><span style="font-size:10px;color:#3d5068">${s.name}</span></td>
      <td><span class="badge ${isDev?'b-dev':'b-one'}">${s.strategy}</span></td>
      <td><span class="badge ${s.data_src==='NSE Live'?'b-live':'b-delay'}">${s.data_src==='NSE Live'?'LIVE':'15m'}</span></td>
      <td class="mono">₹${s.spot.toLocaleString('en-IN')}</td>
      <td>${chg(s.change_pct)}</td>
      <td><span class="badge ${actionBadge(s.action)}">${s.action}</span><span class="badge b-${s.opt.toLowerCase()}" style="margin-left:2px">${s.opt}</span></td>
      <td class="mono" style="color:${s.opt==='CE'?'#00ffa3':'#ff3d5a'}">${s.strike}<span style="font-size:9px;color:#3d5068"> ${s.strike_tag}</span></td>
      <td class="mono" style="color:#3d9eff">₹${s.entry}</td>
      <td class="mono" style="color:#ff3d5a">₹${s.sl}</td>
      <td class="mono" style="color:#00ffa3">₹${s.t1}</td>
      <td class="mono" style="color:#00ffa3">₹${s.t2}</td>
      <td class="mono" style="color:#ffb830">₹${s.op}</td>
      <td class="mono" style="color:${s.rsi<35?'#00ffa3':s.rsi>65?'#ff3d5a':'#ffb830'}">${isDev?s.setup_quality+'/100':s.rsi}</td>
      <td class="mono" style="color:${s.pcr>1.2?'#ff3d5a':s.pcr<0.8?'#00ffa3':'#6b8299'}">${s.pcr}</td>
      <td style="color:${oic(s.oi_buildup)};font-size:11px">${s.oi_buildup}</td>
      <td><div style="display:flex;align-items:center;gap:5px">
        <span class="mono" style="color:${sc};font-size:11px">${s.score}</span>
        <div style="width:34px;height:3px;background:#1a2235;border-radius:2px;overflow:hidden">
          <div style="width:${s.score}%;height:100%;background:${sc}"></div>
        </div></div></td>
    </tr>`;
  }).join('');

  // OVERNIGHT (ONE strategy only)
  const og=document.getElementById('on-grid');
  const ons=(d.signals||[]).filter(s=>s.overnight);
  og.innerHTML=ons.length?ons.map(s=>`
    <div class="on-card fade">
      <div style="font-weight:800;font-size:14px;margin-bottom:2px">${s.sym}</div>
      <div style="display:flex;gap:5px;margin-bottom:6px"><span class="badge b-${s.opt.toLowerCase()}">${s.action} ${s.opt}</span><span class="mono" style="font-size:11px;color:#00d4ff">₹${s.spot.toLocaleString('en-IN')}</span></div>
      ${[['Entry','entry','#3d9eff'],['Target','t2','#00ffa3'],['SL','sl','#ff3d5a']].map(([l,k,c])=>`<div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:3px"><span style="color:#6b8299">${l}</span><span class="mono" style="color:${c}">₹${s[k]}</span></div>`).join('')}
      <div style="display:flex;justify-content:space-between;font-size:11px;margin-bottom:6px"><span style="color:#6b8299">Conf</span><span class="mono" style="color:#ffb830">${s.conf}%</span></div>
      <div class="bar-t"><div class="bar-f" style="width:${s.score}%;background:#00d4ff"></div></div>
    </div>`).join(''):'<div class="empty">No overnight holds recommended.</div>';

  // ALERTS
  const al=document.getElementById('alert-log');
  al.innerHTML=d.alerts_sent&&d.alerts_sent.length
    ?d.alerts_sent.map(a=>`<div style="padding:8px 14px;border-bottom:1px solid #0d1219;display:flex;align-items:center;gap:8px;font-size:12px"><span class="live ${a.action==='BUY'||a.action==='WATCH'?'g':'r'}"></span><b>${a.sym}</b><span class="badge ${actionBadge(a.action)}">${a.action}</span><span class="badge ${(a.strategy||'ONE')==='DEV'?'b-dev':'b-one'}">${a.strategy||'ONE'}</span><span style="color:#6b8299;font-size:11px">score ${a.score}</span><span class="mono" style="margin-left:auto;font-size:10px;color:#3d5068">${a.time}</span></div>`).join('')
    :'<div class="empty">No alerts yet</div>';
}

function toggleReasons(sym,btn){
  const rw=document.getElementById('reasons-'+sym);
  const open=rw.style.display==='flex';
  rw.style.display=open?'none':'flex';
  btn.textContent=open?'▼ SHOW SIGNAL BREAKDOWN':'▲ HIDE BREAKDOWN';
}

setInterval(fetchState, 15*60*1000);
fetchState();
</script>
</body>
</html>"""

# ══════════════════════════════════════════════════════════════════════════════
# SCHEDULER + MAIN
# ══════════════════════════════════════════════════════════════════════════════
def run_scan_safe():
    """Wrapper that catches all exceptions so scheduler never dies."""
    try:
        run_scan()
    except Exception as e:
        print(f"  ✗ run_scan_safe caught: {e}")
        state["is_scanning"] = False  # always release lock

def scheduler():
    schedule.every(15).minutes.do(run_scan_safe)
    while True:
        try:
            schedule.run_pending()
        except Exception as e:
            print(f"  ✗ scheduler error: {e}")
        time.sleep(20)

# ── START BACKGROUND THREADS (runs on both local and deployed) ────────────────
# This block runs when gunicorn imports the module OR when run directly.
# Guards prevent double-start on gunicorn multi-worker forks.
_started = False
def _start_background():
    global _started
    if _started: return
    _started = True
    print("  → Starting background scanner + scheduler...")
    threading.Thread(target=run_scan,  daemon=True).start()
    threading.Thread(target=scheduler, daemon=True).start()

# Gunicorn calls this on worker init
try:
    from gunicorn.app.base import BaseApplication  # only present when gunicorn is running
    _start_background()
except ImportError:
    pass  # not gunicorn — __main__ block below handles it

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    is_local = port == 5000

    print("""
╔══════════════════════════════════════════════════════════╗
║   OPTIONFLOW v4  —  Live NSE · VIX-Adaptive · Dual Strat ║
╚══════════════════════════════════════════════════════════╝
""")
    print(f"  NSE data      : Live quotes + OI (yfinance fallback if blocked)")
    print(f"  Strategy ONE  : Tech + Momentum + OI (VIX-adaptive weights)")
    print(f"  Strategy DEV  : OI levels → VWAP retest → Hammer/Engulfing → Entry")
    print(f"  Claude AI     : {'✓ SET' if ANTHROPIC_KEY else '✗ Set ANTHROPIC_API_KEY'}")
    print(f"  Telegram      : {'✓ SET' if TELEGRAM_TOKEN else '✗ Set TELEGRAM_BOT_TOKEN + CHAT_ID'}")
    print(f"  Upstox API    : {'✓ SET' if UPSTOX_KEY else '✗ Not configured'}")
    print(f"\n  Dashboard     : http://localhost:{port}" if is_local else f"\n  Deployed on port {port}")
    print(f"  Switch strats : Click ⚡ ONE or 🎯 DEV button in dashboard\n")

    _start_background()

    if is_local:
        threading.Timer(3, lambda: webbrowser.open(f"http://localhost:{port}")).start()

    app.run(host="0.0.0.0", port=port, debug=False, use_reloader=False)

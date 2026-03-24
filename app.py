import streamlit as st
import pandas as pd
import numpy as np
import requests
import io
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import warnings
warnings.filterwarnings("ignore")

st.set_page_config(page_title="Stock Trend Trader", layout="wide", initial_sidebar_state="expanded")

st.markdown("""
<style>
    .stApp { background-color: #0a0e17; color: #e8eaf0; }
    section[data-testid="stSidebar"] { background-color: #0f1623; border-right: 1px solid #1e2535; }
    .metric-card { background: linear-gradient(135deg, #141b2d, #1a2340); border: 1px solid #1e2d4a; border-radius: 10px; padding: 14px 18px; margin: 4px 0; }
    .metric-label { font-size: 0.65rem; color: #6b7fa3; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
    .metric-value { font-size: 1.6rem; font-weight: 800; color: #e8eaf0; margin-top: 4px; line-height: 1; }
    .metric-value.positive { color: #00e5b3; }
    .metric-value.negative { color: #ff5555; }
    .stButton > button { background: linear-gradient(135deg, #e63946, #c1121f); color: white; border: none; border-radius: 8px; font-weight: 700; width: 100%; padding: 12px; font-size: 1rem; }
    .stButton > button:hover { opacity: 0.85; }
    div[data-testid="stExpander"] { background: #0f1623; border-radius: 8px; border: 1px solid #1e2535; }
    .info-banner { background: #0d1e3a; border-left: 4px solid #3a7bd5; padding: 12px 18px; border-radius: 6px; margin-bottom: 18px; font-size: 0.9rem; color: #a8bedf; }
    .section-header { font-size: 1.2rem; font-weight: 700; color: #e8eaf0; margin: 20px 0 10px 0; padding-bottom: 8px; border-bottom: 2px solid #1e2d4a; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# GOOGLE DRIVE DATA LAYER
# ─────────────────────────────────────────────
DRIVE_FOLDER_ID = "1sbmWg11ZUU0kgm3yqGVVkQJoXRQ1NNmp"
DRIVE_API_KEY   = "AIzaSyC-jvjhm7FKkbmn2GLLgxWi8_gJCCS9a9Y"
HEADERS         = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
MAX_WORKERS     = 10
REQUEST_TIMEOUT = 30


@st.cache_data(ttl=86400, show_spinner=False)
def get_drive_file_map():
    """
    Fetch all file IDs from the public Google Drive folder using the Drive API.
    Returns dict: {TICKER: file_id}
    """
    mapping    = {}
    page_token = None
    page       = 0
    session    = requests.Session()

    while True:
        page += 1
        params = {
            "q":        f"'{DRIVE_FOLDER_ID}' in parents and trashed=false",
            "fields":   "nextPageToken,files(id,name)",
            "pageSize": 1000,
            "key":      DRIVE_API_KEY,
        }
        if page_token:
            params["pageToken"] = page_token

        try:
            r = session.get(
                "https://www.googleapis.com/drive/v3/files",
                params=params, timeout=30
            )
        except Exception as e:
            st.error(f"Drive API network error: {e}")
            break

        if r.status_code != 200:
            st.error(f"Drive API error {r.status_code}: {r.text[:200]}")
            break

        data  = r.json()
        files = data.get("files", [])

        for f in files:
            name   = f["name"]
            fid    = f["id"]
            ticker = (name
                      .replace(".us.txt","")
                      .replace(".us.csv","")
                      .replace(".txt","")
                      .replace(".csv","")
                      .upper()
                      .replace(".","-"))
            mapping[ticker] = fid

        page_token = data.get("nextPageToken")
        if not page_token:
            break
        import time; time.sleep(0.1)

    return mapping


def get_ticker_list():
    """Return sorted list of all tickers available in Drive."""
    file_map = get_drive_file_map()
    return sorted([t for t in file_map.keys() if t and not t.startswith(".")])


def fetch_single(ticker: str, start_date: str, end_date: str):
    """Download a single ticker CSV from Google Drive."""
    import time
    file_map = get_drive_file_map()
    file_id  = file_map.get(ticker.upper())
    if file_id is None:
        return None
    try:
        url = f"https://drive.google.com/uc?export=download&id={file_id}"
        r   = requests.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
        if r.status_code != 200 or len(r.text) < 30:
            return None
        # Check it's actually CSV data
        first_line = r.text.splitlines()[0].lower()
        if "date" not in first_line and "ticker" not in first_line:
            return None
        df = pd.read_csv(io.StringIO(r.text))
        if df.empty:
            return None
        # Strip angle brackets from Stooq headers e.g. <DATE> -> DATE
        df.columns = [c.strip().upper().replace("<","").replace(">","") for c in df.columns]
        rename = {}
        for c in df.columns:
            if c == "DATE":             rename[c] = "Date"
            elif c == "OPEN":           rename[c] = "Open"
            elif c == "HIGH":           rename[c] = "High"
            elif c == "LOW":            rename[c] = "Low"
            elif c == "CLOSE":          rename[c] = "Close"
            elif c in ("VOL","VOLUME"): rename[c] = "Volume"
        df = df.rename(columns=rename)
        if "Date" not in df.columns or "Close" not in df.columns:
            return None
        df["Date"] = pd.to_datetime(df["Date"].astype(str), format="%Y%m%d", errors="coerce")
        df = df.dropna(subset=["Date"]).set_index("Date").sort_index()
        df = df[str(start_date):str(end_date)]
        if df.empty or len(df) < 20:
            return None
        cols = [c for c in ["Open","High","Low","Close","Volume"] if c in df.columns]
        result = df[cols].apply(pd.to_numeric, errors="coerce").dropna(subset=["Close"])
        return result if not result.empty else None
    except Exception:
        return None


def download_universe(tickers, start_date, end_date, progress_bar=None):
    """Parallel download of all tickers from Google Drive."""
    frames  = {c: {} for c in ["Open","High","Low","Close","Volume"]}
    found   = []
    missing = []
    total   = len(tickers)
    done    = [0]

    def _get(t):
        return t, fetch_single(t, start_date, end_date)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(_get, t): t for t in tickers}
        for fut in as_completed(futures):
            ticker, df = fut.result()
            done[0] += 1
            if progress_bar and done[0] % 100 == 0:
                pct = done[0] / total
                progress_bar.progress(pct, text=f"Downloaded {done[0]:,}/{total:,} — {len(found):,} loaded so far…")
            if df is not None and not df.empty:
                for c in ["Open","High","Low","Close","Volume"]:
                    if c in df.columns:
                        frames[c][ticker] = df[c]
                found.append(ticker)
            else:
                missing.append(ticker)

    if not frames["Close"]:
        return {}, found, missing

    result = {}
    for c in ["Open","High","Low","Close","Volume"]:
        if frames[c]:
            result[c] = pd.DataFrame(frames[c]).sort_index()

    return result, found, missing


def fetch_etf(ticker, start_date, end_date):
    """Fetch ETF price series from Drive, fallback to yfinance."""
    df = fetch_single(ticker, str(start_date), str(end_date))
    if df is not None and "Close" in df.columns:
        return df["Close"]
    try:
        import yfinance as yf
        raw = yf.download(ticker, start=str(start_date), end=str(end_date),
                          auto_adjust=True, progress=False)
        if not raw.empty:
            return raw["Close"].squeeze()
    except Exception:
        pass
    return None


def ensure_data_loaded(start_date, end_date):
    """
    Called at the top of every page render.
    If data is not yet in session state, downloads it now.
    Uses a status banner so the user knows what's happening.
    """
    key_data   = f"data_{start_date}_{end_date}"
    key_found  = f"found_{start_date}_{end_date}"
    key_miss   = f"missing_{start_date}_{end_date}"
    key_tickers= "all_tickers"

    # Load ticker list if not cached
    if key_tickers not in st.session_state:
        with st.spinner("Loading ticker universe from NASDAQ directory…"):
            st.session_state[key_tickers] = get_ticker_list()

    all_tickers = st.session_state[key_tickers]

    # Download price data if not cached for this date range
    if key_data not in st.session_state:
        n = len(all_tickers)
        banner = st.info(
            f"⏳ First-time data load: downloading **{n:,} tickers** from Stooq.com "
            f"({start_date} → {end_date}). This takes 2–5 minutes and is cached for 24 hours."
        )
        prog = st.progress(0, text=f"Starting parallel downloads (0/{n:,})…")
        data, found, missing = download_universe(
            all_tickers, str(start_date), str(end_date),
            progress_bar=prog
        )
        prog.empty()
        banner.empty()
        st.session_state[key_data]    = data
        st.session_state[key_found]   = found
        st.session_state[key_miss]    = missing
        hit = len(found) / max(len(all_tickers), 1) * 100
        st.success(
            f"✅ Data ready — **{len(found):,}/{n:,}** tickers loaded ({hit:.0f}% hit rate). "
            f"All tabs will use this data."
        )

    return (
        st.session_state[key_tickers],
        st.session_state[key_data],
        st.session_state[key_found],
        st.session_state[key_miss],
    )


# ─────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────
def run_backtest(data, tickers, params, initial_capital, strategy_mode="atr", progress_bar=None):
    bp  = params["breakout_period"]
    ap  = params["atr_period"]
    am  = params["atr_mult"]
    ms  = params["min_spacing"]
    ma  = params["max_age"]
    mp  = params["max_pyramid"]
    s2  = params["sma200"]
    s5  = params["sma50"]
    vmp = params["vol_ma_period"]
    vsm = params["vol_spike_mult"]
    mav = params["min_avg_vol"]
    brp = params["base_risk_pct"]
    mnp = params["min_price"]
    rev = params.get("stop_review_bars", 10)

    raw   = []
    close = data.get("Close", pd.DataFrame())
    valid = [t for t in tickers if t in close.columns]
    total = len(valid)
    if total == 0:
        return pd.DataFrame()

    for idx, ticker in enumerate(valid):
        if progress_bar:
            progress_bar.progress((idx+1)/total, text=f"Scanning {ticker}…")
        try:
            c   = data["Close"][ticker].dropna()
            o   = data["Open"][ticker].reindex(c.index).ffill()
            h   = data["High"][ticker].reindex(c.index).ffill()
            lo  = data["Low"][ticker].reindex(c.index).ffill()
            vol = data["Volume"][ticker].reindex(c.index).fillna(0)

            mb = max(bp, ap, s2, vmp) + 10
            if len(c) < mb:
                continue

            tr     = pd.concat([h-lo, (h-c.shift(1)).abs(), (lo-c.shift(1)).abs()], axis=1).max(axis=1)
            atr    = tr.ewm(span=ap, adjust=False).mean()
            sma200 = c.rolling(s2).mean()
            sma50  = c.rolling(s5).mean()
            volma  = vol.rolling(vmp).mean()

            if strategy_mode == "atr":
                cbh  = pd.concat([o, c], axis=1).max(axis=1)
                hb52 = cbh.rolling(bp).max()
            else:
                hb52 = h.rolling(bp).max()

            in_t  = False
            slvl  = entry = hbh = hba = np.nan
            age   = bsa = pyc = bsr = 0
            high_since_review = np.nan

            for j in range(mb, len(c)):
                cv = c.iloc[j]; lv = lo.iloc[j]; hv = h.iloc[j]
                cb = cbh.iloc[j] if strategy_mode == "atr" else hv
                hb = hb52.iloc[j]; av = atr.iloc[j]
                v2 = sma200.iloc[j]; v5 = sma50.iloc[j]
                vl = vol.iloc[j]; vm = volma.iloc[j]
                dt = c.index[j]

                if pd.isna(hb) or pd.isna(av) or pd.isna(v2):
                    continue

                sd = am * av
                ok = vm >= mav; vs = vl >= vm * vsm
                a2 = cv > v2;   a5 = cv > v5; px = cv >= mnp

                if strategy_mode == "atr":
                    esig = cb >= hb and a2 and a5 and not in_t and vs and ok and px
                else:
                    esig = hv == hb and a2 and not in_t and vs and ok and px

                asig = in_t and a2 and hv > hba and bsa >= ms and age < ma and vl > vm and pyc < mp
                sh   = in_t and lv < slvl
                te   = in_t and age >= ma

                if esig:
                    in_t  = True
                    entry = cv; slvl = cv - sd; hbh = cb; hba = hv
                    age   = bsa = pyc = bsr = 0
                    high_since_review = hv
                    raw.append({
                        "ticker": ticker, "entry_date": dt,
                        "entry_price": cv, "stop_dist": sd,
                        "rel_vol": vl/vm if vm > 0 else 1.0,
                        "exit_date": None, "exit_price": None,
                        "exit_type": None, "pyramid_adds": 0,
                        "trade_age": 0, "_qty": 0, "_eq": 0.0,
                    })

                elif sh or te:
                    xp = slvl if sh else cv
                    xt = "Stop" if sh else "TimeExit"
                    for s in reversed(raw):
                        if s["ticker"] == ticker and s["exit_date"] is None:
                            s["exit_date"]    = dt; s["exit_price"] = xp
                            s["exit_type"]    = xt; s["pyramid_adds"] = pyc
                            s["trade_age"]    = age; break
                    in_t  = False
                    slvl  = entry = hbh = hba = np.nan
                    age   = bsa = pyc = bsr = 0
                    high_since_review = np.nan

                elif in_t:
                    age += 1; bsa += 1

                    if strategy_mode == "atr":
                        if cb > hbh: hbh = cb
                        ns = hbh - sd
                        if ns > slvl: slvl = ns
                        if asig: hba = hv; bsa = 0; pyc += 1
                        elif hv > hba: hba = hv

                    elif strategy_mode == "mtp":
                        if asig: slvl = cv - sd; hba = hv; bsa = 0; pyc += 1
                        elif hv > hba: hba = hv

                    elif strategy_mode == "10bar":
                        bsr += 1
                        if hv > high_since_review: high_since_review = hv
                        add_fired = False
                        if asig:
                            slvl = cv - sd; hba = hv; bsa = 0; pyc += 1
                            bsr  = 0; high_since_review = hv; add_fired = True
                        if not add_fired and bsr >= rev:
                            if high_since_review > (slvl + sd):
                                ns = high_since_review - sd
                                if ns > slvl: slvl = ns
                            bsr = 0; high_since_review = hv
                        if not asig and hv > hba: hba = hv

        except Exception:
            continue

    raw = [s for s in raw if s["exit_date"] is not None]
    if not raw:
        return pd.DataFrame()

    LEVERAGE = 2.0
    raw.sort(key=lambda x: (x["entry_date"], -x.get("rel_vol", 0)))
    eq = float(initial_capital); bp_ = eq * LEVERAGE
    open_ = {}; trades = []; taken = set()

    all_dates = sorted(set([s["entry_date"] for s in raw] + [s["exit_date"] for s in raw]))
    by_entry  = defaultdict(list)
    by_exit   = defaultdict(list)
    for s in raw:
        by_entry[s["entry_date"]].append(s)
        by_exit[s["exit_date"]].append(s)

    for dt in all_dates:
        for s in by_exit[dt]:
            if id(s) not in taken: continue
            pnl = (s["exit_price"] - s["entry_price"]) * s["_qty"]
            trades.append({
                "ticker":          s["ticker"],
                "entry_date":      s["entry_date"],
                "exit_date":       s["exit_date"],
                "entry_price":     round(s["entry_price"], 4),
                "exit_price":      round(s["exit_price"], 4),
                "qty":             s["_qty"],
                "pnl_pct":         round((s["exit_price"]-s["entry_price"])/s["entry_price"]*100, 4),
                "pnl_dollar":      round(pnl, 4),
                "exit_type":       s["exit_type"],
                "pyramid_adds":    s["pyramid_adds"],
                "trade_age":       s["trade_age"],
                "equity_at_entry": round(s["_eq"], 2),
            })
            open_.pop(s["ticker"], 0)
            eq  = max(eq + pnl, 1.0)
            used = sum(open_.values()); bp_ = max(eq * LEVERAGE - used, 0.0)

        for s in by_entry[dt]:
            sd = s["stop_dist"]
            if sd <= 0 or pd.isna(sd) or s["ticker"] in open_: continue
            qty  = max(1, int(eq * (brp/100) / sd))
            need = qty * s["entry_price"]
            if need > bp_: continue
            taken.add(id(s)); s["_qty"] = qty; s["_eq"] = eq
            open_[s["ticker"]] = need; bp_ -= need

    trades.sort(key=lambda x: x["exit_date"])
    return pd.DataFrame(trades)


# ─────────────────────────────────────────────
# METRICS
# ─────────────────────────────────────────────
def compute_metrics(df, initial_capital, start_date, end_date):
    if df.empty:
        return {}, pd.Series(dtype=float), pd.Series(dtype=float)
    df = df.copy()
    df["exit_date"]  = pd.to_datetime(df["exit_date"])
    df["entry_date"] = pd.to_datetime(df["entry_date"])
    df = df.sort_values("exit_date")
    idx    = pd.date_range(start=start_date, end=end_date, freq="B")
    equity = pd.Series(float(initial_capital), index=idx)
    cum    = 0.0
    for _, t in df.iterrows():
        cum += t["pnl_dollar"]
        equity[equity.index >= t["exit_date"]] = initial_capital + cum
    tr   = (equity.iloc[-1] - equity.iloc[0]) / equity.iloc[0]
    ny   = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 365.25
    cagr = (1+tr)**(1/ny)-1 if ny > 0 else 0
    dr   = equity.pct_change().dropna()
    sh   = dr.mean()/dr.std()*np.sqrt(252) if dr.std() > 0 else 0
    ds   = dr[dr < 0].std()
    so   = dr.mean()/ds*np.sqrt(252) if ds > 0 else 0
    rm   = equity.cummax(); dd = (equity - rm) / rm; mdd = dd.min()
    cal  = cagr/abs(mdd) if mdd != 0 else 0
    wins = df[df["pnl_dollar"] > 0]; loss = df[df["pnl_dollar"] <= 0]
    wr   = len(wins)/len(df)*100 if len(df) > 0 else 0
    aw   = wins["pnl_pct"].mean() if len(wins) > 0 else 0
    al   = loss["pnl_pct"].mean() if len(loss) > 0 else 0
    gp   = wins["pnl_dollar"].sum() if len(wins) > 0 else 0
    gl   = abs(loss["pnl_dollar"].sum()) if len(loss) > 0 else 1
    pf   = gp/gl if gl > 0 else 0
    exp  = (wr/100*aw) + ((1-wr/100)*al)
    return {
        "Total Return":  f"{tr*100:.2f}%",
        "CAGR":          f"{cagr*100:.2f}%",
        "Sharpe":        f"{sh:.3f}",
        "Sortino":       f"{so:.3f}",
        "Calmar":        f"{cal:.3f}",
        "Max Drawdown":  f"{mdd*100:.2f}%",
        "Total Trades":  len(df),
        "Win Rate":      f"{wr:.2f}%",
        "Avg Win":       f"{aw:.2f}%",
        "Avg Loss":      f"{al:.2f}%",
        "Profit Factor": f"{pf:.2f}",
        "Expectancy":    f"{exp:.2f}%",
        "_cagr": cagr, "_sharpe": sh, "_calmar": cal, "_maxdd": mdd, "_pf": pf,
    }, equity, dd


# ─────────────────────────────────────────────
# BENCHMARKS
# ─────────────────────────────────────────────
def compute_dca(ticker, initial_capital, monthly, start_date, end_date):
    prices = fetch_etf(ticker, str(start_date), str(end_date))
    if prices is None: return None
    idx    = pd.date_range(start=start_date, end=end_date, freq="B")
    prices = prices.reindex(idx).ffill().bfill()
    if prices.isna().all() or prices.iloc[0] == 0: return None
    shares    = initial_capital / prices.iloc[0]
    eq        = pd.Series(index=idx, dtype=float)
    eq.iloc[0]= shares * prices.iloc[0]
    last_m    = prices.index[0].month
    for i, dt in enumerate(prices.index[1:], 1):
        if dt.month != last_m:
            shares += monthly / prices.iloc[i]; last_m = dt.month
        eq.iloc[i] = shares * prices.iloc[i]
    return eq

def add_monthly(equity, monthly):
    if monthly <= 0: return equity
    eq = equity.copy().astype(float); last_m = eq.index[0].month; cum = 0.0
    for i, dt in enumerate(eq.index[1:], 1):
        if dt.month != last_m: cum += monthly; last_m = dt.month
        eq.iloc[i] += cum
    return eq


# ─────────────────────────────────────────────
# CHARTS
# ─────────────────────────────────────────────
COLORS = {"atr": "#00e5b3", "mtp": "#4a90d9", "10bar": "#f5a623", "qqq": "#a855f7", "spy": "#6b7fa3"}

def chart_comparison(results, qqq=None, spy=None, monthly=0):
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28], vertical_spacing=0.03)
    names = {"atr": "S1: ATR Trailing", "mtp": "S2: MTP Static", "10bar": "S3: 10-Bar Static"}
    for mode, (equity, dd) in results.items():
        strat = add_monthly(equity, monthly)
        fig.add_trace(go.Scatter(x=strat.index, y=strat.values,
            line=dict(color=COLORS[mode], width=2), name=names[mode]), row=1, col=1)
        fig.add_trace(go.Scatter(x=dd.index, y=dd.values*100,
            line=dict(color=COLORS[mode], width=1, dash="dot"),
            name=f"{names[mode]} DD", showlegend=False), row=2, col=1)
    if qqq is not None:
        fig.add_trace(go.Scatter(x=qqq.index, y=qqq.values,
            line=dict(color=COLORS["qqq"], width=1.5, dash="dash"), name="QQQ DCA"), row=1, col=1)
    if spy is not None:
        fig.add_trace(go.Scatter(x=spy.index, y=spy.values,
            line=dict(color=COLORS["spy"], width=1.5, dash="dot"), name="SPY DCA"), row=1, col=1)
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=480,
        showlegend=True, legend=dict(bgcolor="rgba(10,14,23,0.9)", bordercolor="#1e2d4a", borderwidth=1, x=0.01, y=0.99))
    fig.update_xaxes(gridcolor="#141b2d"); fig.update_yaxes(gridcolor="#141b2d")
    fig.update_yaxes(title_text="Portfolio ($)", row=1, col=1)
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
    return fig

def chart_annual(equity, color, name):
    ann    = equity.resample("YE").last().pct_change().dropna() * 100
    colors = [color if v >= 0 else "#ff5555" for v in ann.values]
    fig    = go.Figure(go.Bar(x=ann.index.year, y=ann.values, marker_color=colors, name=name))
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=280,
        xaxis_title="Year", yaxis_title="Return (%)", title=name, title_font=dict(color=color))
    fig.update_xaxes(gridcolor="#141b2d")
    fig.update_yaxes(gridcolor="#141b2d", zeroline=True, zerolinecolor="#2a3550")
    return fig

def chart_dist(df, color, name):
    if df.empty: return go.Figure()
    fig = px.histogram(df, x="pnl_pct", nbins=60, color_discrete_sequence=[color],
                       labels={"pnl_pct": "Trade Return (%)"}, title=name)
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=30,b=0), height=280, title_font=dict(color=color))
    fig.update_xaxes(gridcolor="#141b2d"); fig.update_yaxes(gridcolor="#141b2d")
    return fig

def mc(label, value, pos_good=True, color=None):
    neg = isinstance(value, str) and value.startswith("-")
    if color:
        return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value" style="color:{color}">{value}</div></div>'
    css = ("negative" if pos_good else "positive") if neg else ("positive" if pos_good else "negative")
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {css}">{value}</div></div>'


# ─────────────────────────────────────────────
# OPTIMIZER
# ─────────────────────────────────────────────
def suggest(space, past, n_start=10):
    def uniform():
        p = {}
        for k, (lo, hi, step, typ) in space.items():
            if typ == "int":
                p[k] = int(np.random.choice(range(lo, hi+1, step)))
            else:
                n = int(round((hi-lo)/step))
                p[k] = round(lo + np.random.randint(0, n+1)*step, 4)
        return p
    if len(past) < n_start: return uniform()
    best = max(past, key=lambda x: x["score"])["params"]
    p = {}
    for k, (lo, hi, step, typ) in space.items():
        if typ == "int":
            noise = int(np.random.choice([-2,-1,0,0,1,2])) * step
            val   = int(np.clip(best[k]+noise, lo, hi))
            p[k]  = min(range(lo, hi+1, step), key=lambda x: abs(x-val))
        else:
            val   = round(np.clip(best[k] + np.random.uniform(-(hi-lo)*0.15, (hi-lo)*0.15), lo, hi), 4)
            n     = int(round((val-lo)/step))
            p[k]  = round(lo + n*step, 4)
    return p


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")

    with st.expander("📊 Data", expanded=True):
        c1, c2 = st.columns(2)
        with c1: start_date = st.date_input("Start", datetime(1995, 1, 1))
        with c2: end_date   = st.date_input("End",   datetime.today())
        initial_capital      = st.number_input("Capital ($)", min_value=1000, value=10000, step=1000)
        monthly_contribution = st.number_input("Monthly Add ($)", min_value=0, value=500, step=100)
        st.caption("Data auto-loads on first open and is shared across all tabs.")

    with st.expander("🔀 Strategies", expanded=True):
        run_s1   = st.toggle("S1 — ATR Trailing Stop",  value=True)
        run_s2   = st.toggle("S2 — MTP Static Stop",    value=True)
        run_s3   = st.toggle("S3 — 10-Bar Static Stop", value=True)
        show_qqq = st.toggle("Show QQQ benchmark",      value=True)
        show_spy = st.toggle("Show SPY benchmark",       value=True)

    with st.expander("🎯 Parameters", expanded=True):
        breakout_period  = st.number_input("52W Lookback",           10,  504,  252)
        atr_period       = st.number_input("ATR Period",             1,   500,  293)
        atr_mult         = st.number_input("ATR Multiplier",         0.1, 20.0, 7.25, step=0.05)
        max_pyramid      = st.number_input("Max Pyramid Adds",       0,   20,   5)
        min_spacing      = st.number_input("Min Bars Between Adds",  1,   100,  38)
        max_age          = st.number_input("Max Trade Age (bars)",   10,  2000, 584)
        base_risk_pct    = st.number_input("Risk % per Trade",       0.1, 10.0, 2.0,  step=0.25)
        stop_review_bars = st.number_input("Stop Review Bars (S3)",  1,   50,   10)

    with st.expander("📈 Moving Averages", expanded=True):
        sma200 = st.number_input("Trend SMA",   1, 500, 200)
        sma50  = st.number_input("Context SMA", 1, 300, 50)

    with st.expander("🔊 Volume"):
        vol_ma_period  = st.number_input("Volume MA Period",    5,  100,        20)
        vol_spike_mult = st.number_input("Vol Spike Mult",      1.0, 5.0,       1.5, step=0.1)
        min_avg_vol    = st.number_input("Min Avg Volume",      0,   10_000_000, 1_000_000, step=100_000)

    with st.expander("🔍 Price"):
        min_price = st.number_input("Min Price ($)", 0.0, 500.0, 10.0, step=0.5)

    st.markdown("---")
    run_btn = st.button("🚀 Run Comparison")


# ─────────────────────────────────────────────
# AUTO-LOAD DATA (runs every page render)
# ─────────────────────────────────────────────
st.markdown("# 📈 Stock Trend Trader — Strategy Comparison")
st.markdown("*ATR Trailing · MTP Static · 10-Bar Static · vs QQQ & SPY*")
st.markdown("---")

# Load ticker list only on startup (fast — just API call)
if "all_tickers" not in st.session_state:
    with st.spinner("Connecting to Google Drive…"):
        st.session_state["all_tickers"] = get_ticker_list()
all_tickers = st.session_state["all_tickers"]
data = st.session_state.get(f"data_{start_date}_{end_date}", {})
found = st.session_state.get(f"found_{start_date}_{end_date}", [])
missing = st.session_state.get(f"missing_{start_date}_{end_date}", [])

if all_tickers:
    st.caption(f"📂 **{len(all_tickers):,}** tickers available in Google Drive — click Run Comparison to load data")


# ─────────────────────────────────────────────
# TABS
# ─────────────────────────────────────────────
main_tab1, main_tab2 = st.tabs(["📊 Strategy Comparison", "🔬 Optimization"])


# ══════════════════════════════════════════
# TAB 1 — COMPARISON
# ══════════════════════════════════════════
with main_tab1:
    if not run_btn:
        st.markdown("""<div class="info-banner">
        Data is already loaded — just click <strong>Run Comparison</strong> to start.<br>
        Toggle strategies and adjust parameters in the sidebar. All 3 strategies run with identical settings.
        </div>""", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("""**S1 — ATR Trailing** 🟢
Stop ratchets up every bar from highest candle body high since entry.""")
        with c2:
            st.markdown("""**S2 — MTP Static** 🔵
Stop frozen between adds. Only resets to close − ATR when an add fires.""")
        with c3:
            st.markdown("""**S3 — 10-Bar Static** 🟠
Stop reviews every N bars. Only updates if new high made in that window.""")
    else:
        active = []
        if run_s1: active.append("atr")
        if run_s2: active.append("mtp")
        if run_s3: active.append("10bar")

        if not active:
            st.error("Select at least one strategy.")
            st.stop()

        # Download data if not yet loaded for this date range
        key_data  = f"data_{start_date}_{end_date}"
        key_found = f"found_{start_date}_{end_date}"
        key_miss  = f"missing_{start_date}_{end_date}"
        if key_data not in st.session_state:
            n      = len(all_tickers)
            banner = st.info(f"⏳ Downloading **{n:,} tickers** from Google Drive… first run takes a few minutes.")
            prog   = st.progress(0, text="Starting downloads…")
            d, f_, m = download_universe(all_tickers, str(start_date), str(end_date), progress_bar=prog)
            prog.empty(); banner.empty()
            st.session_state[key_data]  = d
            st.session_state[key_found] = f_
            st.session_state[key_miss]  = m
            data   = d; found = f_; missing = m
            hit    = len(found) / max(n, 1) * 100
            st.success(f"✅ **{len(found):,}/{n:,}** tickers loaded ({hit:.0f}% hit rate)")
        else:
            data   = st.session_state[key_data]
            found  = st.session_state[key_found]
            missing= st.session_state[key_miss]

        if not data:
            st.error("No data loaded from Google Drive. Check your API key and folder permissions.")
            st.stop()

        params = {
            "breakout_period":  breakout_period,
            "atr_period":       atr_period,
            "atr_mult":         atr_mult,
            "min_spacing":      min_spacing,
            "max_age":          max_age,
            "max_pyramid":      max_pyramid,
            "sma200":           sma200,
            "sma50":            sma50,
            "vol_ma_period":    vol_ma_period,
            "vol_spike_mult":   vol_spike_mult,
            "min_avg_vol":      min_avg_vol,
            "base_risk_pct":    base_risk_pct,
            "min_price":        min_price,
            "stop_review_bars": stop_review_bars,
        }

        st.info(f"**{len(found):,} tickers** | **{len(active)} strategies** | {start_date} → {end_date}")

        mode_names  = {"atr": "S1: ATR Trailing", "mtp": "S2: MTP Static", "10bar": "S3: 10-Bar Static"}
        mode_colors = {"atr": COLORS["atr"], "mtp": COLORS["mtp"], "10bar": COLORS["10bar"]}

        all_results = {}; all_metrics = {}; all_trades = {}

        for mode in active:
            with st.spinner(f"Running {mode_names[mode]}…"):
                prog   = st.progress(0, text=f"Scanning for {mode_names[mode]}…")
                trades = run_backtest(data, found, params, initial_capital, mode, prog)
                prog.empty()
            if trades.empty:
                st.warning(f"{mode_names[mode]}: No trades generated.")
                continue
            metrics, equity, drawdown = compute_metrics(trades, initial_capital, start_date, end_date)
            all_results[mode] = (equity, drawdown)
            all_metrics[mode] = metrics
            all_trades[mode]  = trades

        if not all_results:
            st.warning("No results for any strategy.")
            st.stop()

        qqq_eq = spy_eq = None
        if show_qqq:
            with st.spinner("Loading QQQ…"):
                qqq_eq = compute_dca("QQQ", initial_capital, monthly_contribution, start_date, end_date)
        if show_spy:
            with st.spinner("Loading SPY…"):
                spy_eq = compute_dca("SPY", initial_capital, monthly_contribution, start_date, end_date)

        st.markdown('<div class="section-header">📈 Equity Curve Comparison</div>', unsafe_allow_html=True)
        st.plotly_chart(chart_comparison(all_results, qqq_eq, spy_eq, monthly_contribution), use_container_width=True)

        n_months = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 30.44
        total_in = initial_capital + monthly_contribution * n_months
        st.caption(f"Total contributed: **${total_in:,.0f}** (${initial_capital:,} initial + ${monthly_contribution:,}/mo)")

        all_cols = list(all_results.keys())
        if show_qqq and qqq_eq is not None: all_cols.append("qqq")
        if show_spy and spy_eq is not None: all_cols.append("spy")

        cols = st.columns(len(all_cols))
        for i, key in enumerate(all_cols):
            with cols[i]:
                if key == "qqq":
                    st.markdown(mc("QQQ Final", f"${qqq_eq.iloc[-1]:,.0f}", color=COLORS["qqq"]), unsafe_allow_html=True)
                elif key == "spy":
                    st.markdown(mc("SPY Final", f"${spy_eq.iloc[-1]:,.0f}", color=COLORS["spy"]), unsafe_allow_html=True)
                else:
                    final = add_monthly(all_results[key][0], monthly_contribution).iloc[-1]
                    st.markdown(mc(f"{mode_names[key]} Final", f"${final:,.0f}", color=mode_colors[key]), unsafe_allow_html=True)

        st.markdown('<div class="section-header">📊 Performance Metrics</div>', unsafe_allow_html=True)
        metric_keys = ["CAGR","Sharpe","Calmar","Max Drawdown","Total Trades","Win Rate","Profit Factor","Expectancy"]
        pos_good    = [True, True, True, False, True, True, True, True]
        cols = st.columns(len(all_results))
        for i, (mode, metrics) in enumerate(all_metrics.items()):
            with cols[i]:
                st.markdown(f'<div style="color:{mode_colors[mode]};font-weight:700;font-size:1rem;margin-bottom:8px">{mode_names[mode]}</div>', unsafe_allow_html=True)
                for mk, pg in zip(metric_keys, pos_good):
                    st.markdown(mc(mk, metrics[mk], pg), unsafe_allow_html=True)

        st.markdown('<div class="section-header">📅 Annual Returns</div>', unsafe_allow_html=True)
        cols = st.columns(len(all_results))
        for i, (mode, (equity, _)) in enumerate(all_results.items()):
            with cols[i]:
                st.plotly_chart(chart_annual(equity, mode_colors[mode], mode_names[mode]), use_container_width=True)

        st.markdown('<div class="section-header">📦 Trade Distribution</div>', unsafe_allow_html=True)
        cols = st.columns(len(all_results))
        for i, (mode, trades) in enumerate(all_trades.items()):
            with cols[i]:
                st.plotly_chart(chart_dist(trades, mode_colors[mode], mode_names[mode]), use_container_width=True)

        st.markdown('<div class="section-header">📋 Trade Logs</div>', unsafe_allow_html=True)
        trade_tabs = st.tabs([mode_names[m] for m in all_trades.keys()])
        for tab, (mode, trades) in zip(trade_tabs, all_trades.items()):
            with tab:
                dcols = ["ticker","entry_date","exit_date","entry_price","exit_price",
                         "pnl_pct","pnl_dollar","exit_type","pyramid_adds","trade_age","equity_at_entry"]
                ddf = trades[dcols].copy()
                for c in ["pnl_pct","pnl_dollar","entry_price","exit_price"]:
                    ddf[c] = ddf[c].round(2)
                st.dataframe(ddf, use_container_width=True, height=350)
                st.download_button(f"⬇️ Download {mode_names[mode]}",
                    ddf.to_csv(index=False), f"trades_{mode}.csv", "text/csv")


# ══════════════════════════════════════════
# TAB 2 — OPTIMIZATION
# Uses the same already-loaded data — no re-download
# ══════════════════════════════════════════
with main_tab2:
    st.markdown("### 🔬 Parameter Optimization")
    st.markdown("""<div class="info-banner">
    Uses the same data already loaded on startup — no extra downloads needed.
    Run multiple times with different metrics, then find your middle ground manually.
    </div>""", unsafe_allow_html=True)

    oc1, oc2, oc3 = st.columns(3)
    with oc1: opt_metric   = st.selectbox("Optimize For", ["Calmar","Sharpe","CAGR","Profit Factor"])
    with oc2: opt_strategy = st.selectbox("Strategy", ["S1: ATR Trailing","S2: MTP Static","S3: 10-Bar Static"])
    with oc3: n_trials     = st.number_input("Trials", 5, 200, 30, step=5)

    st.markdown("#### Search Ranges")
    sr1, sr2 = st.columns(2)
    with sr1:
        bp_r = st.slider("52W Lookback",         50,   504,  (150, 350), step=10)
        ap_r = st.slider("ATR Period",            10,   400,  (150, 350), step=5)
        am_r = st.slider("ATR Multiplier",        2.0,  15.0, (5.0, 10.0), step=0.5)
        mp_r = st.slider("Max Pyramid Adds",      0,    15,   (2, 8),    step=1)
        ms_r = st.slider("Min Bars Between Adds", 5,    80,   (20, 50),  step=1)
        ma_r = st.slider("Max Trade Age",         100,  2000, (400, 800), step=50)
    with sr2:
        s2_r = st.slider("Trend SMA",             100,  300,  (150, 250), step=10)
        s5_r = st.slider("Context SMA",           20,   100,  (40, 70),  step=5)
        vs_r = st.slider("Vol Spike Mult",         1.0,  3.0,  (1.2, 2.0), step=0.1)
        br_r = st.slider("Base Risk %",            0.5,  5.0,  (1.0, 3.5), step=0.25)

    oc4, oc5 = st.columns(2)
    with oc4: opt_start = st.date_input("Start", datetime(2005, 1, 1), key="os")
    with oc5: opt_end   = st.date_input("End",   datetime.today(),     key="oe")
    opt_cap = st.number_input("Capital ($)", 1000, 10_000_000, 10000, step=1000, key="oc")

    run_opt = st.button("🚀 Run Optimization")

    if run_opt:
        opt_mode_map = {"S1: ATR Trailing":"atr","S2: MTP Static":"mtp","S3: 10-Bar Static":"10bar"}
        opt_mode     = opt_mode_map[opt_strategy]

        if not data or not found:
            st.error("No data loaded. Reload the page.")
            st.stop()

        # Filter data to the optimization date range
        opt_data = {}
        for c in ["Open","High","Low","Close","Volume"]:
            if c in data:
                filtered = data[c][str(opt_start):str(opt_end)]
                if not filtered.empty:
                    opt_data[c] = filtered
        opt_found = [t for t in found if t in opt_data.get("Close", pd.DataFrame()).columns]

        st.success(f"Using **{len(opt_found):,}** tickers already in memory. Starting {n_trials} trials…")

        score_map = {"Calmar":"_calmar","Sharpe":"_sharpe","CAGR":"_cagr","Profit Factor":"_pf"}
        sort_map  = {"Calmar":"Calmar","Sharpe":"Sharpe","CAGR":"CAGR%","Profit Factor":"Profit Factor"}
        skey = score_map[opt_metric]
        scol = sort_map[opt_metric]

        space = {
            "breakout_period": (bp_r[0], bp_r[1], 10,   "int"),
            "atr_period":      (ap_r[0], ap_r[1], 5,    "int"),
            "atr_mult":        (am_r[0], am_r[1], 0.5,  "float"),
            "max_pyramid":     (mp_r[0], mp_r[1], 1,    "int"),
            "min_spacing":     (ms_r[0], ms_r[1], 1,    "int"),
            "max_age":         (ma_r[0], ma_r[1], 50,   "int"),
            "sma200":          (s2_r[0], s2_r[1], 10,   "int"),
            "sma50":           (s5_r[0], s5_r[1], 5,    "int"),
            "vol_spike_mult":  (vs_r[0], vs_r[1], 0.1,  "float"),
            "base_risk_pct":   (br_r[0], br_r[1], 0.25, "float"),
        }
        fixed = {
            "vol_ma_period":    vol_ma_period,
            "min_avg_vol":      min_avg_vol,
            "min_price":        min_price,
            "stop_review_bars": stop_review_bars,
        }

        results = []; past = []; best_s = -np.inf; best_t = None
        tbl  = st.empty()
        prog = st.progress(0, text="Starting…")

        for n in range(1, n_trials+1):
            prog.progress(n/n_trials, text=f"Trial {n}/{n_trials} | Best {opt_metric}: {best_s:.4f}")
            tp  = suggest(space, past)
            fp  = {**tp, **fixed}
            tdf = run_backtest(opt_data, opt_found, fp, opt_cap, opt_mode)

            if tdf.empty:
                score = -999.0
                row   = {"trial": n, **{k: tp[k] for k in space},
                         "CAGR%": 0, "Sharpe": 0, "Calmar": 0,
                         "Profit Factor": 0, "Max DD%": 0, "Win Rate%": 0, "Trades": 0}
            else:
                m, _, _ = compute_metrics(tdf, opt_cap, opt_start, opt_end)
                score   = float(m.get(skey, -999))
                row     = {"trial": n, **{k: tp[k] for k in space},
                           "CAGR%":         round(m["_cagr"]*100, 2),
                           "Sharpe":        round(m["_sharpe"], 3),
                           "Calmar":        round(m["_calmar"], 3),
                           "Profit Factor": round(m["_pf"], 3),
                           "Max DD%":       round(m["_maxdd"]*100, 2),
                           "Win Rate%":     float(m["Win Rate"].replace("%","")),
                           "Trades":        m["Total Trades"]}

            past.append({"params": tp, "score": score})
            results.append(row)
            if score > best_s: best_s = score; best_t = row.copy()

            if n % 3 == 0 or n == n_trials:
                tbl.dataframe(pd.DataFrame(results).sort_values(scol, ascending=False),
                              use_container_width=True, height=400)

        prog.empty()
        st.markdown(f"---\n### ✅ Done — Best {opt_metric}: **{best_s:.4f}**")

        if best_t:
            st.markdown(f"#### 🏆 Best Trial — {opt_strategy}")
            bc1, bc2, bc3, bc4 = st.columns(4)
            with bc1:
                st.markdown(mc("CAGR",   f"{best_t['CAGR%']}%"), unsafe_allow_html=True)
                st.markdown(mc("Calmar", str(best_t["Calmar"])), unsafe_allow_html=True)
            with bc2:
                st.markdown(mc("Sharpe", str(best_t["Sharpe"])), unsafe_allow_html=True)
                st.markdown(mc("Profit Factor", str(best_t["Profit Factor"])), unsafe_allow_html=True)
            with bc3:
                st.markdown(mc("Max DD",   f"{best_t['Max DD%']}%", False), unsafe_allow_html=True)
                st.markdown(mc("Win Rate", f"{best_t['Win Rate%']}%"), unsafe_allow_html=True)
            with bc4:
                st.markdown(mc("Trades", str(best_t["Trades"])), unsafe_allow_html=True)
            st.dataframe(pd.DataFrame([{k: best_t[k] for k in space}]), use_container_width=True)

        final = pd.DataFrame(results).sort_values(scol, ascending=False).reset_index(drop=True)
        final.index += 1
        st.markdown("#### All Trials")
        st.dataframe(final, use_container_width=True, height=500)
        st.download_button("⬇️ Download Results",
            final.to_csv(index=False),
            f"opt_{opt_strategy.replace(':','').replace(' ','_').lower()}_{opt_metric.lower()}.csv",
            "text/csv")
        st.caption("💡 Run again with a different target metric. Compare tables and pick your middle ground.")

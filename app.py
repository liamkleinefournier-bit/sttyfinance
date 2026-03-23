import streamlit as st
import pandas as pd
import numpy as np
import yfinance as yf
import os
from datetime import datetime
from collections import defaultdict
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
    .metric-card { background: linear-gradient(135deg, #141b2d, #1a2340); border: 1px solid #1e2d4a; border-radius: 10px; padding: 18px 22px; margin: 5px 0; }
    .metric-label { font-size: 0.7rem; color: #6b7fa3; text-transform: uppercase; letter-spacing: 0.08em; font-weight: 600; }
    .metric-value { font-size: 2rem; font-weight: 800; color: #e8eaf0; margin-top: 6px; line-height: 1; }
    .metric-value.positive { color: #00e5b3; }
    .metric-value.negative { color: #ff5555; }
    .stButton > button { background: linear-gradient(135deg, #e63946, #c1121f); color: white; border: none; border-radius: 8px; font-weight: 700; width: 100%; padding: 12px; font-size: 1rem; }
    .stButton > button:hover { opacity: 0.85; }
    div[data-testid="stExpander"] { background: #0f1623; border-radius: 8px; border: 1px solid #1e2535; }
    .info-banner { background: #0d1e3a; border-left: 4px solid #3a7bd5; padding: 12px 18px; border-radius: 6px; margin-bottom: 18px; font-size: 0.9rem; color: #a8bedf; }
    .section-header { font-size: 1.3rem; font-weight: 700; color: #e8eaf0; margin: 28px 0 14px 0; padding-bottom: 10px; border-bottom: 2px solid #1e2d4a; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# UNIVERSES
# ─────────────────────────────────────────────
NASDAQ = [
    "AAPL","MSFT","NVDA","AMZN","META","GOOGL","GOOG","TSLA","AVGO","COST",
    "NFLX","TMUS","AMD","ADBE","QCOM","PEP","AMAT","CSCO","TXN","INTU",
    "AMGN","HON","CMCSA","BKNG","INTC","VRTX","ADP","PANW","SBUX","GILD",
    "MDLZ","REGN","LRCX","MU","KLAC","SNPS","CDNS","MELI","CRWD","FTNT",
    "ABNB","MNST","ORLY","CTAS","MRVL","ADSK","PCAR","WDAY","ROST","ODFL",
    "FAST","IDXX","BIIB","DXCM","MRNA","CTSH","DDOG","EBAY","NXPI","MCHP",
    "LULU","ISRG","PYPL","CHTR","ZS","TEAM","ON","ENPH","WBD","OKTA",
    "DOCU","MTCH","ILMN","ALGN","VRSK","ANSS","FANG","CEG","GEHC","BKR",
    "TTWO","DLTR","EXC","XEL","AEP","KDP","PAYX","CPRT","SIRI","RIVN",
]

NYSE = [
    "JPM","BAC","WFC","GS","MS","BLK","C","AXP","SPGI","MCO",
    "JNJ","UNH","PFE","MRK","ABT","TMO","DHR","SYK","BSX","MDT",
    "XOM","CVX","COP","SLB","EOG","MPC","PSX","VLO","OXY","HAL",
    "BRK-B","V","MA","PG","KO","WMT","HD","MCD","NKE","DIS",
    "BA","CAT","GE","MMM","RTX","LMT","NOC","GD","DE","EMR",
    "NEE","DUK","SO","AEE","ETR","FE","PPL","EIX","D","ES",
    "AMT","PLD","CCI","EQIX","PSA","O","WELL","DLR","SPG","AVB",
    "LIN","APD","SHW","ECL","PPG","NEM","FCX","NUE","VMC","MLM",
    "UNP","UPS","FDX","CSX","NSC","DAL","UAL","AAL","LUV","JBLU",
    "T","VZ","CVS","CI","HUM","HCA","TGT","TJX","DG","DLTR",
    "LOW","BBY","KR","GM","F","LCID","ORCL","IBM","ACN","NOW",
    "PLTR","UBER","CRM","SNOW","HPQ","BX","KKR","APO","PXD","DVN",
]

def get_universe(choice):
    if choice == "Nasdaq":        return NASDAQ
    if choice == "NYSE":          return NYSE
    if choice == "Nasdaq + NYSE": return list(dict.fromkeys(NASDAQ + NYSE))
    return []


# ─────────────────────────────────────────────
# DATA — yfinance
# ─────────────────────────────────────────────
@st.cache_data(ttl=3600, show_spinner=False)
def fetch_data(tickers, start_date, end_date):
    try:
        raw = yf.download(
            tickers, start=start_date, end=end_date,
            auto_adjust=True, progress=False, threads=True
        )
        if raw.empty:
            return {}
        if len(tickers) == 1:
            raw.columns = pd.MultiIndex.from_product([raw.columns, tickers])
        result = {}
        for col in ["Open","High","Low","Close","Volume"]:
            if col in raw.columns:
                result[col] = raw[col]
        return result
    except Exception as e:
        st.error(f"Data error: {e}")
        return {}

@st.cache_data(ttl=3600, show_spinner=False)
def fetch_etf(ticker, start_date, end_date):
    try:
        df = yf.download(ticker, start=start_date, end=end_date,
                         auto_adjust=True, progress=False)
        if df.empty:
            return None
        return df["Close"].squeeze()
    except Exception:
        return None


# ─────────────────────────────────────────────
# BACKTEST ENGINE
# ─────────────────────────────────────────────
def run_backtest(data, tickers, params, initial_capital, progress_bar=None):
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

    raw   = []
    close = data.get("Close", pd.DataFrame())
    valid = [t for t in tickers if t in close.columns]
    total = len(valid)
    if total == 0:
        return pd.DataFrame()

    for idx, ticker in enumerate(valid):
        if progress_bar:
            progress_bar.progress((idx+1)/total, text=f"Scanning {ticker}...")
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
            cbh    = pd.concat([o, c], axis=1).max(axis=1)
            hb52   = cbh.rolling(bp).max()

            in_t  = False
            slvl  = entry = hbh = hba = np.nan
            age   = bsa = pyc = 0
            ed    = None

            for j in range(mb, len(c)):
                cv  = c.iloc[j]
                lv  = lo.iloc[j]
                cb  = cbh.iloc[j]
                hb  = hb52.iloc[j]
                av  = atr.iloc[j]
                v2  = sma200.iloc[j]
                v5  = sma50.iloc[j]
                vl  = vol.iloc[j]
                vm  = volma.iloc[j]
                dt  = c.index[j]

                if pd.isna(hb) or pd.isna(av) or pd.isna(v2):
                    continue

                sd   = am * av
                ok   = vm >= mav
                vs   = vl >= vm * vsm
                a2   = cv > v2
                a5   = cv > v5
                px   = cv >= mnp

                esig = cb >= hb and a2 and a5 and not in_t and vs and ok and px
                asig = in_t and a2 and cb > hba and bsa >= ms and age < ma and vl > vm and pyc < mp
                sh   = in_t and lv < slvl
                te   = in_t and age >= ma

                if esig:
                    in_t  = True
                    entry = cv
                    hbh   = cb
                    slvl  = cb - sd
                    hba   = cb
                    age   = bsa = pyc = 0
                    ed    = dt
                    raw.append({
                        "ticker": ticker, "entry_date": dt,
                        "entry_price": cv, "stop_dist": sd,
                        "rel_vol": vl/vm if vm > 0 else 1.0,
                        "exit_date": None, "exit_price": None,
                        "exit_type": None, "pyramid_adds": 0,
                        "trade_age": 0, "_qty": 0, "_eq": 0.0
                    })
                elif sh or te:
                    xp = slvl if sh else cv
                    xt = "Stop" if sh else "TimeExit"
                    for s in reversed(raw):
                        if s["ticker"] == ticker and s["exit_date"] is None:
                            s["exit_date"]    = dt
                            s["exit_price"]   = xp
                            s["exit_type"]    = xt
                            s["pyramid_adds"] = pyc
                            s["trade_age"]    = age
                            break
                    in_t  = False
                    slvl  = entry = hbh = hba = np.nan
                    age   = bsa = pyc = 0
                elif in_t:
                    age += 1; bsa += 1
                    if cb > hbh: hbh = cb
                    ns = hbh - sd
                    if ns > slvl: slvl = ns
                    if asig:
                        hba = cb; bsa = 0; pyc += 1
                    elif cb > hba:
                        hba = cb
        except Exception:
            continue

    raw = [s for s in raw if s["exit_date"] is not None]
    if not raw:
        return pd.DataFrame()

    LEVERAGE = 2.0
    raw.sort(key=lambda x: (x["entry_date"], -x.get("rel_vol", 0)))
    eq     = float(initial_capital)
    bp_    = eq * LEVERAGE
    open_  = {}
    trades = []
    taken  = set()

    all_dates = sorted(set([s["entry_date"] for s in raw] + [s["exit_date"] for s in raw]))
    by_entry  = defaultdict(list)
    by_exit   = defaultdict(list)
    for s in raw:
        by_entry[s["entry_date"]].append(s)
        by_exit[s["exit_date"]].append(s)

    for dt in all_dates:
        for s in by_exit[dt]:
            if id(s) not in taken:
                continue
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
            deployed = open_.pop(s["ticker"], 0)
            eq       = max(eq + pnl, 1.0)
            used     = sum(open_.values())
            bp_      = max(eq * LEVERAGE - used, 0.0)

        for s in by_entry[dt]:
            sd = s["stop_dist"]
            if sd <= 0 or pd.isna(sd) or s["ticker"] in open_:
                continue
            qty  = max(1, int(eq * (brp/100) / sd))
            need = qty * s["entry_price"]
            if need > bp_:
                continue
            taken.add(id(s))
            s["_qty"] = qty
            s["_eq"]  = eq
            open_[s["ticker"]] = need
            bp_ -= need

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
    rm   = equity.cummax()
    dd   = (equity - rm) / rm
    mdd  = dd.min()
    cal  = cagr/abs(mdd) if mdd != 0 else 0
    wins = df[df["pnl_dollar"] > 0]
    loss = df[df["pnl_dollar"] <= 0]
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
# DCA BENCHMARK
# ─────────────────────────────────────────────
def compute_dca(ticker, initial_capital, monthly, start_date, end_date):
    prices = fetch_etf(ticker, str(start_date), str(end_date))
    if prices is None:
        return None
    idx       = pd.date_range(start=start_date, end=end_date, freq="B")
    prices    = prices.reindex(idx).ffill().bfill()
    if prices.isna().all() or prices.iloc[0] == 0:
        return None
    shares    = initial_capital / prices.iloc[0]
    eq        = pd.Series(index=idx, dtype=float)
    eq.iloc[0]= shares * prices.iloc[0]
    last_m    = prices.index[0].month
    for i, dt in enumerate(prices.index[1:], 1):
        if dt.month != last_m:
            shares += monthly / prices.iloc[i]
            last_m  = dt.month
        eq.iloc[i] = shares * prices.iloc[i]
    return eq

def add_monthly(equity, monthly):
    if monthly <= 0:
        return equity
    eq     = equity.copy().astype(float)
    last_m = eq.index[0].month
    cum    = 0.0
    for i, dt in enumerate(eq.index[1:], 1):
        if dt.month != last_m:
            cum   += monthly
            last_m = dt.month
        eq.iloc[i] += cum
    return eq


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
    if len(past) < n_start:
        return uniform()
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
# CHARTS
# ─────────────────────────────────────────────
def chart_equity(equity, dd, qqq=None, spy=None, monthly=0):
    fig   = make_subplots(rows=2, cols=1, shared_xaxes=True, row_heights=[0.72, 0.28], vertical_spacing=0.03)
    strat = add_monthly(equity, monthly)
    fig.add_trace(go.Scatter(x=strat.index, y=strat.values, fill="tozeroy",
        fillcolor="rgba(0,229,179,0.08)", line=dict(color="#00e5b3", width=2.5), name="Strategy"), row=1, col=1)
    if qqq is not None:
        fig.add_trace(go.Scatter(x=qqq.index, y=qqq.values,
            line=dict(color="#4a90d9", width=1.5, dash="dot"), name="QQQ DCA"), row=1, col=1)
    if spy is not None:
        fig.add_trace(go.Scatter(x=spy.index, y=spy.values,
            line=dict(color="#f5a623", width=1.5, dash="dash"), name="SPY DCA"), row=1, col=1)
    fig.add_trace(go.Scatter(x=dd.index, y=dd.values*100, fill="tozeroy",
        fillcolor="rgba(255,85,85,0.25)", line=dict(color="#ff5555", width=1), name="Drawdown %"), row=2, col=1)
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=460,
        showlegend=True, legend=dict(bgcolor="rgba(10,14,23,0.9)", bordercolor="#1e2d4a", borderwidth=1, x=0.01, y=0.99))
    fig.update_xaxes(gridcolor="#141b2d")
    fig.update_yaxes(gridcolor="#141b2d")
    fig.update_yaxes(title_text="Portfolio ($)", row=1, col=1)
    fig.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
    return fig

def chart_annual(equity):
    ann    = equity.resample("YE").last().pct_change().dropna() * 100
    colors = ["#00e5b3" if v >= 0 else "#ff5555" for v in ann.values]
    fig    = go.Figure(go.Bar(x=ann.index.year, y=ann.values, marker_color=colors))
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=320,
        xaxis_title="Year", yaxis_title="Return (%)")
    fig.update_xaxes(gridcolor="#141b2d")
    fig.update_yaxes(gridcolor="#141b2d", zeroline=True, zerolinecolor="#2a3550")
    return fig

def chart_dist(df):
    fig = px.histogram(df, x="pnl_pct", nbins=60, color_discrete_sequence=["#3a7bd5"],
        labels={"pnl_pct": "Trade Return (%)"})
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=320)
    fig.update_xaxes(gridcolor="#141b2d")
    fig.update_yaxes(gridcolor="#141b2d")
    return fig

def chart_scatter(df, col):
    fig = px.scatter(df, x="trial", y=col, hover_data=df.columns.tolist(),
        color=col, color_continuous_scale="RdYlGn")
    fig.update_layout(paper_bgcolor="#0a0e17", plot_bgcolor="#0a0e17",
        font=dict(color="#e8eaf0"), margin=dict(l=0,r=0,t=10,b=0), height=350)
    fig.update_xaxes(gridcolor="#141b2d", title="Trial #")
    fig.update_yaxes(gridcolor="#141b2d")
    return fig

def mc(label, value, pos_good=True):
    neg = isinstance(value, str) and value.startswith("-")
    css = ("negative" if pos_good else "positive") if neg else ("positive" if pos_good else "negative")
    return f'<div class="metric-card"><div class="metric-label">{label}</div><div class="metric-value {css}">{value}</div></div>'


# ─────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚙️ Settings")

    with st.expander("📊 Universe & Data", expanded=True):
        universe = st.selectbox("Exchange", ["Nasdaq + NYSE", "Nasdaq", "NYSE", "Custom"])
        custom_tickers = []
        if universe == "Custom":
            raw_input = st.text_area("Tickers (comma-separated)", "AAPL, MSFT, NVDA")
            custom_tickers = [t.strip().upper() for t in raw_input.split(",") if t.strip()]
        c1, c2 = st.columns(2)
        with c1:
            start_date = st.date_input("Start", datetime(2005, 1, 1))
        with c2:
            end_date = st.date_input("End", datetime.today())
        initial_capital      = st.number_input("Capital ($)", min_value=1000, value=10000, step=1000)
        monthly_contribution = st.number_input("Monthly Add ($)", min_value=0, value=500, step=100)

    with st.expander("🎯 Strategy", expanded=True):
        breakout_period = st.number_input("52W Lookback", 10, 504, 220)
        atr_period      = st.number_input("ATR Period", 1, 500, 125)
        atr_mult        = st.number_input("ATR Multiplier", 0.1, 20.0, 7.0, step=0.05)
        max_pyramid     = st.number_input("Max Pyramid Adds", 0, 20, 5)
        min_spacing     = st.number_input("Min Bars Between Adds", 1, 100, 19)
        max_age         = st.number_input("Max Trade Age (bars)", 10, 2000, 550)
        base_risk_pct   = st.number_input("Risk % per Trade", 0.1, 10.0, 2.75, step=0.25)

    with st.expander("📈 Moving Averages", expanded=True):
        sma200 = st.number_input("Trend SMA Period", 1, 500, 200)
        sma50  = st.number_input("Context SMA Period", 1, 300, 70)
        st.caption("Entry only when price is above both SMAs.")

    with st.expander("🔊 Volume"):
        vol_ma_period  = st.number_input("Volume MA Period", 5, 100, 20)
        vol_spike_mult = st.number_input("Volume Spike Multiplier", 1.0, 5.0, 1.4, step=0.1)
        min_avg_vol    = st.number_input("Min Avg Volume", 0, 10_000_000, 1_000_000, step=100_000)

    with st.expander("🔍 Price"):
        min_price = st.number_input("Min Price ($)", 0.0, 500.0, 10.0, step=0.5)

    st.markdown("---")
    run_btn = st.button("🚀 Run Backtest")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
st.markdown("# 📈 Stock Trend Trader")
st.markdown("*52W Body Breakout · ATR Trailing Stop · Compounding · Nasdaq + NYSE*")
st.markdown("---")

tab1, tab2 = st.tabs(["📊 Backtest", "🔬 Optimization"])


# ══════════════════════
# TAB 1 — BACKTEST
# ══════════════════════
with tab1:
    if not run_btn:
        st.markdown("""<div class="info-banner">
        Configure parameters in the sidebar and click <strong>Run Backtest</strong>.<br>
        Data is pulled live from Yahoo Finance. Defaults are set to your optimized median settings.
        </div>""", unsafe_allow_html=True)
        c1, c2, c3 = st.columns(3)
        with c1:
            st.markdown("""**Strategy:**
- 52W candle body breakout
- Above both SMAs
- Volume spike confirmation
- ATR trailing stop
- Pyramid adds on new highs
- 2:1 leverage, compounding""")
        with c2:
            st.markdown("""**Metrics:**
- CAGR — yearly return
- Sharpe — return / risk
- Calmar — CAGR / max DD
- Profit Factor — wins / losses
- Expectancy — avg per trade""")
        with c3:
            st.markdown("""**Median settings:**
- 52W: 220 | ATR: 125 / 7.0x
- Max Pyramid: 5 | Spacing: 19
- SMA: 200 / 70
- Vol Spike: 1.4x | Risk: 2.75%""")
    else:
        params = {
            "breakout_period": breakout_period, "atr_period": atr_period,
            "atr_mult": atr_mult, "min_spacing": min_spacing,
            "max_age": max_age, "max_pyramid": max_pyramid,
            "sma200": sma200, "sma50": sma50,
            "vol_ma_period": vol_ma_period, "vol_spike_mult": vol_spike_mult,
            "min_avg_vol": min_avg_vol, "base_risk_pct": base_risk_pct,
            "min_price": min_price,
        }

        tickers = custom_tickers if universe == "Custom" else get_universe(universe)
        if not tickers:
            st.error("No tickers loaded.")
            st.stop()

        st.info(f"Universe: **{len(tickers)} tickers** | Period: **{start_date}** to **{end_date}**")

        with st.spinner(f"Downloading data for {len(tickers)} tickers from Yahoo Finance..."):
            data = fetch_data(tuple(tickers), str(start_date), str(end_date))

        if not data:
            st.error("No data returned from Yahoo Finance.")
            st.stop()

        prog   = st.progress(0, text="Running backtest...")
        trades = run_backtest(data, tickers, params, initial_capital, prog)
        prog.empty()

        if trades.empty:
            st.warning("No trades generated. Try a longer date range or looser filters.")
            st.stop()

        metrics, equity, drawdown = compute_metrics(trades, initial_capital, start_date, end_date)

        st.markdown('<div class="section-header">📊 Performance Overview</div>', unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(mc("CAGR", metrics["CAGR"]), unsafe_allow_html=True)
            st.markdown(mc("Total Return", metrics["Total Return"]), unsafe_allow_html=True)
        with c2:
            st.markdown(mc("Sharpe", metrics["Sharpe"]), unsafe_allow_html=True)
            st.markdown(mc("Calmar", metrics["Calmar"]), unsafe_allow_html=True)
        with c3:
            st.markdown(mc("Max Drawdown", metrics["Max Drawdown"], False), unsafe_allow_html=True)
            st.markdown(mc("Sortino", metrics["Sortino"]), unsafe_allow_html=True)
        with c4:
            st.markdown(mc("Total Trades", str(metrics["Total Trades"])), unsafe_allow_html=True)
            st.markdown(mc("Profit Factor", metrics["Profit Factor"]), unsafe_allow_html=True)

        c1, c2, c3, c4 = st.columns(4)
        with c1:
            st.markdown(mc("Win Rate", metrics["Win Rate"]), unsafe_allow_html=True)
        with c2:
            st.markdown(mc("Avg Win", metrics["Avg Win"]), unsafe_allow_html=True)
        with c3:
            st.markdown(mc("Avg Loss", metrics["Avg Loss"], False), unsafe_allow_html=True)
        with c4:
            st.markdown(mc("Expectancy", metrics["Expectancy"]), unsafe_allow_html=True)

        st.markdown("---")
        t1, t2, t3, t4 = st.tabs(["📈 Equity Curve", "📅 Annual Returns", "📦 Distribution", "📋 Trade Log"])

        with t1:
            with st.spinner("Loading QQQ and SPY benchmarks..."):
                qqq = compute_dca("QQQ", initial_capital, monthly_contribution, start_date, end_date)
                spy = compute_dca("SPY", initial_capital, monthly_contribution, start_date, end_date)
            st.plotly_chart(chart_equity(equity, drawdown, qqq, spy, monthly_contribution), use_container_width=True)
            n_months = (pd.to_datetime(end_date) - pd.to_datetime(start_date)).days / 30.44
            total_in = initial_capital + monthly_contribution * n_months
            bc1, bc2, bc3 = st.columns(3)
            strat_final = add_monthly(equity, monthly_contribution).iloc[-1]
            with bc1:
                st.markdown(mc("Strategy Final", f"${strat_final:,.0f}"), unsafe_allow_html=True)
            with bc2:
                st.markdown(mc("QQQ Final", f"${qqq.iloc[-1]:,.0f}" if qqq is not None else "No data"), unsafe_allow_html=True)
            with bc3:
                st.markdown(mc("SPY Final", f"${spy.iloc[-1]:,.0f}" if spy is not None else "No data"), unsafe_allow_html=True)
            st.caption(f"Total contributed: **${total_in:,.0f}** (${initial_capital:,} initial + ${monthly_contribution:,}/month)")

        with t2:
            st.plotly_chart(chart_annual(equity), use_container_width=True)
            ann = equity.resample("YE").last().pct_change().dropna()
            c1, c2, c3 = st.columns(3)
            with c1: st.markdown(mc("Winning Years", str((ann > 0).sum())), unsafe_allow_html=True)
            with c2: st.markdown(mc("Losing Years",  str((ann <= 0).sum()), False), unsafe_allow_html=True)
            with c3: st.markdown(mc("Avg Year", f"{ann.mean()*100:.2f}%"), unsafe_allow_html=True)

        with t3:
            st.plotly_chart(chart_dist(trades), use_container_width=True)

        with t4:
            cols = ["ticker","entry_date","exit_date","entry_price","exit_price",
                    "pnl_pct","pnl_dollar","exit_type","pyramid_adds","trade_age","equity_at_entry"]
            ddf = trades[cols].copy()
            for c in ["pnl_pct","pnl_dollar","entry_price","exit_price"]:
                ddf[c] = ddf[c].round(2)
            st.dataframe(ddf, use_container_width=True, height=420)
            st.download_button("⬇️ Download Trade Log", ddf.to_csv(index=False), "trades.csv", "text/csv")

        st.markdown("---")
        st.markdown('<div class="section-header">⚙️ Parameters This Run</div>', unsafe_allow_html=True)
        st.dataframe(pd.DataFrame([{
            "52W": breakout_period, "ATR Per": atr_period, "ATR Mult": atr_mult,
            "MaxPyr": max_pyramid, "MinSpace": min_spacing, "MaxAge": max_age,
            "Risk%": base_risk_pct, "SMA Trend": sma200, "SMA Context": sma50,
            "VolSpike": vol_spike_mult, "CAGR": metrics["CAGR"],
            "Sharpe": metrics["Sharpe"], "Calmar": metrics["Calmar"],
            "MaxDD": metrics["Max Drawdown"], "PF": metrics["Profit Factor"],
        }]), use_container_width=True)


# ══════════════════════
# TAB 2 — OPTIMIZATION
# ══════════════════════
with tab2:
    st.markdown("### 🔬 Parameter Optimization")
    st.markdown("""<div class="info-banner">
    Set search ranges. The optimizer uses Bayesian-style directed search.
    Every trial is shown in the table. Run multiple times with different metrics then find your middle ground manually.
    </div>""", unsafe_allow_html=True)

    oc1, oc2 = st.columns(2)
    with oc1: opt_metric = st.selectbox("Optimize For", ["Calmar","Sharpe","CAGR","Profit Factor"])
    with oc2: n_trials   = st.number_input("Trials", 5, 200, 30, step=5)

    opt_universe = st.selectbox("Universe", ["Nasdaq + NYSE","Nasdaq","NYSE","Custom"], key="ou")
    custom_opt   = []
    if opt_universe == "Custom":
        ci = st.text_area("Tickers", "AAPL, MSFT, NVDA", key="ct")
        custom_opt = [t.strip().upper() for t in ci.split(",") if t.strip()]

    st.markdown("#### Search Ranges")
    sr1, sr2 = st.columns(2)
    with sr1:
        bp_r = st.slider("52W Lookback",         50,   504,  (150, 320), step=10)
        ap_r = st.slider("ATR Period",            10,   400,  (80,  200), step=5)
        am_r = st.slider("ATR Multiplier",        2.0,  15.0, (5.0, 10.0), step=0.5)
        mp_r = st.slider("Max Pyramid Adds",      0,    15,   (2, 8),    step=1)
        ms_r = st.slider("Min Bars Between Adds", 5,    60,   (10, 25),  step=1)
        ma_r = st.slider("Max Trade Age",         100,  2000, (300, 800), step=50)
    with sr2:
        s2_r = st.slider("Trend SMA Period",      100,  300,  (150, 250), step=10)
        s5_r = st.slider("Context SMA Period",    20,   100,  (40, 80),  step=5)
        vs_r = st.slider("Vol Spike Multiplier",  1.0,  3.0,  (1.1, 2.0), step=0.1)
        br_r = st.slider("Base Risk %",           0.5,  5.0,  (1.0, 3.5), step=0.25)

    oc3, oc4, oc5 = st.columns(3)
    with oc3: opt_start = st.date_input("Start", datetime(2005, 1, 1), key="os")
    with oc4: opt_end   = st.date_input("End",   datetime.today(),     key="oe")
    with oc5: opt_cap   = st.number_input("Capital ($)", 1000, 10000000, 10000, step=1000, key="oc")

    run_opt = st.button("🚀 Run Optimization")

    if run_opt:
        opt_tickers = custom_opt if opt_universe == "Custom" else get_universe(opt_universe)
        if not opt_tickers:
            st.error("No tickers.")
            st.stop()

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
        fixed = {"vol_ma_period": vol_ma_period, "min_avg_vol": min_avg_vol, "min_price": min_price}

        with st.spinner(f"Downloading data for {len(opt_tickers)} tickers..."):
            opt_data = fetch_data(tuple(opt_tickers), str(opt_start), str(opt_end))

        if not opt_data:
            st.error("No data loaded.")
            st.stop()

        results = []
        past    = []
        best_s  = -np.inf
        best_t  = None
        tbl     = st.empty()
        prog    = st.progress(0, text="Starting...")

        for n in range(1, n_trials+1):
            prog.progress(n/n_trials, text=f"Trial {n}/{n_trials} | Best {opt_metric}: {best_s:.4f}")
            tp  = suggest(space, past)
            fp  = {**tp, **fixed}
            tdf = run_backtest(opt_data, opt_tickers, fp, opt_cap)

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
            if score > best_s:
                best_s = score
                best_t = row.copy()

            if n % 3 == 0 or n == n_trials:
                tbl.dataframe(pd.DataFrame(results).sort_values(scol, ascending=False),
                              use_container_width=True, height=400)

        prog.empty()
        st.markdown(f"---\n### ✅ Done — Best {opt_metric}: **{best_s:.4f}**")

        if best_t:
            st.markdown("#### 🏆 Best Trial")
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
        st.plotly_chart(chart_scatter(pd.DataFrame(results), scol), use_container_width=True)
        st.download_button("⬇️ Download Results",
            final.to_csv(index=False),
            f"opt_{opt_metric.lower().replace(' ','_')}.csv", "text/csv")
        st.caption("💡 Run again with a different target metric. Compare tables and pick your middle ground.")

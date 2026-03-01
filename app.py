"""
Portfolio Analyzer v2 — Streamlit Web App
Run from mobile or browser via Streamlit Cloud.
"""

import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import time
import sys
import io
import warnings
warnings.filterwarnings("ignore")

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="📊 Portfolio Analyzer v2",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Import the core logic from the original script ────────────────────────────
# We import only what we need — all the heavy lifting stays in portfolio_analyzer_v2.py
from portfolio_analyzer_v2 import (
    PORTFOLIO_TICKERS,
    fetch_all_parallel,
    compute_sector_medians,
    calculate_weighted_score,
    get_recommendation,
    assign_composite_flag,
    generate_top10_recommendations,
    _is_nan,
)

# ── Custom CSS (dark theme matching original HTML report) ─────────────────────
st.markdown("""
<style>
    .main { background-color: #0d1117; }
    .stApp { background-color: #0d1117; color: #c9d1d9; }
    
    /* Metric cards */
    [data-testid="metric-container"] {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 10px;
    }
    
    /* Score badges */
    .badge-strong { background:#1f6feb; color:#fff; padding:3px 8px; border-radius:4px; font-weight:700; font-size:12px; }
    .badge-buy    { background:#238636; color:#fff; padding:3px 8px; border-radius:4px; font-weight:700; font-size:12px; }
    .badge-hold   { background:#9e6a03; color:#fff; padding:3px 8px; border-radius:4px; font-weight:700; font-size:12px; }
    .badge-sell   { background:#b62324; color:#fff; padding:3px 8px; border-radius:4px; font-weight:700; font-size:12px; }
    
    /* Progress bar color */
    .stProgress > div > div { background-color: #1f6feb; }
    
    /* Sidebar */
    [data-testid="stSidebar"] { background-color: #161b22; }
    
    /* Buttons */
    .stButton > button {
        background: #238636;
        color: white;
        border: none;
        border-radius: 6px;
        font-weight: 600;
    }
    .stButton > button:hover { background: #2ea043; }

    /* Table styling */
    .dataframe thead th {
        background-color: #161b22 !important;
        color: #8b949e !important;
    }
    .dataframe tbody tr:hover { background-color: #1f2937 !important; }
</style>
""", unsafe_allow_html=True)


# ── Helper: colour a score ────────────────────────────────────────────────────
def score_badge(score):
    if score is None or (isinstance(score, float) and np.isnan(score)):
        return "-"
    if score >= 78:
        return f'<span class="badge-strong">{score}</span>'
    if score >= 62:
        return f'<span class="badge-buy">{score}</span>'
    if score >= 44:
        return f'<span class="badge-hold">{score}</span>'
    return f'<span class="badge-sell">{score}</span>'


def action_badge(action):
    colors = {
        "STRONG BUY": "badge-strong",
        "BUY": "badge-buy",
        "HOLD": "badge-hold",
        "SELL": "badge-sell",
    }
    cls = colors.get(action, "badge-hold")
    return f'<span class="{cls}">{action}</span>'


def fmt_pct(v, plus=True):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-"
    sign = "+" if plus and v > 0 else ""
    return f"{sign}{v:.1f}%"


def fmt_price(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return "-"
    return f"${v:.2f}"


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 Portfolio Analyzer")
    st.caption("v2 — 344 Stocks · Weighted · Sector-Relative")
    st.divider()

    st.subheader("⚙️ Run Analysis")
    workers = st.slider("Parallel workers", 4, 16, 8, help="Higher = faster but more rate-limit risk")
    top_n   = st.slider("Top N picks", 5, 20, 10)

    run_btn = st.button("🚀 Run Analysis Now", use_container_width=True)
    st.caption(f"Will screen {len(PORTFOLIO_TICKERS)} stocks")

    st.divider()
    st.subheader("🔍 Filters")
    filter_action  = st.multiselect("Action", ["STRONG BUY", "BUY", "HOLD", "SELL"],
                                    default=["STRONG BUY", "BUY"])
    filter_sectors = st.multiselect("Sectors", [], key="sector_filter")
    min_score      = st.slider("Min Score", 0, 100, 60)
    max_de         = st.slider("Max Debt/Equity", 0.0, 10.0, 3.0, step=0.5)
    only_below_ma  = st.checkbox("Only stocks below 200 DMA")

    st.divider()
    st.caption("⚠️ This is a screening tool, not financial advice. Always do your own research.")


# ── Main area ─────────────────────────────────────────────────────────────────
st.title("📊 Stock Portfolio Analyzer v2")
st.caption("344 stocks · Sector-relative scoring · Analyst consensus targets")

# ── Session state ─────────────────────────────────────────────────────────────
if "df" not in st.session_state:
    st.session_state.df = None
if "top10" not in st.session_state:
    st.session_state.top10 = None
if "run_ts" not in st.session_state:
    st.session_state.run_ts = None


# ── Run analysis ──────────────────────────────────────────────────────────────
if run_btn:
    st.session_state.df = None
    st.session_state.top10 = None

    progress_bar  = st.progress(0, text="Starting...")
    status_text   = st.empty()
    log_container = st.empty()

    total_tickers = len(PORTFOLIO_TICKERS)

    with st.spinner("Fetching market data for 344 stocks (this takes ~3-6 minutes)..."):

        # ── Pass 1: fetch all ──────────────────────────────────────────────
        status_text.info("📡 Pass 1: Fetching all tickers...")
        progress_bar.progress(0.05, text="Fetching data (pass 1)...")

        records = fetch_all_parallel(PORTFOLIO_TICKERS, max_workers=workers)
        progress_bar.progress(0.55, text="Pass 1 complete. Checking for empty records...")

        # ── Pass 2: retry empties ──────────────────────────────────────────
        empty_tickers = [r["Ticker"] for r in records if r.get("Price") is None]
        if empty_tickers:
            status_text.info(f"🔄 Pass 2: Retrying {len(empty_tickers)} tickers...")
            progress_bar.progress(0.60, text=f"Retrying {len(empty_tickers)} tickers...")
            time.sleep(3)
            retry_records = fetch_all_parallel(empty_tickers, max_workers=max(workers // 2, 4))
            retry_map = {r["Ticker"]: r for r in retry_records if r.get("Price") is not None}
            records = [retry_map.get(r["Ticker"], r) for r in records]

        # ── Compute sector medians ─────────────────────────────────────────
        progress_bar.progress(0.70, text="Computing sector medians...")
        df = pd.DataFrame(records)
        sector_medians = compute_sector_medians(df)

        # ── Score ──────────────────────────────────────────────────────────
        progress_bar.progress(0.80, text="Scoring stocks...")
        df["Score"]  = df.apply(lambda r: calculate_weighted_score(r, sector_medians), axis=1)
        df = df[df["Score"].notna()].copy()
        df["Action"] = df["Score"].apply(lambda s: get_recommendation(s)[0])
        df["Target"] = df["Analyst_Target"]
        df["Upside"]  = df["Analyst_Upside"]
        df["Composite_Flag"] = df.apply(assign_composite_flag, axis=1)

        df.sort_values("Score", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # ── Top N picks ────────────────────────────────────────────────────
        progress_bar.progress(0.92, text="Generating top picks...")
        top10 = generate_top10_recommendations(df, n=top_n)

        st.session_state.df     = df
        st.session_state.top10  = top10
        st.session_state.run_ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        progress_bar.progress(1.0, text="✅ Done!")
        time.sleep(0.5)
        progress_bar.empty()
        status_text.empty()
        st.success(f"✅ Analysis complete — {len(df)} stocks scored at {st.session_state.run_ts}")


# ── Display results ───────────────────────────────────────────────────────────
df = st.session_state.df

if df is None:
    st.info("👈 Click **Run Analysis Now** in the sidebar to fetch live data and score all 344 stocks.")
    st.markdown("""
    ### What this tool does
    - Fetches live data from Yahoo Finance for 344 stocks
    - Scores each stock on 20+ weighted factors (FCF yield, ROE, revenue growth, valuation, etc.)
    - Applies sector-relative scoring (P/E vs sector median, not absolute)
    - Uses real Wall Street analyst consensus price targets
    - Generates a ranked table + Top 10 deep-dive report

    ### How to share / update
    - This app is hosted on **Streamlit Cloud** — share the URL with anyone
    - To re-run anytime, just click **Run Analysis Now** again
    - All data is fetched fresh each time (no stale cache)
    """)
    st.stop()


# ── Summary cards ─────────────────────────────────────────────────────────────
st.caption(f"Last run: {st.session_state.run_ts}")

col1, col2, col3, col4, col5, col6 = st.columns(6)
col1.metric("Total Stocks", len(df))
col2.metric("⭐ Strong Buy", int((df["Action"] == "STRONG BUY").sum()))
col3.metric("🟢 Buy",        int((df["Action"] == "BUY").sum()))
col4.metric("🟡 Hold",       int((df["Action"] == "HOLD").sum()))
col5.metric("🔴 Sell",       int((df["Action"] == "SELL").sum()))
col6.metric("Avg Score",    f"{df['Score'].mean():.1f}")


# ── Update sidebar sector filter dynamically ───────────────────────────────────
sectors_available = sorted(df["Sector"].dropna().unique().tolist())


# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3 = st.tabs(["📋 Full Screener", "⭐ Top Picks", "📥 Export"])

# ── TAB 1: Full Screener ──────────────────────────────────────────────────────
with tab1:
    # Apply filters
    mask = pd.Series([True] * len(df), index=df.index)

    if filter_action:
        mask &= df["Action"].isin(filter_action)

    chosen_sectors = st.session_state.get("sector_filter", [])
    if chosen_sectors:
        mask &= df["Sector"].isin(chosen_sectors)

    mask &= df["Score"] >= min_score

    de_mask = df["Debt_Equity"].isna() | (df["Debt_Equity"] <= max_de)
    mask &= de_mask

    if only_below_ma:
        mask &= df["Vs_MA200"].notna() & (df["Vs_MA200"] < 0)

    dff = df[mask].copy()
    st.caption(f"Showing {len(dff)} of {len(df)} stocks after filters")

    # Search box
    search = st.text_input("🔍 Search ticker or name", "")
    if search:
        s = search.upper()
        dff = dff[
            dff["Ticker"].str.upper().str.contains(s, na=False) |
            dff["Name"].astype(str).str.upper().str.contains(s, na=False)
        ]

    # Build display table
    display_cols = {
        "Ticker": "Ticker",
        "Name": "Name",
        "Sector": "Sector",
        "Score": "Score",
        "Action": "Action",
        "Composite_Flag": "Flags",
        "Price": "Price",
        "Analyst_Target": "Target",
        "Analyst_Upside": "Upside%",
        "Mkt_Cap": "Mkt Cap",
        "ROE": "ROE%",
        "Rev_Growth": "Rev Gr%",
        "FCF_Yield": "FCF Yld%",
        "Gross_Margin": "Gross Mgn%",
        "Op_Margin": "Op Mgn%",
        "PE_Fwd": "P/E Fwd",
        "PS": "P/S",
        "PEG": "PEG",
        "EV_EBITDA": "EV/EBITDA",
        "Debt_Equity": "D/E",
        "Beta": "Beta",
        "Short_Float": "Short%",
        "Div_Yield": "Div Yld%",
        "Vs_MA200": "Vs 200MA%",
        "EPS_Growth": "EPS Gr%",
        "EPS_Surprise": "EPS Surp%",
        "From_Low_Pct": "From Low%",
        "From_High_Pct": "From High%",
    }

    show_cols = [c for c in display_cols if c in dff.columns]
    tbl = dff[show_cols].rename(columns=display_cols).reset_index(drop=True)

    # Format numeric columns
    for col in ["Price", "Target"]:
        if col in tbl.columns:
            tbl[col] = tbl[col].apply(lambda v: fmt_price(v) if pd.notna(v) else "-")

    for col in ["Upside%", "ROE%", "Rev Gr%", "FCF Yld%", "Gross Mgn%", "Op Mgn%",
                "Short%", "Div Yld%", "Vs 200MA%", "EPS Gr%", "EPS Surp%",
                "From Low%", "From High%"]:
        if col in tbl.columns:
            tbl[col] = tbl[col].apply(lambda v: fmt_pct(v) if pd.notna(v) else "-")

    for col in ["P/E Fwd", "P/S", "PEG", "EV/EBITDA", "D/E", "Beta", "Score"]:
        if col in tbl.columns:
            tbl[col] = tbl[col].apply(lambda v: f"{v:.1f}" if pd.notna(v) and v is not None else "-")

    st.dataframe(
        tbl,
        use_container_width=True,
        height=600,
        column_config={
            "Score": st.column_config.NumberColumn("Score", format="%.1f"),
            "Action": st.column_config.TextColumn("Action"),
        }
    )


# ── TAB 2: Top Picks ──────────────────────────────────────────────────────────
with tab2:
    top10 = st.session_state.top10
    if not top10:
        st.info("Run the analysis first to see top picks.")
    else:
        st.subheader(f"🏆 Top {len(top10)} Recommendations")
        st.caption("Multi-factor conviction scoring · Sector-diversified · Risk-filtered")

        for i, r in enumerate(top10, 1):
            upside  = r.get("upside")
            roe     = r.get("roe")
            fcf     = r.get("fcf")
            de      = r.get("de")
            beta    = r.get("beta")
            rev_g   = r.get("rev_g")
            op_m    = r.get("op_m")
            gross_m = r.get("gross_m")
            eps_s   = r.get("eps_s")
            accel   = None
            rev_prev = r.get("rev_prev")
            if rev_g is not None and rev_prev is not None:
                accel = rev_g - rev_prev

            with st.expander(
                f"#{i}  {r['ticker']}  —  {r['name'][:50]}  |  Score: {r['base_score']}  |  {r.get('action','')}",
                expanded=(i <= 3),
            ):
                c1, c2, c3 = st.columns(3)

                with c1:
                    st.markdown("**💰 Price & Target**")
                    st.write(f"Current Price: **{fmt_price(r.get('price'))}**")
                    st.write(f"Analyst Target: **{fmt_price(r.get('target'))}**  ({r.get('analyst_count') or 'N/A'} analysts)")
                    upside_str = fmt_pct(upside) if upside is not None else "N/A"
                    color = "green" if upside and upside > 0 else "red"
                    st.markdown(f"Upside: **:{color}[{upside_str}]**")
                    st.write(f"Sector: {r.get('sector')}")
                    st.write(f"Mkt Cap: {r.get('mkt_cap','N/A')}")

                with c2:
                    st.markdown("**📊 Quality & Profitability**")
                    st.write(f"ROE:          {fmt_pct(roe, plus=False)}")
                    st.write(f"FCF Yield:    {fmt_pct(fcf, plus=False)}")
                    st.write(f"Operating Mgn:{fmt_pct(op_m, plus=False)}")
                    st.write(f"Gross Margin: {fmt_pct(gross_m, plus=False)}")

                with c3:
                    st.markdown("**🚀 Growth & Risk**")
                    accel_str = f"  ({'▲' if accel and accel > 0 else '▼'} {abs(accel):.1f}pp)" if accel is not None else ""
                    st.write(f"Rev Growth:  {fmt_pct(rev_g)}{accel_str}")
                    st.write(f"EPS Surprise: {fmt_pct(eps_s) if eps_s is not None else 'N/A'}")
                    st.write(f"Debt/Equity: {f'{de:.2f}' if de is not None else 'N/A'}")
                    st.write(f"Beta:        {f'{beta:.2f}' if beta is not None else 'N/A'}")

                flags = r.get("flags", "")
                if flags and flags != "—":
                    st.markdown(f"**🏷️ Signal Flags:** {flags}")

                st.caption(f"Base Score: {r['base_score']}  |  Conviction Score: {r['conv_score']}")


# ── TAB 3: Export ─────────────────────────────────────────────────────────────
with tab3:
    st.subheader("📥 Export Results")

    if df is None:
        st.info("Run analysis first.")
    else:
        # CSV export
        csv_cols = [
            "Ticker", "Name", "Sector", "Score", "Action", "Composite_Flag",
            "Price", "Analyst_Target", "Analyst_Upside", "Mkt_Cap",
            "PE_Fwd", "PS", "PB", "PEG", "EV_EBITDA",
            "ROE", "Rev_Growth", "Rev_Growth_Prev", "Gross_Margin",
            "Op_Margin", "Profit_Margin", "FCF_Yield",
            "EPS_Growth", "EPS_Surprise",
            "From_Low_Pct", "From_High_Pct", "Debt_Equity",
            "Beta", "Short_Float", "Inst_Own", "Insider_Buy_Pct",
            "Div_Yield", "Payout_Ratio", "ROA", "Current_Ratio",
            "MA200", "Vs_MA200", "Analyst_Count",
        ]
        available_cols = [c for c in csv_cols if c in df.columns]
        csv_data = df[available_cols].to_csv(index=False).encode("utf-8")

        st.download_button(
            label="⬇️ Download Full CSV",
            data=csv_data,
            file_name=f"portfolio_analysis_{datetime.now().strftime('%Y%m%d_%H%M')}.csv",
            mime="text/csv",
            use_container_width=True,
        )

        # Top 10 text export
        if st.session_state.top10:
            top10_lines = []
            for i, r in enumerate(st.session_state.top10, 1):
                top10_lines.append(f"#{i} {r['ticker']} — {r['name']}")
                top10_lines.append(f"   Score: {r['base_score']} | Action: {r.get('action')}")
                top10_lines.append(f"   Price: {fmt_price(r.get('price'))} | Target: {fmt_price(r.get('target'))} | Upside: {fmt_pct(r.get('upside'))}")
                top10_lines.append(f"   ROE: {fmt_pct(r.get('roe'), plus=False)} | FCF: {fmt_pct(r.get('fcf'), plus=False)} | Rev Gr: {fmt_pct(r.get('rev_g'))}")
                top10_lines.append(f"   Flags: {r.get('flags','—')}")
                top10_lines.append("")

            top10_txt = "\n".join(top10_lines).encode("utf-8")
            st.download_button(
                label="⬇️ Download Top Picks TXT",
                data=top10_txt,
                file_name=f"top_picks_{datetime.now().strftime('%Y%m%d_%H%M')}.txt",
                mime="text/plain",
                use_container_width=True,
            )

        st.markdown("---")
        st.caption("⚠️ All data sourced from Yahoo Finance via yfinance. This is a screening tool only — not financial advice.")

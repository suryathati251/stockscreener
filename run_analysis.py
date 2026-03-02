#!/usr/bin/env python3
"""
run_analysis.py — Headless cron runner.
Saves results to data/ folder which the Streamlit app reads instantly.

Run manually:    python run_analysis.py
Cron (10am):     0 10 * * 1-5 /path/to/venv/bin/python /path/to/run_analysis.py >> /path/to/cron.log 2>&1
"""
import os, json, time, warnings
import pandas as pd
import numpy as np
from datetime import datetime, timezone
warnings.filterwarnings("ignore")

from portfolio_analyzer_v2 import (
    PORTFOLIO_TICKERS, fetch_all_parallel, compute_sector_medians,
    calculate_weighted_score, get_recommendation, assign_composite_flag,
    generate_top10_recommendations,
)

def _clean(v):
    if v is None: return None
    if isinstance(v, float) and np.isnan(v): return None
    if isinstance(v, np.integer): return int(v)
    if isinstance(v, np.floating): return None if np.isnan(v) else float(v)
    return v

def main():
    os.makedirs("data", exist_ok=True)
    start = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  PORTFOLIO ANALYZER v2 — Cron Run")
    print(f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Stocks  : {len(PORTFOLIO_TICKERS)}")
    print(f"{'='*60}\n")

    print("Pass 1: Fetching all tickers (8 workers)...")
    records = fetch_all_parallel(PORTFOLIO_TICKERS, max_workers=8)

    empty = [r["Ticker"] for r in records if r.get("Price") is None]
    if empty:
        print(f"Retrying {len(empty)} empty tickers (pass 2)...")
        time.sleep(5)
        retry = fetch_all_parallel(empty, max_workers=4)
        rmap  = {r["Ticker"]: r for r in retry if r.get("Price") is not None}
        records = [rmap.get(r["Ticker"], r) for r in records]
        print(f"  Recovered: {len(rmap)}  |  Still empty: {len(empty)-len(rmap)}")

    print("\nScoring...")
    df = pd.DataFrame(records)
    sm = compute_sector_medians(df)
    df["Score"]  = df.apply(lambda r: calculate_weighted_score(r, sm), axis=1)
    df = df[df["Score"].notna()].copy()
    df["Action"] = df["Score"].apply(lambda s: get_recommendation(s)[0])
    df["Composite_Flag"] = df.apply(assign_composite_flag, axis=1)
    df.sort_values("Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    cols = [
        "Ticker","Name","Sector","Industry","Score","Action","Composite_Flag",
        "Price","Analyst_Target","Analyst_Upside","Analyst_Count","Mkt_Cap",
        "PE_Fwd","PS","PB","PEG","EV_EBITDA","ROE",
        "Rev_Growth","Rev_Growth_Prev","Gross_Margin","Op_Margin","Profit_Margin","FCF_Yield",
        "EPS_Growth","EPS_Surprise","From_Low_Pct","From_High_Pct","Debt_Equity",
        "Beta","Short_Float","Inst_Own","Insider_Buy_Pct",
        "Div_Yield","Payout_Ratio","ROA","Current_Ratio","MA200","Vs_MA200",
    ]
    df[[c for c in cols if c in df.columns]].to_csv("data/portfolio_analysis.csv", index=False)
    print(f"  ✅ data/portfolio_analysis.csv  ({len(df)} rows)")

    top10 = generate_top10_recommendations(df, n=10)
    with open("data/top10.json","w") as f:
        json.dump([{k:_clean(v) for k,v in r.items()} for r in top10], f, indent=2)
    print(f"  ✅ data/top10.json  ({len(top10)} picks)")

    end = datetime.now(timezone.utc)
    info = {
        "run_timestamp_utc": end.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "elapsed_minutes": round((end-start).total_seconds()/60, 1),
        "total_stocks": len(df),
        "strong_buy": int((df["Action"]=="STRONG BUY").sum()),
        "buy":        int((df["Action"]=="BUY").sum()),
        "hold":       int((df["Action"]=="HOLD").sum()),
        "sell":       int((df["Action"]=="SELL").sum()),
        "avg_score":  round(float(df["Score"].mean()),1),
    }
    with open("data/run_info.json","w") as f:
        json.dump(info, f, indent=2)
    print(f"  ✅ data/run_info.json")
    print(f"\n  Done in {info['elapsed_minutes']} min | STRONG BUY:{info['strong_buy']} | BUY:{info['buy']} | HOLD:{info['hold']} | SELL:{info['sell']}\n")

if __name__ == "__main__":
    main()

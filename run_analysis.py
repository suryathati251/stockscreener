#!/usr/bin/env python3
"""
run_analysis.py — Headless GitHub Actions runner.
Fetches in small batches with pauses to avoid yfinance rate limits.
Saves results to data/ folder for Streamlit app to read instantly.
"""
import os, json, time, warnings, sys
import pandas as pd
import numpy as np
from datetime import datetime, timezone
warnings.filterwarnings("ignore")

from portfolio_analyzer_v2 import (
    PORTFOLIO_TICKERS, fetch_all_parallel, compute_sector_medians,
    calculate_weighted_score, get_recommendation, assign_composite_flag,
    generate_top10_recommendations, _empty_row,
)

BATCH_SIZE   = 50    # fetch 50 tickers at a time
BATCH_PAUSE  = 20    # seconds to wait between batches (avoids rate limits)
MAX_WORKERS  = 5     # conservative parallelism for GitHub Actions

def _clean(v):
    if v is None: return None
    try:
        if isinstance(v, float) and np.isnan(v): return None
    except Exception: pass
    if isinstance(v, np.integer): return int(v)
    if isinstance(v, np.floating):
        return None if np.isnan(float(v)) else float(v)
    return v

def fetch_in_batches(tickers):
    """Fetch tickers in small batches with pauses between each batch."""
    all_records = []
    batches = [tickers[i:i+BATCH_SIZE] for i in range(0, len(tickers), BATCH_SIZE)]
    total_batches = len(batches)

    for i, batch in enumerate(batches, 1):
        print(f"  Batch {i}/{total_batches}: fetching {len(batch)} tickers...", flush=True)
        records = fetch_all_parallel(batch, max_workers=MAX_WORKERS)
        all_records.extend(records)
        got = sum(1 for r in records if r.get("Price") is not None)
        print(f"    → {got}/{len(batch)} returned data", flush=True)

        # Pause between batches (skip pause after last batch)
        if i < total_batches:
            print(f"    Pausing {BATCH_PAUSE}s before next batch...", flush=True)
            time.sleep(BATCH_PAUSE)

    return all_records

def main():
    os.makedirs("data", exist_ok=True)
    start = datetime.now(timezone.utc)
    print(f"\n{'='*60}")
    print(f"  PORTFOLIO ANALYZER v2 — GitHub Actions Run")
    print(f"  Started : {start.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    print(f"  Stocks  : {len(PORTFOLIO_TICKERS)}")
    print(f"  Batches : {len(PORTFOLIO_TICKERS)//BATCH_SIZE + 1} x {BATCH_SIZE} tickers")
    print(f"{'='*60}\n")

    # ── Pass 1: fetch all in batches ──────────────────────────────────────
    print("Pass 1: Fetching all tickers in batches...")
    records = fetch_in_batches(PORTFOLIO_TICKERS)

    total_with_data = sum(1 for r in records if r.get("Price") is not None)
    print(f"\nPass 1 complete: {total_with_data}/{len(PORTFOLIO_TICKERS)} tickers have data")

    # ── Pass 2: retry empties with longer delays ──────────────────────────
    empty = [r["Ticker"] for r in records if r.get("Price") is None]
    if empty and len(empty) < len(PORTFOLIO_TICKERS) * 0.8:
        # Only retry if less than 80% failed (otherwise it's a systemic issue)
        print(f"\nPass 2: Retrying {len(empty)} empty tickers (slower)...")
        time.sleep(30)  # longer pause before retry burst
        retry_records = []
        for i in range(0, len(empty), 25):
            batch = empty[i:i+25]
            print(f"  Retry batch {i//25+1}: {len(batch)} tickers...", flush=True)
            r = fetch_all_parallel(batch, max_workers=3)
            retry_records.extend(r)
            if i + 25 < len(empty):
                time.sleep(15)

        rmap = {r["Ticker"]: r for r in retry_records if r.get("Price") is not None}
        records = [rmap.get(r["Ticker"], r) for r in records]
        recovered = len(rmap)
        print(f"  Recovered: {recovered}  |  Still empty: {len(empty)-recovered}")
    elif len(empty) >= len(PORTFOLIO_TICKERS) * 0.8:
        print(f"\n⚠️  WARNING: {len(empty)} tickers returned no data — possible rate limit.")
        print("   Waiting 60s then doing a full retry with slower settings...")
        time.sleep(60)
        records = fetch_in_batches(PORTFOLIO_TICKERS)

    # ── Score ─────────────────────────────────────────────────────────────
    df = pd.DataFrame(records)
    usable = df[df["Price"].notna()]
    print(f"\nScoring {len(usable)} stocks with price data...")

    if len(usable) < 10:
        print("ERROR: Too few stocks with data to produce a meaningful result. Aborting.")
        sys.exit(1)

    sm = compute_sector_medians(df)
    df["Score"]          = df.apply(lambda r: calculate_weighted_score(r, sm), axis=1)
    df                   = df[df["Score"].notna()].copy()
    df["Action"]         = df["Score"].apply(lambda s: get_recommendation(s)[0])
    df["Composite_Flag"] = df.apply(assign_composite_flag, axis=1)
    df.sort_values("Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── Save CSV ──────────────────────────────────────────────────────────
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

    # ── Top 10 ────────────────────────────────────────────────────────────
    top10 = generate_top10_recommendations(df, n=10)
    with open("data/top10.json","w") as f:
        json.dump([{k:_clean(v) for k,v in r.items()} for r in top10], f, indent=2)
    print(f"  ✅ data/top10.json  ({len(top10)} picks)")

    # ── Run info ──────────────────────────────────────────────────────────
    end = datetime.now(timezone.utc)
    info = {
        "run_timestamp_utc": end.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "elapsed_minutes":   round((end-start).total_seconds()/60, 1),
        "total_stocks":      len(df),
        "strong_buy": int((df["Action"]=="STRONG BUY").sum()),
        "buy":        int((df["Action"]=="BUY").sum()),
        "hold":       int((df["Action"]=="HOLD").sum()),
        "sell":       int((df["Action"]=="SELL").sum()),
        "avg_score":  round(float(df["Score"].mean()),1),
    }
    with open("data/run_info.json","w") as f:
        json.dump(info, f, indent=2)
    print(f"  ✅ data/run_info.json")

    print(f"\n{'='*60}")
    print(f"  Done in {info['elapsed_minutes']} min")
    print(f"  {info['total_stocks']} stocks scored")
    print(f"  STRONG BUY:{info['strong_buy']}  BUY:{info['buy']}  HOLD:{info['hold']}  SELL:{info['sell']}")
    print(f"  Avg Score: {info['avg_score']}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    main()

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

def compute_moat_score(row):
    """
    Moat Score 0-100: estimates economic moat strength from quantitative signals.

    Moat proxies used:
      Pricing Power   → Gross Margin vs sector (high GM = pricing power)
      Capital Returns → ROE sustained high (>20%) = returns above cost of capital
      Cash Generation → FCF Yield positive = real cash not just accounting profit
      Financial Fort  → Low Debt/Equity = doesn't need to borrow to survive
      Stability       → Low Beta = not volatile like a commodity
      Smart Money     → High Institutional Ownership = professionals hold it
      Scale/Stickiness→ Operating Margin high = hard to displace
      Earnings Quality→ ROA high = assets generate real returns

    Each factor scores 0-1, multiplied by its weight, normalised to 0-100.
    Returns None if fewer than 4 factors have data.
    """
    def v(col):
        val = row.get(col)
        if val is None: return None
        try:
            f = float(val)
            return None if (f != f) else f   # NaN → None
        except Exception:
            return None

    factors = []  # list of (score_0_to_1, weight)

    # 1. Gross Margin — pricing power (weight 20)
    gm = v("Gross_Margin")
    if gm is not None:
        if   gm >= 70: s = 1.0
        elif gm >= 50: s = 0.8
        elif gm >= 35: s = 0.5
        elif gm >= 20: s = 0.2
        else:          s = 0.0
        factors.append((s, 20))

    # 2. ROE — returns above cost of capital (weight 20)
    roe = v("ROE")
    if roe is not None:
        if   roe >= 30: s = 1.0
        elif roe >= 20: s = 0.8
        elif roe >= 12: s = 0.4
        elif roe >= 0:  s = 0.1
        else:           s = 0.0
        factors.append((s, 20))

    # 3. Operating Margin — operational efficiency / pricing power (weight 15)
    om = v("Op_Margin")
    if om is not None:
        if   om >= 25: s = 1.0
        elif om >= 15: s = 0.7
        elif om >= 8:  s = 0.4
        elif om >= 0:  s = 0.1
        else:          s = 0.0
        factors.append((s, 15))

    # 4. FCF Yield — real cash generation (weight 15)
    fcf = v("FCF_Yield")
    if fcf is not None:
        if   fcf >= 8:  s = 1.0
        elif fcf >= 4:  s = 0.7
        elif fcf >= 1:  s = 0.4
        elif fcf >= 0:  s = 0.1
        else:           s = 0.0
        factors.append((s, 15))

    # 5. Debt/Equity — financial fortress (weight 10)
    de = v("Debt_Equity")
    if de is not None:
        if   de <= 0.1: s = 1.0
        elif de <= 0.3: s = 0.8
        elif de <= 0.7: s = 0.5
        elif de <= 1.5: s = 0.2
        else:           s = 0.0
        factors.append((s, 10))

    # 6. ROA — asset efficiency (weight 10)
    roa = v("ROA")
    if roa is not None:
        if   roa >= 15: s = 1.0
        elif roa >= 8:  s = 0.7
        elif roa >= 3:  s = 0.3
        elif roa >= 0:  s = 0.1
        else:           s = 0.0
        factors.append((s, 10))

    # 7. Beta — stability / not a commodity (weight 5)
    beta = v("Beta")
    if beta is not None:
        if   beta <= 0.6: s = 1.0
        elif beta <= 0.9: s = 0.8
        elif beta <= 1.2: s = 0.5
        elif beta <= 1.8: s = 0.2
        else:             s = 0.0
        factors.append((s, 5))

    # 8. Institutional Ownership — smart money confidence (weight 5)
    inst = v("Inst_Own")
    if inst is not None:
        if   inst >= 80: s = 1.0
        elif inst >= 60: s = 0.7
        elif inst >= 40: s = 0.4
        elif inst >= 20: s = 0.1
        else:            s = 0.0
        factors.append((s, 5))

    if len(factors) < 4:
        return None   # not enough data

    total_score  = sum(s * w for s, w in factors)
    total_weight = sum(w for _, w in factors)
    return round((total_score / total_weight) * 100, 1)


def moat_label(score):
    """Convert numeric moat score to label."""
    if score is None: return "—"
    if score >= 75:   return "Wide"
    if score >= 55:   return "Narrow"
    if score >= 35:   return "Weak"
    return "None"


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

    # ── Moat Score ────────────────────────────────────────────────────────
    print("Computing moat scores...")
    df["Moat_Score"] = df.apply(compute_moat_score, axis=1)
    df["Moat_Label"] = df["Moat_Score"].apply(moat_label)
    wide_moat  = int(df["Moat_Label"].eq("Wide").sum())
    narrow_moat = int(df["Moat_Label"].eq("Narrow").sum())
    print(f"  Wide moat: {wide_moat}  |  Narrow moat: {narrow_moat}")

    df.sort_values("Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── Save CSV ──────────────────────────────────────────────────────────
    cols = [
        "Ticker","Name","Sector","Industry","Score","Action","Composite_Flag",
        "Moat_Score","Moat_Label",
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

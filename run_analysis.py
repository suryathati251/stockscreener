#!/usr/bin/env python3
"""
run_analysis.py — Bridge between portfolio_master.py and the Streamlit app.

Runs the full analysis pipeline and writes the output files that app.py reads:
  data/portfolio_analysis.csv   — full scored + moat-labeled dataset
  data/run_info.json            — timestamp, elapsed, summary stats
  data/top10.json               — top 10 picks with all fields for the UI

Usage:
  python run_analysis.py              # run once, no email
  python run_analysis.py --email      # run once + send email
  python run_analysis.py --schedule   # weekday schedule at time in portfolio_master.py

Schedule via cron (runs at 9:40 AM weekdays):
  40 9 * * 1-5  cd /path/to/your/app && python run_analysis.py >> logs/cron.log 2>&1
"""

import argparse
import json
import os
import sys
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path

# ── Ensure the app's data/ directory exists ──────────────────────────────────
DATA_DIR = Path(__file__).parent / "data"
DATA_DIR.mkdir(exist_ok=True)

# ── Import the master analyzer ────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent))
import portfolio_master as pm   # noqa: E402  (must come after sys.path insert)


def run_and_export(send_email: bool = False):
    """Full pipeline: fetch → score → moat → write data/ files → (optional) email."""
    t0 = time.time()
    print("\n" + "=" * 64)
    print("  PORTFOLIO MASTER — ANALYSIS + STREAMLIT EXPORT")
    print("=" * 64)

    import pandas as pd
    import numpy as np

    # ── 1. Fetch all tickers ──────────────────────────────────────────────────
    n = len(pm.PORTFOLIO_TICKERS)
    print(f"\n  Fetching {n} tickers (pass 1)…")
    records = pm.fetch_all_parallel(pm.PORTFOLIO_TICKERS, max_workers=8)

    # ── 2. Retry empty tickers ────────────────────────────────────────────────
    empty = [r["Ticker"] for r in records if r.get("Price") is None]
    if empty:
        print(f"  Retrying {len(empty)} tickers (pass 2)…")
        time.sleep(3)
        retry = pm.fetch_all_parallel(empty, max_workers=4)
        retry_map = {r["Ticker"]: r for r in retry if r.get("Price") is not None}
        records = [retry_map.get(r["Ticker"], r) for r in records]
        print(f"  Recovered: {len(retry_map)}  |  Still empty: {len(empty) - len(retry_map)}")

    # ── 3. Score ──────────────────────────────────────────────────────────────
    print("\n  Computing sector medians…")
    df = pd.DataFrame(records)
    sector_medians = pm.compute_sector_medians(df)

    print("  Scoring…")
    df["Score"] = df.apply(lambda r: pm.calculate_weighted_score(r, sector_medians), axis=1)
    dropped = df["Score"].isna().sum()
    df = df[df["Score"].notna()].copy()
    if dropped:
        print(f"  ⚠️  Dropped {int(dropped)} tickers with insufficient data")

    df["Action"]         = df["Score"].apply(lambda s: pm.get_recommendation(s)[0])
    df["Target"]         = df["Analyst_Target"]
    df["Upside"]         = df["Analyst_Upside"]
    df["Composite_Flag"] = df.apply(pm.assign_composite_flag, axis=1)

    # ── 4. Moat scoring ───────────────────────────────────────────────────────
    print("  Computing moat scores (brand · switching costs · network effects)…")
    moat_results     = df.apply(lambda r: pm.calculate_moat_score(r, sector_medians), axis=1)
    df["Moat_Score"]     = moat_results.apply(lambda x: x[0])
    df["Moat_Label"]     = moat_results.apply(lambda x: x[1])
    df["Moat_Brand"]     = moat_results.apply(lambda x: x[2].get("brand"))
    df["Moat_Switching"] = moat_results.apply(lambda x: x[2].get("switching"))
    df["Moat_Network"]   = moat_results.apply(lambda x: x[2].get("network"))

    def _append_moat_flag(row):
        existing = str(row.get("Composite_Flag") or "—")
        mf = pm.assign_moat_flag(row)
        if mf:
            return (mf + " · " + existing) if existing != "—" else mf
        return existing
    df["Composite_Flag"] = df.apply(_append_moat_flag, axis=1)

    # ── 5. Hypergrowth scoring ────────────────────────────────────────────────
    print("  Computing hypergrowth scores (growth · leverage · PMF · discovery)…")
    df["Sector_Rev_Growth_Med"] = df["Sector"].apply(
        lambda s: sector_medians.get(s, {}).get("Rev_Growth"))
    hg_results     = df.apply(lambda r: pm.calculate_hypergrowth_score(r, sector_medians), axis=1)
    df["HG_Score"]     = hg_results.apply(lambda x: x[0])
    df["HG_Label"]     = hg_results.apply(lambda x: x[1])
    df["HG_Growth"]    = hg_results.apply(lambda x: x[2].get("growth"))
    df["HG_Leverage"]  = hg_results.apply(lambda x: x[2].get("leverage"))
    df["HG_PMF"]       = hg_results.apply(lambda x: x[2].get("pmf"))
    df["HG_Discovery"] = hg_results.apply(lambda x: x[2].get("discovery"))

    def _append_hg_flag(row):
        existing = str(row.get("Composite_Flag") or "—")
        hf = pm.assign_hypergrowth_flag(row)
        if hf:
            return (hf + " · " + existing) if existing != "—" else hf
        return existing
    df["Composite_Flag"] = df.apply(_append_hg_flag, axis=1)

    df.sort_values("Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    # ── 5. Write CSV ──────────────────────────────────────────────────────────
    csv_cols = [
        "Ticker", "Name", "Sector", "Score", "Action", "Composite_Flag",
        "Moat_Score", "Moat_Label", "Moat_Brand", "Moat_Switching", "Moat_Network",
        "HG_Score", "HG_Label", "HG_Growth", "HG_Leverage", "HG_PMF", "HG_Discovery",
        "Rev_Accel_Streak", "GM_Expansion_4Q", "Op_Leverage_Ratio",
        "Rule_Of_40", "EV_Sales_Div_Growth", "RD_Pct_Rev",
        "Deferred_Rev_Growth", "Cash_Runway_Qtrs",
        "Price", "Target", "Upside", "Mkt_Cap",
        "PE_Fwd", "PS", "PB", "PEG", "EV_EBITDA",
        "ROE", "Rev_Growth", "Rev_Growth_Prev", "Gross_Margin",
        "Op_Margin", "Profit_Margin", "FCF_Yield",
        "EPS_Growth", "EPS_Surprise",
        "From_Low_Pct", "From_High_Pct", "Debt_Equity",
        "Beta", "Short_Float", "Inst_Own", "Insider_Buy_Pct",
        "Div_Yield", "Payout_Ratio", "ROA", "Current_Ratio",
        "MA200", "Vs_MA200",
        "Analyst_Target", "Analyst_Count", "Analyst_Upside",
    ]
    csv_path = DATA_DIR / "portfolio_analysis.csv"
    df[csv_cols].to_csv(str(csv_path), index=False)
    print(f"  ✅  CSV  → {csv_path}")

    # ── 6. Write run_info.json ────────────────────────────────────────────────
    elapsed_min = round((time.time() - t0) / 60, 1)
    run_info = {
        "run_timestamp_utc": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        "elapsed_minutes":   elapsed_min,
        "total_stocks":      len(df),
        "strong_buy":        int((df["Action"] == "STRONG BUY").sum()),
        "buy":               int((df["Action"] == "BUY").sum()),
        "hold":              int((df["Action"] == "HOLD").sum()),
        "sell":              int((df["Action"] == "SELL").sum()),
        "avg_score":         round(float(df["Score"].mean()), 1),
        "wide_moat_count":   int((df["Moat_Label"] == "Wide").sum()),
        "narrow_moat_count": int((df["Moat_Label"] == "Narrow").sum()),
        "hg_rocket_count":   int((df["HG_Label"] == "🚀 Rocket").sum()),
        "hg_high_count":     int((df["HG_Label"] == "🔥 High").sum()),
    }
    ri_path = DATA_DIR / "run_info.json"
    with open(str(ri_path), "w") as f:
        json.dump(run_info, f, indent=2)
    print(f"  ✅  Run info → {ri_path}")

    # ── 7. Generate Top 10 and write top10.json ───────────────────────────────
    print("  Generating Top 10 recommendations…")
    top10 = pm.generate_top10_recommendations(df, n=10)
    top10_path = DATA_DIR / "top10.json"
    with open(str(top10_path), "w") as f:
        json.dump(top10, f, indent=2, default=str)
    print(f"  ✅  Top 10  → {top10_path}")

    # Also save the human-readable top10 text file
    top10_txt = pm.print_top10_report(top10, output_file=str(DATA_DIR / "top10_recommendations.txt"))

    # ── 8. Summary ────────────────────────────────────────────────────────────
    print(f"\n  📊  {n} stocks  |  Avg Score: {run_info['avg_score']}")
    print(f"  ⭐  STRONG BUY: {run_info['strong_buy']}  BUY: {run_info['buy']}  "
          f"HOLD: {run_info['hold']}  SELL: {run_info['sell']}")
    print(f"  🏰  Wide Moat: {run_info['wide_moat_count']}  "
          f"Narrow Moat: {run_info['narrow_moat_count']}")
    print(f"  ⏱️  Completed in {elapsed_min} min\n")

    # ── 9. Optional email ─────────────────────────────────────────────────────
    if send_email:
        msg      = pm.build_email(True, top10_txt)
        email_ok = pm.send_email(msg)
        if not email_ok:
            print("⚠️  Email failed — check GMAIL config in portfolio_master.py")
            return False

    return True


def main():
    parser = argparse.ArgumentParser(
        description="Run portfolio analysis and export data for the Streamlit app."
    )
    parser.add_argument("--email",    action="store_true", help="Send email after analysis")
    parser.add_argument("--schedule", action="store_true", help="Run on weekday schedule")
    args = parser.parse_args()

    if args.schedule:
        try:
            import schedule
        except ImportError:
            print("⚠️  Install schedule: pip install schedule")
            sys.exit(1)

        def job():
            from datetime import datetime as _dt
            if _dt.now().weekday() >= 5:
                print("⏭️  Weekend — skipping.")
                return
            print(f"🕘  Scheduled run at {_dt.now().strftime('%H:%M')}…")
            run_and_export(send_email=args.email)

        schedule.every().day.at(pm.SCHEDULE_TIME).do(job)
        print(f"🗓️  Scheduler running — weekdays at {pm.SCHEDULE_TIME}. Ctrl+C to stop.")
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        ok = run_and_export(send_email=args.email)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()

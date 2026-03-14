#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║           PORTFOLIO MASTER — Analyzer + Scheduler + Email Sender            ║
║                                                                              ║
║  All-in-one script combining portfolio_analyzer_v2.py (screening engine)    ║
║  and run_portfolio_and_email.py (scheduling + Gmail delivery).               ║
║                                                                              ║
║  Screening logic: sector-relative scoring, analyst consensus targets,        ║
║  EV/EBITDA, earnings surprise, revenue acceleration, insider buying,         ║
║  weighted FCF/ROE/growth engine, composite signal flags.                     ║
║                                                                              ║
║  SETUP (one-time):                                                           ║
║    1. pip install yfinance pandas numpy schedule                             ║
║    2. Generate a Gmail App Password at:                                      ║
║       https://myaccount.google.com/apppasswords                              ║
║    3. Fill in the ✏️  CONFIG section below.                                   ║
║    4. Run modes:                                                              ║
║       • python portfolio_master.py          → run once immediately           ║
║       • python portfolio_master.py --schedule → run every weekday at 9:40AM  ║
║       • python portfolio_master.py --no-email → run once, skip email         ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import argparse
import smtplib
import os
import sys
import time
import warnings
import traceback
import logging
import numpy as np
import pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

warnings.filterwarnings("ignore")

# =============================================================================
# ✏️  CONFIG — edit these before running
# =============================================================================
GMAIL_SENDER        = "aithasravani01@gmail.com"      # Gmail address sending the report
GMAIL_APP_PASSWORD  = "mrtq zxgz fmiw wuql"           # Gmail App Password (NOT login password)
EMAIL_RECIPIENTS    = ["aithasravani01@gmail.com"]     # List of recipients

SCHEDULE_TIME       = "09:40"   # 24-hour format, weekdays only

ATTACH_HTML         = True      # Attach the HTML report as a dated file
INLINE_HTML_BODY    = True      # Embed full HTML as email body
INLINE_TOP10        = True      # Include top10 text in plain-text fallback

OUTPUT_DIR          = Path(__file__).parent.resolve()
CSV_FILE            = OUTPUT_DIR / "portfolio_analysis_v2.csv"
HTML_FILE           = OUTPUT_DIR / "portfolio_analysis_v2.html"
TOP10_FILE          = OUTPUT_DIR / "top10_recommendations.txt"
LOG_FILE            = OUTPUT_DIR / "portfolio_master.log"
# =============================================================================

# Logging setup — writes to file AND stdout
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)


# =============================================================================
# ALL TICKERS — 344 STOCKS
# =============================================================================
PORTFOLIO_TICKERS = [
    'AAPL', 'ABNB', 'ACGL', 'ACT', 'ADBE', 'ADI', 'ADM', 'ADP', 'AFRM', 'AI',
    'ALKS', 'ALSN', 'ALV', 'AMAT', 'AMD', 'AMKR', 'AMPH', 'AMTM', 'AMZN', 'ANGO',
    'AOS', 'APAM', 'APEI', 'ASST', 'AU', 'AVGO', 'AX', 'AXS', 'AZZ', 'BABA',
    'BAH', 'BAP', 'BBY', 'BCRX', 'BH', 'BHE', 'BIIB', 'BKH', 'BKKT', 'BKNG',
    'BLBD', 'BMY', 'BNTX', 'BR', 'CAG', 'CAH', 'CAT', 'CATY', 'CBOE', 'CCJ',
    'CDE', 'CDW', 'CERS', 'CF', 'CHRW', 'CI', 'CLMB', 'CMCSA', 'CMP', 'CMPR',
    'CMRE', 'CNA', 'CNI', 'CNM', 'COIN', 'COLL', 'COP', 'COR', 'COST', 'CPB',
    'CPNG', 'CRCL', 'CRCT', 'CRM', 'CRUS', 'CRWD', 'CSGS', 'CTRN', 'CTSH', 'CTVA',
    'CVE', 'CVS', 'CVX', 'CYH', 'DAKT', 'DBD', 'DBI', 'DCI', 'DDOG', 'DDS',
    'DE', 'DECK', 'DELL', 'DG', 'DGICA', 'DGII', 'DIS', 'DNOW', 'DOCU', 'EAT',
    'EB', 'ELA', 'ELMD', 'ENVA', 'ESCA', 'ETSY', 'EXEL', 'EXPE', 'F', 'FBIO',
    'FET', 'FFIV', 'FHI', 'FIGS', 'FLEX', 'FLXS', 'FOR', 'FOXA', 'FTI', 'FTNT',
    'FUBO', 'GAP', 'GAU', 'GDDY', 'GE', 'GEHC', 'GEN', 'GFF', 'GGG', 'GHC',
    'GIB', 'GILT', 'GIS', 'GL', 'GM', 'GOOGL', 'GTN', 'HAS', 'HBB', 'HCSG',
    'HIG', 'HL', 'HNI', 'HOMB', 'HOOD', 'HPQ', 'HRMY', 'HTO', 'IBM', 'ICLR',
    'IDN', 'IMKTA', 'IMO', 'INTC', 'IRWD', 'ISSC', 'JAZZ', 'JBL', 'JBSS',
    'JD', 'JNJ', 'KDP', 'KFY', 'KGC', 'KHC', 'KLAC', 'KROS', 'KVUE', 'LCID',
    'LDOS', 'LHX', 'LI', 'LINC', 'LLY', 'LRCX', 'LULU', 'LUMN', 'LXFR', 'LYFT',
    'M', 'MARA', 'MCB', 'MCK', 'MDB', 'MELI', 'META', 'MG', 'MKSI', 'MLAB',
    'MLI', 'MO', 'MOV', 'MPWR', 'MPX', 'MRNA', 'MRVL', 'MSFT', 'MU', 'NATR',
    'NESR', 'NET', 'NEWT', 'NFLX', 'NGVC', 'NIO', 'NOW', 'NTAP', 'NVDA', 'NVEC',
    'NVO', 'NXPI', 'OC', 'OFG', 'OII', 'OKTA', 'OMC', 'ONON', 'ORCL', 'OTIS',
    'OZK', 'PAGS', 'PANW', 'PATH', 'PAYX', 'PBYI', 'PDD', 'PFBC', 'PFE', 'PFS',
    'PGY', 'PINE', 'PINS', 'PLTR', 'PLXS', 'POWW', 'PPC', 'PRGS', 'PRI', 'PRK',
    'PSKY', 'PSTL', 'PTC', 'PYPL', 'QCOM', 'QLYS', 'QUAD', 'RBBN', 'RCKY', 'RGA',
    'RGCO', 'RIOT', 'RIVN', 'RJF', 'RMD', 'RNR', 'ROP', 'RVLV', 'S', 'SAFT',
    'SAM', 'SE', 'SEIC', 'SHOP', 'SLB', 'SMCI', 'SNAP', 'SNEX', 'SNOW', 'SNX',
    'SOFI', 'SPOT', 'SU', 'TAP', 'TBRG', 'TCMD', 'TDOC', 'TDY', 'TEAM',
    'TENB', 'TGNA', 'TGT', 'THC', 'THR', 'TNK', 'TREE', 'TSLA', 'TSM', 'TXN',
    'UBER', 'UCB', 'UFCS', 'UHS', 'UI', 'UNFI', 'UNH', 'UPST', 'UVE', 'VLGEA',
    'VLTO', 'VPG', 'VREX', 'VRSN', 'VRT', 'W', 'WCC', 'WDAY', 'WKHS', 'WMG',
    'WMT', 'WOOF', 'XOM', 'XPEV', 'YUMC', 'ZBRA', 'ZM', 'ZS', 'CLOV', 'FISV',
    'ZVRA', 'PLTK', 'HUBS', 'TTD', 'GTLB', 'BILL', 'PAYC', 'PCTY', 'VEEV', 'INTU',
    'CDNS', 'SNPS', 'NTNX', 'ESTC', 'CFLT', 'DT', 'MNDY', 'BRZE', 'TOST',
    'APP', 'IOT', 'MSI', 'CHKP', 'FICO', 'TYL', 'CIEN', 'BSY', 'ZETA', 'NKE', 'HIMS',
]

# Tickers to skip in Top 10 (e.g. positions you already hold or want to exclude)
EXCLUDE_TICKERS: list[str] = []


# =============================================================================
# HELPERS
# =============================================================================
def safe_float(value, round_n=2):
    try:
        if value is None or pd.isna(value) or value == "N/A":
            return None
        return round(float(value), round_n)
    except Exception:
        return None


def safe_pct(value):
    val = safe_float(value, 4)
    return round(val * 100, 2) if val is not None else None


def fmt(value, prefix="", suffix="", decimals=2, na="-"):
    try:
        if value is None:
            return na
        if isinstance(value, float) and np.isnan(value):
            return na
        try:
            if pd.isna(value):
                return na
        except Exception:
            pass
        if str(value).lower() in ("nan", "none", "nat", ""):
            return na
        return prefix + "{:.{}f}".format(float(value), decimals) + suffix
    except Exception:
        return na


def _is_nan(v):
    if v is None:
        return True
    try:
        return (isinstance(v, float) and np.isnan(v)) or pd.isna(v)
    except Exception:
        return False


def _empty_row(ticker):
    keys = [
        "Ticker", "Name", "Sector", "Industry", "Price", "Mkt_Cap",
        "PE_Fwd", "PS", "PB", "PEG", "EV_EBITDA",
        "ROE", "Debt_Equity", "EPS_Growth", "EPS_Surprise",
        "Rev_Growth", "Rev_Growth_Prev", "Gross_Margin", "Profit_Margin",
        "Op_Margin", "FCF_Yield", "Beta", "Short_Float",
        "From_Low_Pct", "From_High_Pct", "Inst_Own",
        "Insider_Buy_Pct",
        "Div_Yield", "Payout_Ratio", "ROA", "Current_Ratio",
        "MA200", "Vs_MA200",
        "Analyst_Target", "Analyst_Count", "Analyst_Upside",
        "Composite_Flag",
        # Hypergrowth fields
        "Rev_Accel_Streak", "GM_Expansion_4Q", "Op_Leverage_Ratio",
        "Rule_Of_40", "EV_Sales_Div_Growth", "RD_Pct_Rev",
        "Deferred_Rev_Growth", "Cash_Runway_Qtrs", "Sector_Rev_Growth_Med",
    ]
    return {k: (ticker if k in ("Ticker", "Name") else None) for k in keys}


# =============================================================================
# DATA FETCHING
# =============================================================================
def fetch_ticker_data(ticker, _retries=5, _delay=2.0):
    import yfinance as yf
    import random as _r
    ticker = ticker.upper().strip()
    for _attempt in range(_retries):
        try:
            t    = yf.Ticker(ticker)
            info = t.info
            has_price = (
                info.get("currentPrice") or
                info.get("regularMarketPrice") or
                info.get("previousClose")
            ) if info else None
            if not info or len(info) < 5 or not has_price:
                if _attempt < _retries - 1:
                    wait = _delay * (2 ** _attempt) + _r.uniform(0.5, 2.0)
                    time.sleep(wait)
                    continue
                return _empty_row(ticker)

            price  = (info.get("currentPrice") or info.get("regularMarketPrice")
                      or info.get("previousClose"))
            high52 = info.get("fiftyTwoWeekHigh")
            low52  = info.get("fiftyTwoWeekLow")
            pct_from_low  = round(((price - low52)  / low52)  * 100, 2) if price and low52  else None
            pct_from_high = round(((price - high52) / high52) * 100, 2) if price and high52 else None

            pe_fwd    = safe_float(info.get("forwardPE"))
            ps        = safe_float(info.get("priceToSalesTrailing12Months"))
            pb        = safe_float(info.get("priceToBook"))
            peg       = safe_float(info.get("pegRatio"))
            ev_ebitda = safe_float(info.get("enterpriseToEbitda"))

            eps_growth_raw = info.get("earningsQuarterlyGrowth")
            eps_growth     = safe_pct(eps_growth_raw)

            # Recalculate PEG if missing
            if (peg is None or peg <= 0) and pe_fwd and eps_growth_raw and eps_growth_raw > 0:
                try:
                    peg = round(pe_fwd / (eps_growth_raw * 100), 2)
                except Exception:
                    peg = None

            # Earnings surprise
            eps_surprise = None
            try:
                cal = t.calendar
                if cal is not None and not cal.empty:
                    eps_est = cal.get("Earnings Average") if "Earnings Average" in cal.index else None
                    eps_act = info.get("trailingEps")
                    if eps_est is not None and eps_act is not None and float(eps_est) != 0:
                        eps_surprise = round(
                            ((float(eps_act) - float(eps_est)) / abs(float(eps_est))) * 100, 2
                        )
            except Exception:
                pass

            roe = safe_float(info.get("returnOnEquity"))
            if roe is not None:
                roe = round(roe * 100, 2)

            de = safe_float(info.get("debtToEquity"))
            if de is not None:
                de = round(de / 100, 2)

            mkt_cap   = info.get("marketCap")
            fcf       = info.get("freeCashflow")
            fcf_yield = round((fcf / mkt_cap) * 100, 2) if fcf and mkt_cap else None

            rev_growth   = safe_pct(info.get("revenueGrowth"))
            gross_margin = safe_pct(info.get("grossMargins"))
            profit_margin= safe_pct(info.get("profitMargins"))
            op_margin    = safe_pct(info.get("operatingMargins"))
            beta         = safe_float(info.get("beta"))
            short_float  = safe_pct(info.get("shortPercentOfFloat"))
            inst_own     = safe_pct(info.get("heldPercentInstitutions"))

            # Revenue growth acceleration (current vs prior quarter YoY)
            rev_growth_prev = None
            try:
                qfin = t.quarterly_financials
                if qfin is not None and not qfin.empty and "Total Revenue" in qfin.index:
                    revs = qfin.loc["Total Revenue"].dropna()
                    if len(revs) >= 4:
                        g1 = (revs.iloc[0] - revs.iloc[2]) / abs(revs.iloc[2]) * 100 if revs.iloc[2] != 0 else None
                        g2 = (revs.iloc[1] - revs.iloc[3]) / abs(revs.iloc[3]) * 100 if revs.iloc[3] != 0 else None
                        if g1 is not None and g2 is not None:
                            rev_growth_prev = round(float(g2), 2)
                            if rev_growth is None:
                                rev_growth = round(float(g1), 2)
            except Exception:
                pass

            # Insider buying signal
            insider_buy_pct = None
            try:
                insider = t.insider_transactions
                if insider is not None and not insider.empty:
                    recent    = insider.head(20)
                    buys      = (recent["Shares"] > 0).sum() if "Shares" in recent.columns else 0
                    total_tx  = len(recent)
                    if total_tx > 0:
                        insider_buy_pct = round((buys / total_tx) * 100, 1)
            except Exception:
                pass

            div_yield_raw = info.get("dividendYield")
            div_yield     = round(div_yield_raw * 100, 2) if div_yield_raw else None
            payout_raw    = info.get("payoutRatio")
            payout_ratio  = round(payout_raw * 100, 2) if payout_raw else None

            roa_raw       = info.get("returnOnAssets")
            roa           = round(roa_raw * 100, 2) if roa_raw is not None else None
            current_ratio = safe_float(info.get("currentRatio"))

            ma200    = safe_float(info.get("twoHundredDayAverage"))
            vs_ma200 = round(((price - ma200) / ma200) * 100, 2) if price and ma200 else None

            analyst_target = safe_float(info.get("targetMeanPrice"))
            analyst_count  = info.get("numberOfAnalystOpinions")
            analyst_upside = None
            if analyst_target and price and price > 0:
                analyst_upside = round(((analyst_target - price) / price) * 100, 2)

            if mkt_cap:
                if   mkt_cap >= 1e12: mkt_cap_fmt = "${:.1f}T".format(mkt_cap / 1e12)
                elif mkt_cap >= 1e9:  mkt_cap_fmt = "${:.1f}B".format(mkt_cap / 1e9)
                else:                 mkt_cap_fmt = "${:.0f}M".format(mkt_cap / 1e6)
            else:
                mkt_cap_fmt = None

            # ── HYPERGROWTH extra fields ──────────────────────────────────────
            # 1. Revenue acceleration streak & gross margin trajectory
            rev_accel_streak   = None
            gm_expansion_4q    = None
            op_leverage_ratio  = None
            deferred_rev_growth = None
            rd_pct_rev         = None
            cash_runway_qtrs   = None
            try:
                qfin = t.quarterly_financials
                if qfin is not None and not qfin.empty:
                    # ── Rev acceleration streak (how many consecutive quarters accelerating)
                    if "Total Revenue" in qfin.index:
                        revs = qfin.loc["Total Revenue"].dropna()
                        if len(revs) >= 6:
                            # compute YoY growth for last 4 quarters
                            yoy = []
                            for i in range(4):
                                if revs.iloc[i+2] != 0:
                                    yoy.append((revs.iloc[i] - revs.iloc[i+2]) / abs(revs.iloc[i+2]) * 100)
                            # count streak of acceleration from most recent
                            streak = 0
                            for i in range(len(yoy) - 1):
                                if yoy[i] > yoy[i+1]:
                                    streak += 1
                                else:
                                    break
                            rev_accel_streak = streak

                    # ── Gross margin expansion vs 4Q avg
                    if "Gross Profit" in qfin.index and "Total Revenue" in qfin.index:
                        gp   = qfin.loc["Gross Profit"].dropna()
                        rv   = qfin.loc["Total Revenue"].dropna()
                        idx  = gp.index.intersection(rv.index)
                        if len(idx) >= 5:
                            gms = [(float(gp[i]) / float(rv[i]) * 100) for i in idx[:5] if float(rv[i]) != 0]
                            if len(gms) >= 5:
                                current_gm = gms[0]
                                avg_4q     = sum(gms[1:5]) / 4
                                gm_expansion_4q = round(current_gm - avg_4q, 1)  # bps equivalent in pct pts

                    # ── Operating leverage: rev growth / opex growth
                    if "Total Revenue" in qfin.index and "Total Expenses" in qfin.index:
                        rv2  = qfin.loc["Total Revenue"].dropna()
                        opex = qfin.loc["Total Expenses"].dropna()
                        idx2 = rv2.index.intersection(opex.index)
                        if len(idx2) >= 3:
                            rg_r  = (float(rv2[idx2[0]]) - float(rv2[idx2[2]])) / abs(float(rv2[idx2[2]])) if float(rv2[idx2[2]]) != 0 else None
                            opg_r = (float(opex[idx2[0]]) - float(opex[idx2[2]])) / abs(float(opex[idx2[2]])) if float(opex[idx2[2]]) != 0 else None
                            if rg_r and opg_r and opg_r != 0:
                                op_leverage_ratio = round(rg_r / opg_r, 2)

                    # ── Deferred revenue growth
                    bs = t.quarterly_balance_sheet
                    if bs is not None and not bs.empty:
                        dr_keys = [k for k in bs.index if "Deferred" in str(k) and "Revenue" in str(k)]
                        if dr_keys:
                            dr = bs.loc[dr_keys[0]].dropna()
                            if len(dr) >= 3 and float(dr.iloc[2]) != 0:
                                deferred_rev_growth = round(
                                    (float(dr.iloc[0]) - float(dr.iloc[2])) / abs(float(dr.iloc[2])) * 100, 1)

                    # ── Cash runway
                    cf = t.quarterly_cashflow
                    if cf is not None and not cf.empty and bs is not None and not bs.empty:
                        cash_keys = [k for k in bs.index if "Cash" in str(k)]
                        burn_keys = [k for k in cf.index if "Operating" in str(k)]
                        if cash_keys and burn_keys:
                            cash  = float(bs.loc[cash_keys[0]].dropna().iloc[0])
                            qburn = float(cf.loc[burn_keys[0]].dropna().iloc[0])
                            if qburn < 0:  # negative = burning cash
                                cash_runway_qtrs = round(cash / abs(qburn), 1)
            except Exception:
                pass

            # ── R&D as % of revenue
            try:
                ann = t.financials
                if ann is not None and not ann.empty:
                    rd_keys  = [k for k in ann.index if "Research" in str(k)]
                    rev_keys = [k for k in ann.index if "Total Revenue" in str(k)]
                    if rd_keys and rev_keys:
                        rd_val  = float(ann.loc[rd_keys[0]].dropna().iloc[0])
                        rev_val = float(ann.loc[rev_keys[0]].dropna().iloc[0])
                        if rev_val > 0:
                            rd_pct_rev = round(abs(rd_val) / rev_val * 100, 1)
            except Exception:
                pass

            # ── Rule of 40 & EV/Sales÷Growth
            rule_of_40       = None
            ev_sales_div_growth = None
            try:
                fcf_margin = (fcf / (info.get("totalRevenue") or 1)) * 100 if fcf and info.get("totalRevenue") else None
                if rev_growth is not None and fcf_margin is not None:
                    rule_of_40 = round(rev_growth + fcf_margin, 1)
                ev = info.get("enterpriseValue")
                total_rev = info.get("totalRevenue")
                if ev and total_rev and total_rev > 0 and rev_growth and rev_growth > 0:
                    ev_sales = ev / total_rev
                    ev_sales_div_growth = round(ev_sales / rev_growth, 3)
            except Exception:
                pass

            return {
                "Ticker": ticker, "Name": info.get("shortName", ticker),
                "Sector": info.get("sector", "Unknown"), "Industry": info.get("industry", "Unknown"),
                "Price": price, "Mkt_Cap": mkt_cap_fmt,
                "PE_Fwd": pe_fwd, "PS": ps, "PB": pb, "PEG": peg, "EV_EBITDA": ev_ebitda,
                "ROE": roe, "Debt_Equity": de, "EPS_Growth": eps_growth, "EPS_Surprise": eps_surprise,
                "Rev_Growth": rev_growth, "Rev_Growth_Prev": rev_growth_prev,
                "Gross_Margin": gross_margin, "Profit_Margin": profit_margin,
                "Op_Margin": op_margin, "FCF_Yield": fcf_yield,
                "Beta": beta, "Short_Float": short_float,
                "From_Low_Pct": pct_from_low, "From_High_Pct": pct_from_high,
                "Inst_Own": inst_own, "Insider_Buy_Pct": insider_buy_pct,
                "Div_Yield": div_yield, "Payout_Ratio": payout_ratio,
                "ROA": roa, "Current_Ratio": current_ratio,
                "MA200": ma200, "Vs_MA200": vs_ma200,
                "Analyst_Target": analyst_target,
                "Analyst_Count": analyst_count,
                "Analyst_Upside": analyst_upside,
                "Composite_Flag": None,
                # Hypergrowth fields
                "Rev_Accel_Streak":      rev_accel_streak,
                "GM_Expansion_4Q":       gm_expansion_4q,
                "Op_Leverage_Ratio":     op_leverage_ratio,
                "Rule_Of_40":            rule_of_40,
                "EV_Sales_Div_Growth":   ev_sales_div_growth,
                "RD_Pct_Rev":            rd_pct_rev,
                "Deferred_Rev_Growth":   deferred_rev_growth,
                "Cash_Runway_Qtrs":      cash_runway_qtrs,
                "Sector_Rev_Growth_Med": None,  # filled after sector medians computed
            }
        except Exception:
            if _attempt < _retries - 1:
                time.sleep(_delay * (_attempt + 1))
                continue
            return _empty_row(ticker)
    return _empty_row(ticker)


def fetch_all_parallel(tickers, max_workers=8):
    import random as _rj
    results, completed, total, start = [], 0, len(tickers), time.time()

    def _jwrap(t):
        time.sleep(_rj.uniform(0, 0.4))
        return fetch_ticker_data(t)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_jwrap, t): t for t in tickers}
        for future in as_completed(futures):
            completed += 1
            eta    = ((time.time() - start) / completed) * (total - completed)
            filled = int(40 * completed / total)
            sys.stdout.write("\r  [{}{}] {:5.1f}%  {}/{}  ETA: {:.0f}s  ".format(
                "█" * filled, "░" * (40 - filled),
                completed / total * 100, completed, total, eta))
            sys.stdout.flush()
            results.append(future.result())
    print()
    return results


# =============================================================================
# SECTOR MEDIANS — for relative scoring
# =============================================================================
def compute_sector_medians(df):
    metrics = ["PE_Fwd", "PS", "EV_EBITDA", "Gross_Margin", "Op_Margin", "Rev_Growth"]
    sector_medians = {}
    for sector, group in df.groupby("Sector"):
        sector_medians[sector] = {}
        for m in metrics:
            vals = group[m].dropna()
            sector_medians[sector][m] = float(vals.median()) if len(vals) >= 3 else None
    return sector_medians


# =============================================================================
# SCORING ENGINE — WEIGHTED, SECTOR-RELATIVE
# =============================================================================
WEIGHTS = {
    # Quality / profitability
    "fcf_yield":        8,
    "roe":              7,
    "op_margin":        5,
    "roa":              4,
    "gross_margin_rel": 4,
    "current_ratio":    3,
    # Growth
    "rev_growth":       7,
    "rev_accel":        5,
    "eps_growth":       5,
    "eps_surprise":     4,
    # Valuation — relative to sector
    "pe_rel":           5,
    "ps_rel":           4,
    "ev_ebitda_rel":    4,
    "peg":              5,
    # Technical / momentum
    "vs_ma200":         5,
    "from_low":         3,
    # Analyst signal
    "analyst_upside":   6,
    "analyst_count":    2,
    # Sentiment / risk
    "debt_equity":      4,
    "short_float":      3,
    "inst_own":         2,
    "insider_buy":      3,
    "beta":             2,
    # Dividend / stability
    "div_yield":        3,
    "payout_ratio":     2,
}
TOTAL_WEIGHT = sum(WEIGHTS.values())


def calculate_weighted_score(row, sector_medians):
    """Returns a 0–100 score. Returns None if insufficient data."""
    def _has(m):
        v = row.get(m)
        return v is not None and not (isinstance(v, float) and np.isnan(v))

    if not _has("Price"):
        return None

    financial_metrics = [
        "PE_Fwd", "PS", "PB", "ROE", "FCF_Yield", "Rev_Growth",
        "Gross_Margin", "Op_Margin", "Profit_Margin", "Debt_Equity",
        "EV_EBITDA", "Beta", "ROA", "Current_Ratio", "EPS_Growth"
    ]
    if sum(1 for m in financial_metrics if _has(m)) < 2:
        return None

    sector = row.get("Sector", "Unknown")
    sm     = sector_medians.get(sector, {})
    total  = 0.0
    w      = lambda key: WEIGHTS.get(key, 0)

    # ── FCF Yield (weight 8) ──────────────────────────────────────────────
    fcf = row["FCF_Yield"]
    if fcf is not None:
        if   fcf > 10:  total += w("fcf_yield") * 1.0
        elif fcf > 7:   total += w("fcf_yield") * 0.8
        elif fcf > 4:   total += w("fcf_yield") * 0.5
        elif fcf > 1:   total += w("fcf_yield") * 0.2
        elif fcf < -2:  total += w("fcf_yield") * -0.8
        elif fcf < 0:   total += w("fcf_yield") * -0.3

    # ── ROE (weight 7) ────────────────────────────────────────────────────
    roe = row["ROE"]
    if roe is not None:
        if   roe > 40:  total += w("roe") * 1.0
        elif roe > 25:  total += w("roe") * 0.8
        elif roe > 15:  total += w("roe") * 0.5
        elif roe > 8:   total += w("roe") * 0.2
        elif roe < 0:   total += w("roe") * -0.8
        else:           total += w("roe") * -0.1

    # ── Operating Margin (weight 5) ───────────────────────────────────────
    om = row["Op_Margin"]
    if om is not None:
        if   om > 30:   total += w("op_margin") * 1.0
        elif om > 20:   total += w("op_margin") * 0.7
        elif om > 10:   total += w("op_margin") * 0.3
        elif om > 0:    total += w("op_margin") * 0.0
        elif om < -20:  total += w("op_margin") * -1.0
        else:           total += w("op_margin") * -0.5

    # ── ROA (weight 4) ────────────────────────────────────────────────────
    roa = row["ROA"]
    if roa is not None:
        if   roa > 15:  total += w("roa") * 1.0
        elif roa > 8:   total += w("roa") * 0.6
        elif roa > 3:   total += w("roa") * 0.2
        elif roa < 0:   total += w("roa") * -0.8

    # ── Gross Margin RELATIVE to sector (weight 4) ────────────────────────
    gm     = row["Gross_Margin"]
    gm_med = sm.get("Gross_Margin")
    if gm is not None and gm_med is not None and gm_med > 0:
        rel = (gm - gm_med) / gm_med
        total += w("gross_margin_rel") * max(-1.0, min(1.0, rel * 2))
    elif gm is not None:
        if   gm > 70:   total += w("gross_margin_rel") * 0.8
        elif gm > 40:   total += w("gross_margin_rel") * 0.3
        elif gm < 10:   total += w("gross_margin_rel") * -0.5

    # ── Current Ratio (weight 3) ──────────────────────────────────────────
    cr = row["Current_Ratio"]
    if cr is not None:
        if   cr > 3.0:  total += w("current_ratio") * 0.8
        elif cr > 2.0:  total += w("current_ratio") * 1.0
        elif cr > 1.5:  total += w("current_ratio") * 0.5
        elif cr > 1.0:  total += w("current_ratio") * 0.0
        else:           total += w("current_ratio") * -0.8

    # ── Revenue Growth (weight 7) ─────────────────────────────────────────
    rg = row["Rev_Growth"]
    if rg is not None:
        if   rg > 40:   total += w("rev_growth") * 1.0
        elif rg > 25:   total += w("rev_growth") * 0.8
        elif rg > 15:   total += w("rev_growth") * 0.5
        elif rg > 5:    total += w("rev_growth") * 0.2
        elif rg < -15:  total += w("rev_growth") * -1.0
        elif rg < -5:   total += w("rev_growth") * -0.5
        else:           total += w("rev_growth") * -0.1

    # ── Revenue Growth Acceleration (weight 5) ────────────────────────────
    rg_prev = row["Rev_Growth_Prev"]
    if rg is not None and rg_prev is not None:
        accel = rg - rg_prev
        if   accel > 15:  total += w("rev_accel") * 1.0
        elif accel > 5:   total += w("rev_accel") * 0.6
        elif accel > 0:   total += w("rev_accel") * 0.2
        elif accel < -15: total += w("rev_accel") * -1.0
        elif accel < -5:  total += w("rev_accel") * -0.5
        else:             total += w("rev_accel") * -0.1

    # ── EPS Growth (weight 5) ─────────────────────────────────────────────
    eg = row["EPS_Growth"]
    if eg is not None:
        if   eg > 50:   total += w("eps_growth") * 1.0
        elif eg > 25:   total += w("eps_growth") * 0.7
        elif eg > 10:   total += w("eps_growth") * 0.3
        elif eg < -25:  total += w("eps_growth") * -1.0
        elif eg < -10:  total += w("eps_growth") * -0.5
        else:           total += w("eps_growth") * -0.1

    # ── Earnings Surprise (weight 4) ──────────────────────────────────────
    eps_s = row["EPS_Surprise"]
    if eps_s is not None:
        if   eps_s > 20:  total += w("eps_surprise") * 1.0
        elif eps_s > 10:  total += w("eps_surprise") * 0.7
        elif eps_s > 3:   total += w("eps_surprise") * 0.3
        elif eps_s < -15: total += w("eps_surprise") * -1.0
        elif eps_s < -5:  total += w("eps_surprise") * -0.5

    # ── P/E Forward RELATIVE to sector (weight 5) ─────────────────────────
    pe     = row["PE_Fwd"]
    pe_med = sm.get("PE_Fwd")
    if pe is not None and pe > 0:
        if pe_med and pe_med > 0:
            rel = (pe_med - pe) / pe_med
            total += w("pe_rel") * max(-1.0, min(1.0, rel * 1.5))
        else:
            if   pe < 12:  total += w("pe_rel") * 0.8
            elif pe < 20:  total += w("pe_rel") * 0.5
            elif pe > 60:  total += w("pe_rel") * -0.8
            elif pe > 40:  total += w("pe_rel") * -0.4
    elif pe is not None and pe < 0:
        total += w("pe_rel") * -0.6

    # ── P/S RELATIVE to sector (weight 4) ────────────────────────────────
    ps     = row["PS"]
    ps_med = sm.get("PS")
    if ps is not None:
        if ps_med and ps_med > 0:
            rel = (ps_med - ps) / ps_med
            total += w("ps_rel") * max(-1.0, min(1.0, rel * 1.5))
        else:
            if   ps < 1:   total += w("ps_rel") * 0.8
            elif ps < 3:   total += w("ps_rel") * 0.4
            elif ps > 20:  total += w("ps_rel") * -0.8
            elif ps > 10:  total += w("ps_rel") * -0.4

    # ── EV/EBITDA RELATIVE to sector (weight 4) ───────────────────────────
    ev     = row["EV_EBITDA"]
    ev_med = sm.get("EV_EBITDA")
    if ev is not None and ev > 0:
        if ev_med and ev_med > 0:
            rel = (ev_med - ev) / ev_med
            total += w("ev_ebitda_rel") * max(-1.0, min(1.0, rel * 1.5))
        else:
            if   ev < 10:  total += w("ev_ebitda_rel") * 0.8
            elif ev < 20:  total += w("ev_ebitda_rel") * 0.4
            elif ev > 50:  total += w("ev_ebitda_rel") * -0.8
            elif ev > 30:  total += w("ev_ebitda_rel") * -0.3

    # ── PEG (weight 5) ────────────────────────────────────────────────────
    peg = row["PEG"]
    if peg is not None:
        if   0 < peg < 0.7:  total += w("peg") * 1.0
        elif peg < 1.0:      total += w("peg") * 0.7
        elif peg < 1.5:      total += w("peg") * 0.3
        elif peg > 4.0:      total += w("peg") * -0.8
        elif peg > 2.5:      total += w("peg") * -0.4

    # ── 200 DMA position (weight 5) ───────────────────────────────────────
    vs200 = row["Vs_MA200"]
    if vs200 is not None:
        if   vs200 > 30:   total += w("vs_ma200") * 0.5
        elif vs200 > 10:   total += w("vs_ma200") * 1.0
        elif vs200 > 0:    total += w("vs_ma200") * 0.5
        elif vs200 > -10:  total += w("vs_ma200") * -0.3
        elif vs200 > -25:  total += w("vs_ma200") * -0.6
        else:              total += w("vs_ma200") * -1.0

    # ── From 52-week low (weight 3) ───────────────────────────────────────
    fl = row["From_Low_Pct"]
    if fl is not None:
        if   fl < 10:   total += w("from_low") * 1.0
        elif fl < 25:   total += w("from_low") * 0.5
        elif fl > 200:  total += w("from_low") * -0.5

    # ── Analyst Upside (weight 6) — real consensus target ─────────────────
    au = row["Analyst_Upside"]
    if au is not None:
        if   au > 40:   total += w("analyst_upside") * 1.0
        elif au > 25:   total += w("analyst_upside") * 0.8
        elif au > 15:   total += w("analyst_upside") * 0.5
        elif au > 5:    total += w("analyst_upside") * 0.2
        elif au < -10:  total += w("analyst_upside") * -1.0
        elif au < 0:    total += w("analyst_upside") * -0.5

    # ── Analyst Count (weight 2) ──────────────────────────────────────────
    ac = row["Analyst_Count"]
    if ac is not None and not _is_nan(ac):
        try:
            ac = int(ac)
            if   ac >= 20:  total += w("analyst_count") * 1.0
            elif ac >= 10:  total += w("analyst_count") * 0.5
            elif ac <= 2:   total += w("analyst_count") * -0.5
        except Exception:
            pass

    # ── Debt/Equity (weight 4) ────────────────────────────────────────────
    de = row["Debt_Equity"]
    if de is not None:
        if   de < 0.2:   total += w("debt_equity") * 1.0
        elif de < 0.5:   total += w("debt_equity") * 0.5
        elif de < 1.0:   total += w("debt_equity") * 0.0
        elif de < 2.0:   total += w("debt_equity") * -0.4
        else:            total += w("debt_equity") * -1.0

    # ── Short Float (weight 3) ────────────────────────────────────────────
    sf = row["Short_Float"]
    if sf is not None:
        if   sf > 25:   total += w("short_float") * -1.0
        elif sf > 15:   total += w("short_float") * -0.5
        elif sf < 2:    total += w("short_float") * 0.5

    # ── Institutional Ownership (weight 2) ────────────────────────────────
    io = row["Inst_Own"]
    if io is not None:
        if   io > 80:   total += w("inst_own") * 0.8
        elif io > 60:   total += w("inst_own") * 0.3
        elif io < 10:   total += w("inst_own") * -0.5

    # ── Insider Buying (weight 3) ─────────────────────────────────────────
    ib = row["Insider_Buy_Pct"]
    if ib is not None:
        if   ib > 70:   total += w("insider_buy") * 1.0
        elif ib > 50:   total += w("insider_buy") * 0.5
        elif ib < 20:   total += w("insider_buy") * -0.3

    # ── Beta / volatility (weight 2) ──────────────────────────────────────
    beta = row["Beta"]
    if beta is not None:
        if   beta > 2.5:  total += w("beta") * -1.0
        elif beta > 1.8:  total += w("beta") * -0.5
        elif beta < 0.7:  total += w("beta") * 0.5

    # ── Dividend Yield (weight 3) ─────────────────────────────────────────
    dy = row["Div_Yield"]
    if dy is not None and dy > 0:
        if   dy > 5.0:   total += w("div_yield") * 1.0
        elif dy > 3.0:   total += w("div_yield") * 0.7
        elif dy > 1.5:   total += w("div_yield") * 0.4
        elif dy > 0.3:   total += w("div_yield") * 0.1

    # ── Payout Ratio (weight 2) ───────────────────────────────────────────
    pr = row["Payout_Ratio"]
    if pr is not None and pr > 0:
        if   pr < 35:   total += w("payout_ratio") * 0.8
        elif pr < 60:   total += w("payout_ratio") * 0.4
        elif pr > 100:  total += w("payout_ratio") * -1.0
        elif pr > 80:   total += w("payout_ratio") * -0.5

    # ── Rule of 40 bonus (weight 4) ──────────────────────────────────────
    # Added on top of base score — rewards efficient growth without penalizing
    # high-multiple stocks that are scaling well. Cap at +4 pts to keep it
    # a signal boost, not a score driver.
    r40 = row.get("Rule_Of_40")
    if r40 is not None:
        if   r40 > 60:  total += 4.0   # elite growers — full bonus
        elif r40 > 40:  total += 2.5   # passes the bar
        elif r40 > 20:  total += 1.0   # below bar but not a drag
        elif r40 < -20: total -= 3.0   # burning cash with no growth — penalty
        elif r40 < 0:   total -= 1.5

    # ── Normalize to 0–100 ────────────────────────────────────────────────
    max_possible = float(TOTAL_WEIGHT) + 4.0   # +4 accounts for Rule of 40 bonus ceiling
    score = ((total + max_possible) / (2 * max_possible)) * 100
    return min(max(round(score, 1), 0), 100)


def get_recommendation(score):
    if score >= 78: return "STRONG BUY", "bg-success", "GREEN"
    if score >= 62: return "BUY",         "bg-info",    "GREEN"
    if score >= 44: return "HOLD",        "bg-warning", "ORANGE"
    return                  "SELL",        "bg-danger",  "RED"


# =============================================================================
# COMPOSITE FLAGS
# =============================================================================
def assign_composite_flag(row):
    flags = []
    roe   = row.get("ROE");          fcf   = row.get("FCF_Yield")
    rg    = row.get("Rev_Growth");   de    = row.get("Debt_Equity")
    dy    = row.get("Div_Yield");    au    = row.get("Analyst_Upside")
    beta  = row.get("Beta");         sf    = row.get("Short_Float")
    vs200 = row.get("Vs_MA200");     peg   = row.get("PEG")
    accel = None
    if row.get("Rev_Growth") and row.get("Rev_Growth_Prev"):
        accel = row["Rev_Growth"] - row["Rev_Growth_Prev"]

    if roe and roe > 20 and fcf and fcf > 4 and (de is None or de < 1.0):
        flags.append("⭐ Compounder")
    if accel and accel > 10 and rg and rg > 15:
        flags.append("🚀 Accel Growth")
    if peg and 0 < peg < 1.0 and fcf and fcf > 3:
        flags.append("💎 Deep Value")
    ac = row.get("Analyst_Count")
    if au and not _is_nan(au) and au > 25 and ac and not _is_nan(ac) and int(ac) >= 15:
        flags.append("📈 Analyst Conviction")
    if dy and dy > 3 and (not de or de < 1.5):
        flags.append("💰 Income")
    # Rule of 40 flag — highlights efficient growth companies
    r40 = row.get("Rule_Of_40")
    if r40 is not None:
        if r40 > 60:
            flags.append("📐 Rule of 40 Elite")   # >60 = top tier (NVDA, MSFT level)
        elif r40 > 40:
            flags.append("📐 Rule of 40")          # passes the standard bar
    if sf and sf > 20:
        flags.append("⚠️ High Short")
    if beta and beta > 2.0:
        flags.append("⚠️ High Beta")
    if de and de > 2.5:
        flags.append("⚠️ High Leverage")
    if vs200 and vs200 < -20:
        flags.append("⚠️ Below 200 DMA")

    return " · ".join(flags) if flags else "—"


# =============================================================================
# MOAT SCORING ENGINE
# Three pillars: Brand/Pricing Power · Switching Costs · Network Effects
#
# Each pillar scores 0–33 pts. Total 0–100 → Wide / Narrow / Weak / None
#
# Brand/Pricing Power  — proxied by gross margin stability + absolute level
# Switching Costs      — proxied by revenue predictability + recurring-style growth
# Network Effects      — proxied by revenue growth rate + user/platform scale signals
# =============================================================================

MOAT_WEIGHTS = {
    # ── Brand / Pricing Power (33 pts max) ───────────────────────────────────
    "gross_margin_abs":    10,   # absolute gross margin level
    "gross_margin_vs_sec": 10,   # above-sector premium = pricing power
    "op_margin_abs":        8,   # operating leverage = brand moat
    "fcf_yield_brand":      5,   # high FCF = not competing on price

    # ── Switching Costs (33 pts max) ─────────────────────────────────────────
    "rev_stability":       12,   # positive & consistent rev growth = sticky customers
    "roe_consistency":     11,   # high sustained ROE = structural advantage
    "debt_discipline":      5,   # low D/E = doesn't need leverage to compete
    "current_ratio_sw":     5,   # liquidity strength = earnings quality

    # ── Network Effects (33 pts max) ─────────────────────────────────────────
    "rev_growth_net":      13,   # high growth = growing network
    "rev_accel_net":        8,   # accelerating = network flywheel
    "inst_ownership":       7,   # smart money piling in = network recognized
    "analyst_upside_net":   5,   # analyst conviction on platform upside
}
MOAT_TOTAL = sum(MOAT_WEIGHTS.values())   # = 99 → normalize to 100


def calculate_moat_score(row, sector_medians):
    """
    Returns (moat_score 0–100, moat_label, pillar_scores dict).
    moat_label: 'Wide' (>=65) | 'Narrow' (>=40) | 'Weak' (>=20) | 'None'
    """
    def _has(m):
        v = row.get(m)
        return v is not None and not (isinstance(v, float) and np.isnan(v))

    # Need at least a price to score
    if not _has("Price"):
        return None, "None", {}

    sector = row.get("Sector", "Unknown")
    sm     = sector_medians.get(sector, {})
    total  = 0.0
    w      = lambda key: MOAT_WEIGHTS.get(key, 0)
    pillars = {"brand": 0.0, "switching": 0.0, "network": 0.0}

    # ── PILLAR 1: BRAND / PRICING POWER ──────────────────────────────────────

    # 1a. Gross margin absolute level (10 pts)
    gm = row.get("Gross_Margin")
    if gm is not None:
        if   gm > 75:  pts = w("gross_margin_abs") * 1.0
        elif gm > 60:  pts = w("gross_margin_abs") * 0.8
        elif gm > 45:  pts = w("gross_margin_abs") * 0.5
        elif gm > 30:  pts = w("gross_margin_abs") * 0.2
        elif gm < 10:  pts = w("gross_margin_abs") * -0.4
        else:          pts = 0.0
        total += pts; pillars["brand"] += pts

    # 1b. Gross margin vs sector median (10 pts) — above peers = pricing power
    gm_med = sm.get("Gross_Margin")
    if gm is not None and gm_med and gm_med > 0:
        rel = (gm - gm_med) / gm_med
        pts = w("gross_margin_vs_sec") * max(-1.0, min(1.0, rel * 2.5))
        total += pts; pillars["brand"] += pts

    # 1c. Operating margin absolute (8 pts)
    om = row.get("Op_Margin")
    if om is not None:
        if   om > 30:  pts = w("op_margin_abs") * 1.0
        elif om > 20:  pts = w("op_margin_abs") * 0.7
        elif om > 10:  pts = w("op_margin_abs") * 0.3
        elif om > 0:   pts = 0.0
        elif om < -20: pts = w("op_margin_abs") * -1.0
        else:          pts = w("op_margin_abs") * -0.5
        total += pts; pillars["brand"] += pts

    # 1d. FCF yield as brand moat proxy (5 pts) — not competing on price
    fcf = row.get("FCF_Yield")
    if fcf is not None:
        if   fcf > 10: pts = w("fcf_yield_brand") * 1.0
        elif fcf > 6:  pts = w("fcf_yield_brand") * 0.7
        elif fcf > 3:  pts = w("fcf_yield_brand") * 0.4
        elif fcf < 0:  pts = w("fcf_yield_brand") * -0.5
        else:          pts = 0.0
        total += pts; pillars["brand"] += pts

    # ── PILLAR 2: SWITCHING COSTS ─────────────────────────────────────────────

    # 2a. Revenue stability / predictability (12 pts)
    rg = row.get("Rev_Growth")
    if rg is not None:
        if   rg > 20:   pts = w("rev_stability") * 1.0
        elif rg > 10:   pts = w("rev_stability") * 0.7
        elif rg > 3:    pts = w("rev_stability") * 0.4
        elif rg > 0:    pts = w("rev_stability") * 0.1
        elif rg < -15:  pts = w("rev_stability") * -1.0
        elif rg < -5:   pts = w("rev_stability") * -0.5
        else:           pts = w("rev_stability") * -0.2
        total += pts; pillars["switching"] += pts

    # 2b. ROE consistency (11 pts) — high sustained ROE = structural advantage
    roe = row.get("ROE")
    if roe is not None:
        if   roe > 40:  pts = w("roe_consistency") * 1.0
        elif roe > 25:  pts = w("roe_consistency") * 0.8
        elif roe > 15:  pts = w("roe_consistency") * 0.5
        elif roe > 8:   pts = w("roe_consistency") * 0.1
        elif roe < 0:   pts = w("roe_consistency") * -0.8
        else:           pts = 0.0
        total += pts; pillars["switching"] += pts

    # 2c. Debt discipline (5 pts) — low D/E = doesn't need leverage to defend share
    de = row.get("Debt_Equity")
    if de is not None:
        if   de < 0.2:  pts = w("debt_discipline") * 1.0
        elif de < 0.5:  pts = w("debt_discipline") * 0.5
        elif de < 1.0:  pts = 0.0
        elif de < 2.0:  pts = w("debt_discipline") * -0.4
        else:           pts = w("debt_discipline") * -1.0
        total += pts; pillars["switching"] += pts

    # 2d. Current ratio (5 pts) — quality of earnings
    cr = row.get("Current_Ratio")
    if cr is not None:
        if   cr > 2.5:  pts = w("current_ratio_sw") * 0.8
        elif cr > 1.5:  pts = w("current_ratio_sw") * 0.5
        elif cr > 1.0:  pts = 0.0
        else:           pts = w("current_ratio_sw") * -0.8
        total += pts; pillars["switching"] += pts

    # ── PILLAR 3: NETWORK EFFECTS ─────────────────────────────────────────────

    # 3a. Revenue growth (13 pts) — fast growth = growing network
    if rg is not None:
        if   rg > 40:  pts = w("rev_growth_net") * 1.0
        elif rg > 25:  pts = w("rev_growth_net") * 0.8
        elif rg > 15:  pts = w("rev_growth_net") * 0.5
        elif rg > 5:   pts = w("rev_growth_net") * 0.2
        elif rg < -15: pts = w("rev_growth_net") * -1.0
        elif rg < -5:  pts = w("rev_growth_net") * -0.5
        else:          pts = 0.0
        total += pts; pillars["network"] += pts

    # 3b. Revenue acceleration (8 pts) — flywheel signal
    rg_prev = row.get("Rev_Growth_Prev")
    if rg is not None and rg_prev is not None:
        accel = rg - rg_prev
        if   accel > 15:  pts = w("rev_accel_net") * 1.0
        elif accel > 5:   pts = w("rev_accel_net") * 0.6
        elif accel > 0:   pts = w("rev_accel_net") * 0.2
        elif accel < -15: pts = w("rev_accel_net") * -1.0
        elif accel < -5:  pts = w("rev_accel_net") * -0.5
        else:             pts = 0.0
        total += pts; pillars["network"] += pts

    # 3c. Institutional ownership (7 pts) — smart money = network recognized
    io = row.get("Inst_Own")
    if io is not None:
        if   io > 85:  pts = w("inst_ownership") * 1.0
        elif io > 70:  pts = w("inst_ownership") * 0.6
        elif io > 50:  pts = w("inst_ownership") * 0.2
        elif io < 10:  pts = w("inst_ownership") * -0.5
        else:          pts = 0.0
        total += pts; pillars["network"] += pts

    # 3d. Analyst upside on platform conviction (5 pts)
    au = row.get("Analyst_Upside")
    if au is not None:
        if   au > 35:  pts = w("analyst_upside_net") * 1.0
        elif au > 20:  pts = w("analyst_upside_net") * 0.6
        elif au > 5:   pts = w("analyst_upside_net") * 0.2
        elif au < -5:  pts = w("analyst_upside_net") * -0.8
        else:          pts = 0.0
        total += pts; pillars["network"] += pts

    # ── Normalize to 0–100 ────────────────────────────────────────────────────
    moat_score = round(min(max(((total + MOAT_TOTAL) / (2 * MOAT_TOTAL)) * 100, 0), 100), 1)

    if   moat_score >= 65: label = "Wide"
    elif moat_score >= 45: label = "Narrow"
    elif moat_score >= 28: label = "Weak"
    else:                  label = "None"

    return moat_score, label, {k: round(v, 1) for k, v in pillars.items()}


def assign_moat_flag(row):
    """
    Adds moat signals to composite flags string.
    Called after moat scores are computed.
    """
    label = row.get("Moat_Label")
    if label == "Wide":
        return "🏰 Wide Moat"
    if label == "Narrow":
        return "〰️ Narrow Moat"
    return None


# =============================================================================
# HYPERGROWTH SCORING ENGINE — catches 10x candidates
#
# PHILOSOPHY: Deliberately ignores valuation multiples (P/E, EV/EBITDA etc.)
# because early 10x stocks always look "expensive" on those metrics.
# Instead scores purely on: growth trajectory · operating leverage ·
# market position · early PMF signals · discovery phase indicators.
#
# Score 0–100. Label: 🚀 Rocket (≥70) | 🔥 High (≥50) | 📈 Emerging (≥35) | — Below
#
# Four pillars (25 pts each):
#   1. Growth Trajectory   — acceleration streak, Rev growth vs sector, GM expansion
#   2. Operating Leverage  — op leverage ratio, Rule of 40, R&D investment
#   3. PMF & Stickiness    — deferred rev growth, EPS surprise streak, analyst accel
#   4. Discovery Phase     — inst ownership trajectory, short squeeze potential,
#                            analyst count growth, cash runway (won't dilute)
# =============================================================================

HG_WEIGHTS = {
    # Pillar 1: Growth Trajectory (25 pts)
    "rev_growth_abs":        8,   # raw growth rate — hypergrowth needs >25%
    "rev_vs_sector":         7,   # beating sector median = market share gain
    "rev_accel_streak":      6,   # 2+ consecutive quarters accelerating = inflection
    "gm_expansion":          4,   # expanding gross margins = pricing power emerging

    # Pillar 2: Operating Leverage (25 pts)
    "op_leverage":           9,   # rev growing faster than costs = scaling
    "rule_of_40":            8,   # growth + fcf margin — best single SaaS metric
    "rd_investment":         4,   # high R&D = building next moat
    "ev_sales_growth":       4,   # cheap relative to growth speed

    # Pillar 3: PMF & Stickiness (25 pts)
    "deferred_rev":          8,   # customers paying upfront = pull demand
    "eps_surprise":          7,   # beating estimates consistently = PMF
    "analyst_upside_hg":     6,   # analysts raising targets = narrative shifting
    "cash_runway":           4,   # won't need to dilute shareholders

    # Pillar 4: Discovery Phase (25 pts)
    "inst_own_level":        7,   # 20–60% = mid-discovery sweet spot
    "short_squeeze":         6,   # declining short interest = bears capitulating
    "analyst_count_hg":      6,   # 5–20 analysts = being discovered not yet crowded
    "beta_momentum":         6,   # higher beta in uptrend = momentum building
}
HG_TOTAL = sum(HG_WEIGHTS.values())  # = 100


def calculate_hypergrowth_score(row, sector_medians):
    """
    Returns (hg_score 0–100, hg_label, pillar_scores dict).
    hg_label: '🚀 Rocket' (>=70) | '🔥 High' (>=50) | '📈 Emerging' (>=35) | '—'
    """
    def _has(m):
        v = row.get(m)
        return v is not None and not (isinstance(v, float) and np.isnan(v))

    if not _has("Price"):
        return None, "—", {}

    sector = row.get("Sector", "Unknown")
    sm     = sector_medians.get(sector, {})
    total  = 0.0
    w      = lambda key: HG_WEIGHTS.get(key, 0)
    pillars = {"growth": 0.0, "leverage": 0.0, "pmf": 0.0, "discovery": 0.0}

    # ══════════════════════════════════════════════════════════════════════
    # PILLAR 1: GROWTH TRAJECTORY
    # ══════════════════════════════════════════════════════════════════════

    # 1a. Raw revenue growth (weight 8) — hypergrowth threshold is >25%
    rg = row.get("Rev_Growth")
    if rg is not None:
        if   rg > 50:   pts = w("rev_growth_abs") * 1.0
        elif rg > 35:   pts = w("rev_growth_abs") * 0.85
        elif rg > 25:   pts = w("rev_growth_abs") * 0.65
        elif rg > 15:   pts = w("rev_growth_abs") * 0.35
        elif rg > 5:    pts = w("rev_growth_abs") * 0.1
        elif rg < -10:  pts = w("rev_growth_abs") * -0.8
        else:           pts = w("rev_growth_abs") * -0.2
        total += pts; pillars["growth"] += pts

    # 1b. Rev growth vs sector median (weight 7) — are they taking share?
    sec_rg = sm.get("Rev_Growth")
    if rg is not None and sec_rg is not None and sec_rg != 0:
        beat = rg - sec_rg
        if   beat > 30:   pts = w("rev_vs_sector") * 1.0
        elif beat > 15:   pts = w("rev_vs_sector") * 0.7
        elif beat > 5:    pts = w("rev_vs_sector") * 0.4
        elif beat > 0:    pts = w("rev_vs_sector") * 0.1
        elif beat < -20:  pts = w("rev_vs_sector") * -0.8
        elif beat < -5:   pts = w("rev_vs_sector") * -0.3
        else:             pts = 0.0
        total += pts; pillars["growth"] += pts

    # 1c. Acceleration streak (weight 6) — 3+ quarters = S-curve inflection confirmed
    streak = row.get("Rev_Accel_Streak")
    if streak is not None:
        if   streak >= 3: pts = w("rev_accel_streak") * 1.0   # confirmed inflection
        elif streak == 2: pts = w("rev_accel_streak") * 0.6   # building momentum
        elif streak == 1: pts = w("rev_accel_streak") * 0.25  # possible start
        else:             pts = w("rev_accel_streak") * -0.3  # decelerating
        total += pts; pillars["growth"] += pts

    # 1d. Gross margin expansion vs 4Q avg (weight 4)
    gm_exp = row.get("GM_Expansion_4Q")
    if gm_exp is not None:
        if   gm_exp > 5:    pts = w("gm_expansion") * 1.0   # strong pricing power emerging
        elif gm_exp > 2:    pts = w("gm_expansion") * 0.6
        elif gm_exp > 0:    pts = w("gm_expansion") * 0.2
        elif gm_exp < -5:   pts = w("gm_expansion") * -1.0  # margins collapsing
        elif gm_exp < -2:   pts = w("gm_expansion") * -0.5
        else:               pts = 0.0
        total += pts; pillars["growth"] += pts

    # ══════════════════════════════════════════════════════════════════════
    # PILLAR 2: OPERATING LEVERAGE
    # ══════════════════════════════════════════════════════════════════════

    # 2a. Op leverage ratio: rev growth / opex growth (weight 9)
    # >1.5x means revenue scaling faster than costs — the holy grail
    ol = row.get("Op_Leverage_Ratio")
    if ol is not None:
        if   ol > 2.5:  pts = w("op_leverage") * 1.0   # exceptional scale efficiency
        elif ol > 1.75: pts = w("op_leverage") * 0.8
        elif ol > 1.25: pts = w("op_leverage") * 0.5   # healthy leverage
        elif ol > 0.75: pts = w("op_leverage") * 0.0   # neutral
        elif ol > 0:    pts = w("op_leverage") * -0.4  # costs growing faster
        else:           pts = w("op_leverage") * -0.8  # negative leverage (burning)
        total += pts; pillars["leverage"] += pts

    # 2b. Rule of 40 (weight 8) — best single metric for growth+profitability balance
    r40 = row.get("Rule_Of_40")
    if r40 is not None:
        if   r40 > 60:  pts = w("rule_of_40") * 1.0   # elite (Snowflake, Cloudflare territory)
        elif r40 > 40:  pts = w("rule_of_40") * 0.7   # passes the bar
        elif r40 > 20:  pts = w("rule_of_40") * 0.2   # below bar but growing
        elif r40 < -20: pts = w("rule_of_40") * -1.0  # burning fast with no growth
        elif r40 < 0:   pts = w("rule_of_40") * -0.4
        else:           pts = 0.0
        total += pts; pillars["leverage"] += pts

    # 2c. R&D as % of revenue (weight 4) — investing in next moat
    # High R&D is a POSITIVE signal for hypergrowth (opposite of value screener)
    rd = row.get("RD_Pct_Rev")
    if rd is not None:
        if   rd > 25:   pts = w("rd_investment") * 1.0   # heavy R&D = building advantage
        elif rd > 15:   pts = w("rd_investment") * 0.7
        elif rd > 8:    pts = w("rd_investment") * 0.3
        elif rd < 2:    pts = w("rd_investment") * -0.3  # no R&D = no moat building
        else:           pts = 0.0
        total += pts; pillars["leverage"] += pts

    # 2d. EV/Sales ÷ Rev growth (weight 4) — cheap relative to growth speed
    # Lower = you're not overpaying for the growth. <0.5 is excellent.
    evsg = row.get("EV_Sales_Div_Growth")
    if evsg is not None and evsg > 0:
        if   evsg < 0.3:  pts = w("ev_sales_growth") * 1.0  # very cheap for growth rate
        elif evsg < 0.5:  pts = w("ev_sales_growth") * 0.7
        elif evsg < 1.0:  pts = w("ev_sales_growth") * 0.3
        elif evsg > 3.0:  pts = w("ev_sales_growth") * -0.7  # very expensive for growth
        elif evsg > 2.0:  pts = w("ev_sales_growth") * -0.3
        else:             pts = 0.0
        total += pts; pillars["leverage"] += pts

    # ══════════════════════════════════════════════════════════════════════
    # PILLAR 3: PRODUCT-MARKET FIT & STICKINESS
    # ══════════════════════════════════════════════════════════════════════

    # 3a. Deferred revenue growth (weight 8) — customers paying upfront = pull demand
    drg = row.get("Deferred_Rev_Growth")
    if drg is not None:
        if   drg > 50:   pts = w("deferred_rev") * 1.0   # backlog exploding
        elif drg > 25:   pts = w("deferred_rev") * 0.7
        elif drg > 10:   pts = w("deferred_rev") * 0.3
        elif drg < -20:  pts = w("deferred_rev") * -0.8  # customers not renewing
        elif drg < 0:    pts = w("deferred_rev") * -0.3
        else:            pts = 0.0
        total += pts; pillars["pmf"] += pts

    # 3b. EPS surprise (weight 7) — consistent beats = management has visibility
    eps_s = row.get("EPS_Surprise")
    if eps_s is not None:
        if   eps_s > 25:  pts = w("eps_surprise") * 1.0   # massive beats = demand > supply
        elif eps_s > 15:  pts = w("eps_surprise") * 0.75
        elif eps_s > 5:   pts = w("eps_surprise") * 0.4
        elif eps_s > 0:   pts = w("eps_surprise") * 0.1
        elif eps_s < -20: pts = w("eps_surprise") * -1.0
        elif eps_s < -5:  pts = w("eps_surprise") * -0.5
        else:             pts = 0.0
        total += pts; pillars["pmf"] += pts

    # 3c. Analyst upside (weight 6) — used differently here: large upside = narrative shift
    au = row.get("Analyst_Upside")
    if au is not None:
        if   au > 50:   pts = w("analyst_upside_hg") * 1.0   # analysts far behind = stock being re-rated
        elif au > 30:   pts = w("analyst_upside_hg") * 0.75
        elif au > 15:   pts = w("analyst_upside_hg") * 0.4
        elif au > 0:    pts = w("analyst_upside_hg") * 0.1
        elif au < -10:  pts = w("analyst_upside_hg") * -0.8  # analysts ahead of price = peaked
        else:           pts = 0.0
        total += pts; pillars["pmf"] += pts

    # 3d. Cash runway (weight 4) — >8 quarters means won't dilute you
    cr = row.get("Cash_Runway_Qtrs")
    if cr is not None:
        if   cr > 12:   pts = w("cash_runway") * 1.0   # very safe, won't need to raise
        elif cr > 8:    pts = w("cash_runway") * 0.6
        elif cr > 4:    pts = w("cash_runway") * 0.0   # borderline
        else:           pts = w("cash_runway") * -0.8  # imminent dilution risk
        total += pts; pillars["pmf"] += pts

    # ══════════════════════════════════════════════════════════════════════
    # PILLAR 4: DISCOVERY PHASE
    # ══════════════════════════════════════════════════════════════════════

    # 4a. Institutional ownership (weight 7) — mid-range = still being discovered
    # 20–60% is the sweet spot: institutions are interested but not crowded yet
    io = row.get("Inst_Own")
    if io is not None:
        if   20 <= io <= 55:  pts = w("inst_own_level") * 1.0   # discovery sweet spot
        elif io < 20:         pts = w("inst_own_level") * 0.5   # undiscovered
        elif io <= 70:        pts = w("inst_own_level") * 0.3   # crowded but ok
        else:                 pts = w("inst_own_level") * -0.3  # fully owned = crowded trade
        total += pts; pillars["discovery"] += pts

    # 4b. Short float — high short = bearish consensus = potential squeeze fuel (weight 6)
    sf = row.get("Short_Float")
    if sf is not None:
        if   sf > 20:   pts = w("short_squeeze") * 0.8   # high short = massive squeeze potential
        elif sf > 10:   pts = w("short_squeeze") * 0.5   # meaningful short interest
        elif sf > 5:    pts = w("short_squeeze") * 0.2
        elif sf < 1:    pts = w("short_squeeze") * -0.3  # no one shorting = no squeeze fuel
        else:           pts = 0.0
        total += pts; pillars["discovery"] += pts

    # 4c. Analyst count (weight 6) — 5–20 analysts = being discovered, not yet consensus
    ac = row.get("Analyst_Count")
    if ac is not None and not _is_nan(ac):
        try:
            ac = int(ac)
            if   5 <= ac <= 15:   pts = w("analyst_count_hg") * 1.0   # sweet spot: discovered not crowded
            elif ac < 5:          pts = w("analyst_count_hg") * 0.5   # undercovered = hidden gem potential
            elif ac <= 25:        pts = w("analyst_count_hg") * 0.3
            else:                 pts = w("analyst_count_hg") * -0.2  # everyone already knows
        except Exception:
            pts = 0.0
        total += pts; pillars["discovery"] += pts

    # 4d. Beta as momentum signal (weight 6)
    # In a hypergrowth context, high beta in uptrend = momentum behind it
    beta = row.get("Beta")
    vs200 = row.get("Vs_MA200")
    if beta is not None and vs200 is not None:
        if beta > 1.5 and vs200 > 10:    pts = w("beta_momentum") * 1.0   # high beta + above 200MA = momentum
        elif beta > 1.2 and vs200 > 0:   pts = w("beta_momentum") * 0.6
        elif beta > 1.0:                 pts = w("beta_momentum") * 0.2
        elif beta > 2.0 and vs200 < -10: pts = w("beta_momentum") * -0.8  # high beta below 200 = dangerous
        else:                            pts = 0.0
        total += pts; pillars["discovery"] += pts

    # ── Normalize to 0–100 ────────────────────────────────────────────────────
    hg_score = round(min(max(((total + HG_TOTAL) / (2 * HG_TOTAL)) * 100, 0), 100), 1)

    if   hg_score >= 70: label = "🚀 Rocket"
    elif hg_score >= 50: label = "🔥 High"
    elif hg_score >= 35: label = "📈 Emerging"
    else:                label = "—"

    return hg_score, label, {k: round(v, 1) for k, v in pillars.items()}


def assign_hypergrowth_flag(row):
    """Returns a flag string for top hypergrowth candidates."""
    label = row.get("HG_Label", "")
    if label == "🚀 Rocket":
        return "🚀 Rocket"
    if label == "🔥 High":
        return "🔥 HG High"
    return None


# =============================================================================
# MAGIC FORMULA — Joel Greenblatt ("The Little Book That Still Beats the Market")
#
# The Magic Formula ranks stocks on two metrics and combines the ranks:
#
#   1. Earnings Yield  = EBIT / Enterprise Value  (higher = cheaper)
#      — Best proxy available: inverse of EV/EBITDA (1 / EV_EBITDA)
#        Uses Op_Margin × implied revenue as fallback earnings proxy.
#
#   2. Return on Capital = EBIT / (Net Working Capital + Net Fixed Assets)
#      — Best proxy: ROA (Return on Assets) — captures EBIT efficiency
#        relative to total asset base. Falls back to ROE when ROA missing.
#
# Greenblatt excludes: financials, utilities, foreign ADRs, micro-caps.
# We apply a soft filter: warn but still rank them so the user sees all.
#
# Combined Magic Rank = EY_Rank + ROC_Rank  (lower combined rank = better)
# Top 20 by combined rank are surfaced in the Magic Formula tab.
# =============================================================================

# Sectors excluded by Greenblatt's original formula
_MF_EXCLUDE_SECTORS = {"Financial Services", "Utilities", "Real Estate"}


def calculate_magic_formula_ranks(df):
    """
    Given the full DataFrame, compute Earnings Yield, Return on Capital,
    their individual ranks, combined Magic Rank, and return a new DataFrame
    of eligible stocks sorted by Magic Rank ascending (best first).

    Returns (mf_df, excluded_count) where mf_df contains all columns plus:
      MF_EarningsYield   — 1/EV_EBITDA as proxy (%)
      MF_ReturnOnCapital — ROA (or ROE fallback) as proxy (%)
      MF_EY_Rank         — rank on earnings yield (1 = highest yield = best)
      MF_ROC_Rank        — rank on return on capital (1 = highest ROC = best)
      MF_Combined_Rank   — sum of two ranks (lower = better)
      MF_Excluded        — True if Greenblatt would exclude this sector
    """

    work = df.copy()

    # ── Step 1: Compute Earnings Yield proxy ─────────────────────────────────
    # Greenblatt: EY = EBIT / EV.  We have EV/EBITDA → invert it.
    # EY = 1 / EV_EBITDA  expressed as a percentage.
    def _earnings_yield(row):
        ev_ebitda = row.get("EV_EBITDA")
        if ev_ebitda is not None and not _is_nan(ev_ebitda) and ev_ebitda > 0:
            return round((1.0 / ev_ebitda) * 100, 2)
        # Fallback: use Forward P/E inverted  (E/P)
        pe = row.get("PE_Fwd")
        if pe is not None and not _is_nan(pe) and pe > 0:
            return round((1.0 / pe) * 100, 2)
        return None

    # ── Step 2: Compute Return on Capital proxy ───────────────────────────────
    # Greenblatt: ROC = EBIT / (NWC + NFA).
    # Best available proxy: ROA (uses total assets as denominator).
    # Fallback: ROE (uses equity, slightly different but correlated).
    def _return_on_capital(row):
        roa = row.get("ROA")
        if roa is not None and not _is_nan(roa):
            return roa  # already in %
        roe = row.get("ROE")
        if roe is not None and not _is_nan(roe):
            return roe  # fallback — less precise
        return None

    work["MF_EarningsYield"]   = work.apply(_earnings_yield,    axis=1)
    work["MF_ReturnOnCapital"] = work.apply(_return_on_capital, axis=1)

    # ── Step 3: Mark excluded sectors ────────────────────────────────────────
    work["MF_Excluded"] = work["Sector"].apply(
        lambda s: str(s) in _MF_EXCLUDE_SECTORS
    )
    excluded_count = int(work["MF_Excluded"].sum())

    # Only rank eligible stocks (Greenblatt excludes financials/utilities/RE)
    eligible = work[~work["MF_Excluded"]].copy()

    # Need both metrics to rank
    eligible = eligible.dropna(subset=["MF_EarningsYield", "MF_ReturnOnCapital"])

    # Only include stocks with positive EY and positive ROC (Greenblatt requires this)
    eligible = eligible[
        (eligible["MF_EarningsYield"]   > 0) &
        (eligible["MF_ReturnOnCapital"] > 0)
    ].copy()

    if len(eligible) == 0:
        return work, excluded_count

    # ── Step 4: Rank each metric (1 = best) ──────────────────────────────────
    # Higher EY is better (more earnings per dollar of enterprise value)
    eligible["MF_EY_Rank"]  = eligible["MF_EarningsYield"].rank(
        ascending=False, method="min").astype(int)

    # Higher ROC is better (more efficient capital allocation)
    eligible["MF_ROC_Rank"] = eligible["MF_ReturnOnCapital"].rank(
        ascending=False, method="min").astype(int)

    # ── Step 5: Combined rank (lower = better) ────────────────────────────────
    eligible["MF_Combined_Rank"] = eligible["MF_EY_Rank"] + eligible["MF_ROC_Rank"]

    eligible.sort_values("MF_Combined_Rank", ascending=True, inplace=True)
    eligible.reset_index(drop=True, inplace=True)

    return eligible, excluded_count


def _build_magic_formula_tab(df):
    """Build HTML for the Magic Formula tab."""
    mf_df, excl_count = calculate_magic_formula_ranks(df)
    top20 = mf_df.head(20)
    total_eligible = len(mf_df)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows_html = ""
    for rank_idx, (_, row) in enumerate(top20.iterrows(), 1):
        ticker    = str(row.get("Ticker", ""))
        name      = str(row.get("Name", ticker))
        sector    = str(row.get("Sector", ""))
        ey        = row.get("MF_EarningsYield")
        roc       = row.get("MF_ReturnOnCapital")
        ey_rank   = row.get("MF_EY_Rank")
        roc_rank  = row.get("MF_ROC_Rank")
        comb_rank = row.get("MF_Combined_Rank")
        ev_ebitda = row.get("EV_EBITDA")
        roa       = row.get("ROA")
        flags     = str(row.get("Composite_Flag") or "—")
        au        = row.get("Analyst_Upside")
        score     = row.get("Score")
        price     = row.get("Price")
        mkt_cap   = str(row.get("Mkt_Cap") or "-")

        ey_src  = "EV/EBITDA" if (ev_ebitda and not _is_nan(ev_ebitda) and ev_ebitda > 0) else "Fwd P/E"
        roc_src = "ROA" if (roa and not _is_nan(roa)) else "ROE"
        medal   = {1:"🥇",2:"🥈",3:"🥉"}.get(rank_idx, str(rank_idx))
        au_cls  = "analyst-up" if (au and not _is_nan(au) and au >= 0) else "analyst-dn"

        rows_html += """<tr>
<td class="tc" style="font-size:13px">{medal}</td>
<td><strong style="font-size:13px">{ticker}</strong><br><span style="font-size:11px;color:#6e7681">{name}</span></td>
<td class="tc" style="font-weight:700;color:#a371f7;font-size:15px">{comb}</td>
<td class="tc"><span style="color:#58a6ff;font-weight:600">{ey}</span><br><span style="font-size:10px;color:#6e7681">#{ey_r} · {ey_src}</span></td>
<td class="tc"><span style="color:#3fb950;font-weight:600">{roc}</span><br><span style="font-size:10px;color:#6e7681">#{roc_r} · {roc_src}</span></td>
<td class="tc">{price}</td>
<td class="tc">{mkt_cap}</td>
<td class="tc"><span class="{au_cls}">{au}</span></td>
<td class="tc">{score}</td>
<td class="flag-cell">{flags}</td>
<td style="font-size:11px;color:#6e7681">{sector}</td>
</tr>""".format(
            medal=medal, ticker=ticker, name=name[:30],
            comb=str(int(comb_rank)) if comb_rank else "-",
            ey=("{:.2f}%".format(ey) if ey else "-"), ey_r=int(ey_rank) if ey_rank else "-", ey_src=ey_src,
            roc=("{:.2f}%".format(roc) if roc else "-"), roc_r=int(roc_rank) if roc_rank else "-", roc_src=roc_src,
            price=("${:.2f}".format(price) if price else "-"), mkt_cap=mkt_cap,
            au_cls=au_cls, au=("{:+.1f}%".format(au) if (au and not _is_nan(au)) else "-"),
            score=str(score) if score else "-", flags=flags, sector=sector,
        )

    return """<div id="tab-magic" class="tab-panel">
<div class="tab-content">
  <div class="info-card">
    <h2 style="color:#a371f7">🧙 Magic Formula — Top 20</h2>
    <p>Based on <em>The Little Book That Still Beats the Market</em> by Joel Greenblatt.<br>
    Ranks {eligible} eligible stocks on Earnings Yield + Return on Capital. {excl_count} excluded (Financials, Utilities, Real Estate).<br>Generated: {ts}</p>
    <div class="pill-row">
      <div class="pill"><span class="p-num" style="color:#58a6ff">EY</span><span class="p-lbl">1÷EV/EBITDA</span></div>
      <div class="pill"><span class="p-num" style="color:#3fb950">ROC</span><span class="p-lbl">ROA proxy</span></div>
      <div class="pill"><span class="p-num" style="color:#a371f7">Rank</span><span class="p-lbl">Lower = Better</span></div>
    </div>
    <p style="font-size:11px;margin:8px 0 0">⚠️ Proxy calculations — not exact Greenblatt. Verify with official filings.</p>
  </div>
  <div class="data-table-wrap">
  <table class="data-table">
  <thead><tr>
    <th>#</th><th>Ticker</th>
    <th style="color:#a371f7">Combined ↑</th>
    <th style="color:#58a6ff">Earnings Yield</th>
    <th style="color:#3fb950">Return on Capital</th>
    <th>Price</th><th>Mkt Cap</th><th>Analyst ↑</th><th>Score</th><th>Flags</th><th>Sector</th>
  </tr></thead>
  <tbody>{rows}</tbody>
  </table>
  </div>
  <div class="note-card" style="background:#130d1f;border-color:#a371f744">
    <strong style="color:#a371f7">📖 Greenblatt's 6-Step Process</strong><br>
    1. Screen large-cap stocks &nbsp;·&nbsp; 2. Exclude financials, utilities, ADRs &nbsp;·&nbsp;
    3. Rank by Earnings Yield (highest = #1) &nbsp;·&nbsp; 4. Rank by Return on Capital (highest = #1) &nbsp;·&nbsp;
    5. Add ranks — lowest combined wins &nbsp;·&nbsp; 6. Buy top 20–30, hold 1 year, rebalance<br>
    <span style="color:#6e7681">Backtest 1988–2004: ~30.8%/yr vs S&amp;P 12.4%. Past performance ≠ future results.</span>
  </div>
</div></div>""".format(eligible=total_eligible, excl_count=excl_count, ts=ts, rows=rows_html)
    top20 = mf_df.head(20)
    total_eligible = len(mf_df)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    rows_html = ""
    for rank_idx, (_, row) in enumerate(top20.iterrows(), 1):
        ticker    = str(row.get("Ticker", ""))
        name      = str(row.get("Name", ticker))
        sector    = str(row.get("Sector", ""))
        price     = row.get("Price")
        mkt_cap   = str(row.get("Mkt_Cap") or "-")
        ey        = row.get("MF_EarningsYield")
        roc       = row.get("MF_ReturnOnCapital")
        ey_rank   = row.get("MF_EY_Rank")
        roc_rank  = row.get("MF_ROC_Rank")
        comb_rank = row.get("MF_Combined_Rank")
        ev_ebitda = row.get("EV_EBITDA")
        pe_fwd    = row.get("PE_Fwd")
        roe       = row.get("ROE")
        roa       = row.get("ROA")
        flags     = str(row.get("Composite_Flag") or "—")
        au        = row.get("Analyst_Upside")
        score     = row.get("Score")

        # Data source indicator
        ey_src  = "EV/EBITDA" if (ev_ebitda and not _is_nan(ev_ebitda) and ev_ebitda > 0) else "Fwd P/E"
        roc_src = "ROA" if (roa and not _is_nan(roa)) else "ROE"

        price_s   = "${:.2f}".format(price)   if price  else "-"
        ey_s      = "{:.2f}%".format(ey)      if ey     else "-"
        roc_s     = "{:.2f}%".format(roc)     if roc    else "-"
        ey_r_s    = "#{}".format(int(ey_rank))  if ey_rank  else "-"
        roc_r_s   = "#{}".format(int(roc_rank)) if roc_rank else "-"
        comb_s    = str(int(comb_rank))        if comb_rank else "-"
        score_s   = str(score)                 if score  else "-"
        au_s      = "{:+.1f}%".format(au)     if (au and not _is_nan(au)) else "-"
        au_cls    = "analyst-up" if (au and not _is_nan(au) and au >= 0) else "analyst-dn"

        # Medal emoji for top 3
        medal = {1: "🥇", 2: "🥈", 3: "🥉"}.get(rank_idx, str(rank_idx))

        rows_html += """<tr>
  <td class="tc" style="font-weight:700;font-size:14px">{medal}</td>
  <td><strong>{ticker}</strong><br><small>{name}</small></td>
  <td class="tc" style="font-weight:700;color:#f0b429">{comb}</td>
  <td class="tc">{ey} <small style="color:#8b949e">({ey_r})</small><br><small style="color:#8b949e">{ey_src}</small></td>
  <td class="tc">{roc} <small style="color:#8b949e">({roc_r})</small><br><small style="color:#8b949e">{roc_src}</small></td>
  <td class="tc">{price}</td>
  <td class="tc">{mkt_cap}</td>
  <td class="tc"><span class="{au_cls}">{au}</span></td>
  <td class="tc"><span style="background:#1f2937;padding:2px 7px;border-radius:4px">{score}</span></td>
  <td class="tc flag-cell">{flags}</td>
  <td class="tc" style="color:#8b949e;font-size:11px">{sector}</td>
</tr>
""".format(
            medal=medal, ticker=ticker, name=name[:35],
            comb=comb_s, ey=ey_s, ey_r=ey_r_s, ey_src=ey_src,
            roc=roc_s, roc_r=roc_r_s, roc_src=roc_src,
            price=price_s, mkt_cap=mkt_cap, au=au_s, au_cls=au_cls,
            score=score_s, flags=flags, sector=sector,
        )

    html = """
<div id="tab-magic" class="tab-panel" style="display:none;padding:0 20px 30px">

  <!-- Header card -->
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:18px 22px;margin:16px 0 20px">
    <h2 style="color:#f0b429;margin:0 0 6px;font-size:1.2rem">🧙 Magic Formula — Top 20 Stocks</h2>
    <p style="color:#8b949e;font-size:12px;margin:0 0 10px">
      Based on <em>The Little Book That Still Beats the Market</em> by Joel Greenblatt (2006, updated 2010).<br>
      Ranks {eligible} eligible stocks on two metrics and picks the best combined rank.
      {excl_count} stocks excluded (Financials, Utilities, Real Estate per Greenblatt).
      Generated: {ts}
    </p>
    <div style="display:flex;gap:30px;flex-wrap:wrap;font-size:12px">
      <div>
        <strong style="color:#58a6ff">📊 Earnings Yield</strong> = EBIT / Enterprise Value<br>
        <span style="color:#8b949e">Proxy: 1 ÷ (EV/EBITDA) or 1 ÷ (Fwd P/E). Higher = cheaper.</span>
      </div>
      <div>
        <strong style="color:#3fb950">🏭 Return on Capital</strong> = EBIT / (Net Working Capital + Net Fixed Assets)<br>
        <span style="color:#8b949e">Proxy: ROA (or ROE fallback). Higher = better capital efficiency.</span>
      </div>
      <div>
        <strong style="color:#f0b429">🏆 Combined Rank</strong> = EY Rank + ROC Rank (lower is better)<br>
        <span style="color:#8b949e">Greenblatt: buy top 20–30 yearly, hold 1 year, rebalance.</span>
      </div>
    </div>
    <p style="color:#6e7681;font-size:11px;margin:10px 0 0">
      ⚠️ Quantitative screen only — not financial advice. Data from yfinance; EY &amp; ROC are proxies, not exact Greenblatt calculations.
      Always verify with official filings before investing.
    </p>
  </div>

  <!-- Top 20 table -->
  <div style="overflow-x:auto;border:1px solid #21262d;border-radius:8px">
  <table style="border-collapse:collapse;width:100%;white-space:nowrap;font-size:13px">
  <thead>
    <tr style="background:#161b22;color:#8b949e">
      <th style="padding:9px 8px">Rank</th>
      <th style="padding:9px 8px">Ticker</th>
      <th style="padding:9px 8px;color:#f0b429">Combined Rank ↑</th>
      <th style="padding:9px 8px;color:#58a6ff">Earnings Yield</th>
      <th style="padding:9px 8px;color:#3fb950">Return on Capital</th>
      <th style="padding:9px 8px">Price</th>
      <th style="padding:9px 8px">Mkt Cap</th>
      <th style="padding:9px 8px">Analyst Upside</th>
      <th style="padding:9px 8px">Our Score</th>
      <th style="padding:9px 8px">Flags</th>
      <th style="padding:9px 8px">Sector</th>
    </tr>
  </thead>
  <tbody>
{rows}
  </tbody>
  </table>
  </div>

  <!-- Greenblatt methodology note -->
  <div style="background:#0d1f2e;border:1px solid #1f6feb44;border-radius:8px;padding:14px 18px;margin-top:18px;font-size:12px;color:#8b949e">
    <strong style="color:#58a6ff">📖 How Greenblatt's Magic Formula Works</strong><br><br>
    1. Screen for stocks with market cap &gt; $50M (we screen 344 curated stocks).<br>
    2. Exclude utilities, financials, and foreign ADRs (excluded {excl_count} here).<br>
    3. Rank all remaining stocks by <strong>Earnings Yield</strong> (highest = rank 1).<br>
    4. Rank all remaining stocks by <strong>Return on Capital</strong> (highest = rank 1).<br>
    5. Add the two ranks together. <strong>Lowest combined rank = best Magic Formula stock.</strong><br>
    6. Buy the top 20–30 stocks. Hold for 1 year. Rebalance annually.<br><br>
    <span style="color:#6e7681">Greenblatt's backtest (1988–2004) showed ~30.8% annual return vs S&amp;P 500's 12.4%. Past performance does not guarantee future results.</span>
  </div>
</div>
""".format(
        eligible=total_eligible,
        excl_count=excl_count,
        ts=ts,
        rows=rows_html,
    )
    return html


# =============================================================================
# TOP 10 RECOMMENDATIONS
# =============================================================================
def generate_top10_recommendations(df, n=10):
    """
    Builds a conviction-scored Top 10 list with:
      - sector diversification cap (max 2 per sector)
      - minimum analyst coverage filter
      - layered conviction scoring on top of base score
    """
    def _val(v):
        return None if _is_nan(v) else v

    exclude = {t.upper() for t in EXCLUDE_TICKERS}
    rows = []

    for _, r in df.iterrows():
        ticker = str(r["Ticker"])
        if ticker in exclude:
            continue
        score  = r["Score"]
        if score < 55:
            break  # df is sorted desc — everything below 55 filtered out

        action = str(r.get("Action", ""))
        if action not in ("STRONG BUY", "BUY"):
            continue

        sector         = str(r.get("Sector") or "Unknown")
        analyst_count  = _val(r.get("Analyst_Count"))
        if analyst_count is not None and analyst_count < 3:
            continue

        price    = _val(r.get("Price"))
        upside   = _val(r.get("Analyst_Upside"))
        roe      = _val(r.get("ROE"))
        fcf      = _val(r.get("FCF_Yield"))
        rev_g    = _val(r.get("Rev_Growth"))
        rev_prev = _val(r.get("Rev_Growth_Prev"))
        de       = _val(r.get("Debt_Equity"))
        beta     = _val(r.get("Beta"))
        peg      = _val(r.get("PEG"))
        op_m     = _val(r.get("Op_Margin"))
        gross_m  = _val(r.get("Gross_Margin"))
        eps_s    = _val(r.get("EPS_Surprise"))
        insider  = _val(r.get("Insider_Buy_Pct"))
        from_low = _val(r.get("From_Low_Pct"))
        moat_score = _val(r.get("Moat_Score"))
        moat_label = str(r.get("Moat_Label") or "None")

        # ── Conviction score ─────────────────────────────────────────────
        conv = score
        if upside is not None:
            if   upside > 50:  conv += 12
            elif upside > 30:  conv += 8
            elif upside > 15:  conv += 4
            elif upside < 0:   conv -= 10
        if roe is not None and fcf is not None:
            if   roe > 25 and fcf > 5:   conv += 8
            elif roe > 15 and fcf > 3:   conv += 4
        if rev_g is not None and rev_prev is not None:
            accel = rev_g - rev_prev
            if   accel > 10:  conv += 6
            elif accel > 3:   conv += 3
            elif accel < -10: conv -= 5
        if eps_s is not None:
            if   eps_s > 15: conv += 4
            elif eps_s > 5:  conv += 2
            elif eps_s < -5: conv -= 4
        if de is not None and de < 0.3 and beta is not None and beta < 1.3:
            conv += 5
        if peg is not None and 0 < peg < 1.0:
            conv += 4
        if insider is not None and insider > 60:
            conv += 3
        if from_low is not None and from_low > 150:
            conv -= 4
        if analyst_count is not None and analyst_count < 6:
            conv -= 3
        # ── Moat bonus (structural advantage raises conviction) ───────────
        if moat_label == "Wide":
            conv += 10
        elif moat_label == "Narrow":
            conv += 5

        rows.append({
            "ticker": ticker, "name": str(r.get("Name", ticker)),
            "sector": sector, "action": action,
            "base_score": round(score, 1), "conv_score": round(conv, 1),
            "price": price, "target": _val(r.get("Analyst_Target")),
            "upside": upside, "roe": roe, "fcf": fcf,
            "rev_g": rev_g, "rev_prev": rev_prev,
            "de": de, "beta": beta, "peg": peg,
            "op_m": op_m, "gross_m": gross_m,
            "eps_s": eps_s, "analyst_count": analyst_count,
            "mkt_cap": str(r.get("Mkt_Cap") or "-"),
            "flags": str(r.get("Composite_Flag") or "—"),
            "moat_score": moat_score,
            "moat_label": moat_label,
        })

    rows.sort(key=lambda x: x["conv_score"], reverse=True)

    # Sector cap: max 2 per sector
    sector_counts, top10 = {}, []
    for row in rows:
        sec = row["sector"]
        cnt = sector_counts.get(sec, 0)
        if cnt >= 2:
            continue
        sector_counts[sec] = cnt + 1
        top10.append(row)
        if len(top10) == n:
            break

    return top10


def _fmt_pct(v, plus=True):
    if v is None: return "N/A"
    return ("{:+.1f}%" if plus else "{:.1f}%").format(v)

def _fmt_price(v):
    if v is None: return "N/A"
    return "${:.2f}".format(v)

def _accel_label(rev_g, rev_prev):
    if rev_g is None or rev_prev is None: return ""
    diff = rev_g - rev_prev
    if   diff >  5: return "  ▲ accelerating ({:+.1f}pp)".format(diff)
    elif diff < -5: return "  ▼ decelerating ({:+.1f}pp)".format(diff)
    return "  → stable"


def print_top10_report(top10, output_file=None):
    """Prints a formatted Top 10 narrative and optionally saves to file."""
    lines = []
    def p(s=""):
        lines.append(s)
        print(s)

    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "═" * 72
    p(); p(sep)
    p("  🏆  TOP 10 STOCK RECOMMENDATIONS")
    p("  Generated: {}".format(ts))
    p("  Methodology: Base score + analyst upside + quality + growth momentum")
    p("               Max 2 stocks per sector for diversification")
    p(sep); p()
    p("  ⚠️  DISCLAIMER: Quantitative screen only — not financial advice.")
    p("      Always do your own due diligence before investing."); p()

    for i, r in enumerate(top10, 1):
        accel  = _accel_label(r["rev_g"], r["rev_prev"])
        de_s   = "{:.2f}".format(r["de"])   if r["de"]   is not None else "N/A"
        beta_s = "{:.2f}".format(r["beta"]) if r["beta"] is not None else "N/A"
        peg_s  = "{:.2f}".format(r["peg"])  if r["peg"]  is not None else "N/A"
        roe_s  = _fmt_pct(r["roe"],   plus=False)
        fcf_s  = _fmt_pct(r["fcf"],   plus=False)
        rev_s  = _fmt_pct(r["rev_g"], plus=False)
        op_s   = _fmt_pct(r["op_m"],  plus=False)
        ac_s   = str(int(r["analyst_count"])) if r["analyst_count"] is not None else "N/A"
        eps_s  = _fmt_pct(r["eps_s"]) if r["eps_s"] is not None else "N/A"

        p("  ┌─ #{} {}  —  {}".format(i, r["ticker"], r["name"][:45]))
        p("  │  Sector: {:30}  Action: {}".format(r["sector"], r["action"]))
        p("  │  Base Score: {:5}   Conviction Score: {:5}   Mkt Cap: {}".format(
            r["base_score"], r["conv_score"], r["mkt_cap"]))
        p("  │")
        p("  │  💰 PRICE & TARGET")
        p("  │     Current Price : {}".format(_fmt_price(r["price"])))
        p("  │     Analyst Target: {}  ({} analysts)".format(_fmt_price(r["target"]), ac_s))
        p("  │     Upside        : {}".format(_fmt_pct(r["upside"])))
        p("  │")
        p("  │  📊 QUALITY & PROFITABILITY")
        p("  │     ROE            : {}".format(roe_s))
        p("  │     FCF Yield      : {}".format(fcf_s))
        p("  │     Operating Mgn  : {}".format(op_s))
        p("  │     Gross Margin   : {}".format(_fmt_pct(r["gross_m"], plus=False)))
        p("  │")
        p("  │  🚀 GROWTH")
        p("  │     Revenue Growth : {}{}".format(rev_s, accel))
        p("  │     EPS Surprise   : {}".format(eps_s))
        p("  │")
        p("  │  🛡️  RISK")
        p("  │     Debt/Equity    : {}".format(de_s))
        p("  │     Beta           : {}".format(beta_s))
        p("  │     PEG Ratio      : {}".format(peg_s))
        p("  │")
        p("  │  🏷️  SIGNAL FLAGS")
        p("  │     {}".format(r["flags"] if r["flags"] and r["flags"] != "—" else "No flags"))
        p("  └" + "─" * 68); p()

    p(); p("  SUMMARY TABLE"); p("  " + "─" * 68)
    p("  {:<3} {:<7} {:<22} {:>5} {:>7} {:>6} {:>5} {:<22}".format(
        "#", "Ticker", "Name", "Score", "Upside", "ROE%", "D/E", "Sector"))
    p("  " + "─" * 68)
    for i, r in enumerate(top10, 1):
        p("  {:<3} {:<7} {:<22} {:>5} {:>6}% {:>5}% {:>5} {:<22}".format(
            i, r["ticker"], r["name"][:21], r["base_score"],
            "{:.1f}".format(r["upside"])  if r["upside"] is not None else " N/A",
            "{:.0f}".format(r["roe"])     if r["roe"]    is not None else " N/A",
            "{:.2f}".format(r["de"])      if r["de"]     is not None else " N/A",
            r["sector"][:21]))
    p("  " + "─" * 68); p()

    if output_file:
        with open(output_file, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))
        print("  ✅  Top 10 report → {}\n".format(output_file))

    return "\n".join(lines)


# =============================================================================
# HTML REPORT
# =============================================================================

def _build_top10_tab(df):
    """Build HTML content for the Top 10 Picks tab."""
    top10 = generate_top10_recommendations(df, n=10)
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not top10:
        return '<div id="tab-top10" class="tab-panel"><div class="tab-content"><p style="color:#6e7681">No picks meet the quality threshold yet.</p></div></div>'

    cards_html = ""
    for i, r in enumerate(top10, 1):
        medal  = {1:"🥇",2:"🥈",3:"🥉"}.get(i,"#{:02d}".format(i))
        action = r["action"]
        a_cls  = {"STRONG BUY":"bg-info","BUY":"bg-success"}.get(action,"bg-warning")

        def _pct(v,plus=False):
            if v is None: return "-"
            return ("{:+.1f}%" if plus else "{:.1f}%").format(v)
        def _p(v):
            return "${:.2f}".format(v) if v is not None else "-"
        def _f(v,d=2):
            return "{:.{}f}".format(v,d) if v is not None else "-"

        upside_s = _pct(r["upside"],plus=True)
        upside_c = "#3fb950" if (r["upside"] and r["upside"]>=0) else "#f85149"

        accel_html = ""
        if r["rev_g"] is not None and r["rev_prev"] is not None:
            diff = r["rev_g"] - r["rev_prev"]
            if diff > 5:   accel_html = ' <span style="color:#3fb950;font-size:10px">▲{:+.1f}pp</span>'.format(diff)
            elif diff < -5: accel_html = ' <span style="color:#f85149;font-size:10px">▼{:.1f}pp</span>'.format(diff)

        moat_s = "{} {:.0f}".format(r["moat_label"], r["moat_score"]) if r["moat_score"] is not None else r["moat_label"]

        cards_html += """
<div class="pick-card">
  <div class="pick-header">
    <div class="pick-title">
      <span class="pick-medal">{medal}</span>
      <div>
        <div class="pick-ticker">{ticker} <span class="badge {a_cls}" style="font-size:10px">{action}</span></div>
        <div class="pick-name">{name} &nbsp;·&nbsp; {sector}</div>
      </div>
    </div>
    <div class="pick-meta">
      <div class="pick-scores">Base <strong>{base}</strong> &nbsp; Conv <strong style="color:#f0b429">{conv}</strong></div>
      <div style="font-size:11px;color:#6e7681">{mkt_cap}</div>
    </div>
  </div>
  <div class="pick-grid">
    <div class="pick-box">
      <div class="pick-box-title">💰 Price &amp; Target</div>
      <div class="pick-kv"><span class="k">Price</span><span class="v">{price}</span></div>
      <div class="pick-kv"><span class="k">Target</span><span class="v">{target}</span></div>
      <div class="pick-kv"><span class="k">Analysts</span><span class="v">{ac}</span></div>
      <div class="pick-upside" style="color:{upside_c}">{upside}</div>
    </div>
    <div class="pick-box">
      <div class="pick-box-title">📊 Quality</div>
      <div class="pick-kv"><span class="k">ROE</span><span class="v">{roe}</span></div>
      <div class="pick-kv"><span class="k">FCF Yield</span><span class="v">{fcf}</span></div>
      <div class="pick-kv"><span class="k">Op Margin</span><span class="v">{op}</span></div>
      <div class="pick-kv"><span class="k">Gross Mgn</span><span class="v">{gross}</span></div>
    </div>
    <div class="pick-box">
      <div class="pick-box-title">🚀 Growth</div>
      <div class="pick-kv"><span class="k">Rev Growth</span><span class="v">{rev}{accel}</span></div>
      <div class="pick-kv"><span class="k">EPS Surprise</span><span class="v">{eps}</span></div>
    </div>
    <div class="pick-box">
      <div class="pick-box-title">🛡️ Risk</div>
      <div class="pick-kv"><span class="k">D/E</span><span class="v">{de}</span></div>
      <div class="pick-kv"><span class="k">Beta</span><span class="v">{beta}</span></div>
      <div class="pick-kv"><span class="k">PEG</span><span class="v">{peg}</span></div>
    </div>
    <div class="pick-box" style="grid-column:1/-1">
      <div class="pick-box-title">🏰 Moat &amp; Signals</div>
      <div style="font-size:12px;color:#e6b450;margin-bottom:4px">{moat}</div>
      <div style="font-size:11px;color:#6e7681;line-height:1.5">{flags}</div>
    </div>
  </div>
</div>""".format(
            medal=medal, ticker=r["ticker"], name=r["name"][:35],
            action=action, a_cls=a_cls, sector=r["sector"],
            base=r["base_score"], conv=r["conv_score"], mkt_cap=r["mkt_cap"],
            price=_p(r["price"]), target=_p(r["target"]),
            ac=str(int(r["analyst_count"])) if r["analyst_count"] is not None else "-",
            upside=upside_s, upside_c=upside_c,
            roe=_pct(r["roe"]), fcf=_pct(r["fcf"]), op=_pct(r["op_m"]),
            gross=_pct(r["gross_m"]),
            rev=_pct(r["rev_g"]), accel=accel_html,
            eps=("{:+.1f}%".format(r["eps_s"]) if r["eps_s"] is not None else "-"),
            de=_f(r["de"]), beta=_f(r["beta"]), peg=_f(r["peg"]),
            moat=moat_s,
            flags=r["flags"] if r["flags"]!="—" else "No signal flags",
        )

    return """<div id="tab-top10" class="tab-panel">
<div class="tab-content">
  <div class="info-card">
    <h2 style="color:#f0b429">🏆 Top 10 Stock Picks</h2>
    <p>Conviction-scored · max 2 per sector · analyst coverage ≥ 3 · base score ≥ 55<br>Generated: {ts}</p>
    <p style="font-size:11px;margin:0">⚠️ Quantitative screen only — not financial advice.</p>
  </div>
{cards}
</div></div>""".format(ts=ts, cards=cards_html)


def _build_moat_tab(df):
    """Build HTML content for the Moat Leaderboard tab."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    moat_df = df[df["Moat_Score"].notna()].copy()
    moat_df = moat_df[moat_df["Moat_Score"]>0].sort_values("Moat_Score",ascending=False).head(30)
    wide_c   = int((df["Moat_Label"]=="Wide").sum())
    narrow_c = int((df["Moat_Label"]=="Narrow").sum())
    weak_c   = int((df["Moat_Label"]=="Weak").sum())

    if moat_df.empty:
        return '<div id="tab-moat" class="tab-panel"><div class="tab-content"><p style="color:#6e7681">No moat data yet.</p></div></div>'

    rows_html = ""
    for i,(_, row) in enumerate(moat_df.iterrows(),1):
        ticker    = str(row.get("Ticker",""))
        name      = str(row.get("Name",ticker))
        sector    = str(row.get("Sector",""))
        ms        = row.get("Moat_Score"); ml = str(row.get("Moat_Label") or "None")
        brand     = row.get("Moat_Brand"); sw = row.get("Moat_Switching"); net = row.get("Moat_Network")
        au        = row.get("Analyst_Upside"); sc = row.get("Score")
        lc = {"Wide":"#f0b429","Narrow":"#58a6ff","Weak":"#8b949e"}.get(ml,"#6e7681")
        icon = {"Wide":"🏰","Narrow":"〰️","Weak":"🔹"}.get(ml,"·")
        au_cls = "analyst-up" if (au and not _is_nan(au) and au>=0) else "analyst-dn"

        def _bar(v, color):
            if v is None: return ""
            pct = max(0,min(100,float(v)))
            return '<div class="bar-wrap"><div class="bar-fill" style="background:{};width:{:.0f}%"></div></div>'.format(color,pct)

        rows_html += """<tr>
<td class="tc" style="color:#6e7681;font-size:11px;font-weight:600">{i}</td>
<td><strong style="font-size:13px">{ticker}</strong><br><span style="font-size:11px;color:#6e7681">{name}</span></td>
<td class="tc" style="color:{lc};font-weight:700">{icon} {ml}</td>
<td class="tc" style="font-weight:700;font-size:15px;color:{lc}">{ms}</td>
<td class="tc"><span style="color:#e6b450;font-size:12px">{brand}</span>{bbar}</td>
<td class="tc"><span style="color:#58a6ff;font-size:12px">{sw}</span>{swbar}</td>
<td class="tc"><span style="color:#3fb950;font-size:12px">{net}</span>{netbar}</td>
<td class="tc"><span class="{au_cls}">{au}</span></td>
<td class="tc"><span style="color:#cdd9e5">{sc}</span></td>
<td style="font-size:11px;color:#6e7681">{sector}</td>
</tr>""".format(
            i=i, ticker=ticker, name=name[:30], lc=lc, icon=icon, ml=ml,
            ms="{:.1f}".format(ms) if ms else "-",
            brand="{:.1f}".format(brand) if brand else "-",
            bbar=_bar(brand,"#e6b45066"),
            sw="{:.1f}".format(sw) if sw else "-",
            swbar=_bar(sw,"#58a6ff66"),
            net="{:.1f}".format(net) if net else "-",
            netbar=_bar(net,"#3fb95066"),
            au_cls=au_cls,
            au=("{:+.1f}%".format(au) if (au and not _is_nan(au)) else "-"),
            sc=str(sc) if sc else "-", sector=sector,
        )

    return """<div id="tab-moat" class="tab-panel">
<div class="tab-content">
  <div class="info-card">
    <h2 style="color:#e6b450">🏰 Moat Leaderboard — Top 30</h2>
    <p>Ranked by Economic Moat Score (0–100). Three pillars: Brand · Switching Costs · Network Effects<br>Generated: {ts}</p>
    <div class="pill-row">
      <div class="pill"><span class="p-num" style="color:#f0b429">{wide}</span><span class="p-lbl">🏰 Wide</span></div>
      <div class="pill"><span class="p-num" style="color:#58a6ff">{narrow}</span><span class="p-lbl">〰️ Narrow</span></div>
      <div class="pill"><span class="p-num" style="color:#8b949e">{weak}</span><span class="p-lbl">🔹 Weak</span></div>
    </div>
  </div>
  <div class="data-table-wrap">
  <table class="data-table">
  <thead><tr>
    <th>#</th><th>Ticker</th><th>Moat</th><th>Score</th>
    <th style="color:#e6b450">🏷 Brand</th>
    <th style="color:#58a6ff">🔄 Switch</th>
    <th style="color:#3fb950">🌐 Network</th>
    <th>Analyst ↑</th><th>Score</th><th>Sector</th>
  </tr></thead>
  <tbody>{rows}</tbody>
  </table>
  </div>
  <div class="note-card">
    <strong>Pillar Methodology</strong> — each 0–33 pts &nbsp;·&nbsp;
    Wide ≥ 65 &nbsp;·&nbsp; Narrow ≥ 45 &nbsp;·&nbsp; Weak ≥ 28<br>
    <span style="color:#e6b450">Brand</span>: gross margin vs sector, op margin, FCF &nbsp;·&nbsp;
    <span style="color:#58a6ff">Switch</span>: rev stability, ROE consistency, debt &nbsp;·&nbsp;
    <span style="color:#3fb950">Network</span>: rev growth, accel, inst ownership
  </div>
</div></div>""".format(ts=ts,wide=wide_c,narrow=narrow_c,weak=weak_c,rows=rows_html)


def _build_hypergrowth_tab(df):
    """Build HTML content for the Hypergrowth Hunter tab."""
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    hg_df = df[df["HG_Score"].notna()].copy()
    hg_df = hg_df[hg_df["HG_Score"]>0].sort_values("HG_Score",ascending=False).head(25)
    rocket_c   = int((df["HG_Label"]=="🚀 Rocket").sum())
    high_c     = int((df["HG_Label"]=="🔥 High").sum())
    emerging_c = int((df["HG_Label"]=="📈 Emerging").sum())

    if hg_df.empty:
        return '<div id="tab-hg" class="tab-panel"><div class="tab-content"><p style="color:#6e7681">No hypergrowth data yet.</p></div></div>'

    rows_html = ""
    for i,(_, row) in enumerate(hg_df.iterrows(),1):
        ticker   = str(row.get("Ticker",""))
        name     = str(row.get("Name",ticker))
        sector   = str(row.get("Sector",""))
        hg       = row.get("HG_Score"); hl = str(row.get("HG_Label") or "—")
        gr       = row.get("HG_Growth"); lv = row.get("HG_Leverage")
        pmf      = row.get("HG_PMF");   disc = row.get("HG_Discovery")
        rev_g    = row.get("Rev_Growth"); r40 = row.get("Rule_Of_40")
        streak   = row.get("Rev_Accel_Streak")
        au       = row.get("Analyst_Upside"); sc = row.get("Score")
        lc = {"🚀 Rocket":"#f0b429","🔥 High":"#f85149","📈 Emerging":"#3fb950"}.get(hl,"#8b949e")
        au_cls = "analyst-up" if (au and not _is_nan(au) and au>=0) else "analyst-dn"

        def _bar(v, color, mx=25):
            if v is None: return ""
            pct = max(0,min(100,(float(v)/mx)*100))
            return '<div class="bar-wrap"><div class="bar-fill" style="background:{};width:{:.0f}%"></div></div>'.format(color,pct)

        rows_html += """<tr>
<td class="tc" style="color:#6e7681;font-size:11px;font-weight:600">{i}</td>
<td><strong style="font-size:13px">{ticker}</strong><br><span style="font-size:11px;color:#6e7681">{name}</span></td>
<td class="tc" style="color:{lc};font-weight:700;font-size:11px">{hl}</td>
<td class="tc" style="font-weight:700;font-size:15px;color:{lc}">{hg}</td>
<td class="tc"><span style="color:#f0b429;font-size:12px">{gr}</span>{grbar}</td>
<td class="tc"><span style="color:#58a6ff;font-size:12px">{lv}</span>{lvbar}</td>
<td class="tc"><span style="color:#3fb950;font-size:12px">{pmf}</span>{pmfbar}</td>
<td class="tc"><span style="color:#a371f7;font-size:12px">{disc}</span>{discbar}</td>
<td class="tc">{rev}</td>
<td class="tc">{r40}</td>
<td class="tc">{streak}</td>
<td class="tc"><span class="{au_cls}">{au}</span></td>
<td class="tc">{sc}</td>
<td style="font-size:11px;color:#6e7681">{sector}</td>
</tr>""".format(
            i=i,ticker=ticker,name=name[:30],lc=lc,hl=hl,
            hg="{:.1f}".format(hg) if hg else "-",
            gr="{:.1f}".format(gr) if gr else "-",   grbar=_bar(gr,"#f0b42966"),
            lv="{:.1f}".format(lv) if lv else "-",   lvbar=_bar(lv,"#58a6ff66"),
            pmf="{:.1f}".format(pmf) if pmf else "-",pmfbar=_bar(pmf,"#3fb95066"),
            disc="{:.1f}".format(disc) if disc else "-",discbar=_bar(disc,"#a371f766"),
            rev=("{:.1f}%".format(rev_g) if rev_g else "-"),
            r40=("{:.1f}".format(r40) if r40 else "-"),
            streak=("{}Q".format(int(streak)) if streak is not None else "-"),
            au_cls=au_cls,
            au=("{:+.1f}%".format(au) if (au and not _is_nan(au)) else "-"),
            sc=str(sc) if sc else "-",sector=sector,
        )

    return """<div id="tab-hg" class="tab-panel">
<div class="tab-content">
  <div class="info-card">
    <h2 style="color:#f85149">🚀 Hypergrowth Hunter — Top 25</h2>
    <p>Exceptional growth trajectories. Valuation-agnostic — early 10x stocks always look expensive.<br>Generated: {ts}</p>
    <div class="pill-row">
      <div class="pill"><span class="p-num" style="color:#f0b429">{rocket}</span><span class="p-lbl">🚀 Rocket</span></div>
      <div class="pill"><span class="p-num" style="color:#f85149">{high}</span><span class="p-lbl">🔥 High</span></div>
      <div class="pill"><span class="p-num" style="color:#3fb950">{emerging}</span><span class="p-lbl">📈 Emerging</span></div>
    </div>
  </div>
  <div class="data-table-wrap">
  <table class="data-table">
  <thead><tr>
    <th>#</th><th>Ticker</th><th>Label</th><th>HG Score</th>
    <th style="color:#f0b429">📈 Growth</th>
    <th style="color:#58a6ff">⚙️ Leverage</th>
    <th style="color:#3fb950">🎯 PMF</th>
    <th style="color:#a371f7">🔍 Discovery</th>
    <th>Rev Growth</th><th>Rule of 40</th><th>Accel</th>
    <th>Analyst ↑</th><th>Score</th><th>Sector</th>
  </tr></thead>
  <tbody>{rows}</tbody>
  </table>
  </div>
  <div class="note-card" style="background:#120810;border-color:#f8514933">
    <strong style="color:#f85149">Four pillars (25 pts each)</strong><br>
    <span style="color:#f0b429">📈 Growth</span>: Rev growth, vs sector, accel streak, GM expansion &nbsp;·&nbsp;
    <span style="color:#58a6ff">⚙️ Leverage</span>: Op leverage, Rule of 40, R&amp;D &nbsp;·&nbsp;
    <span style="color:#3fb950">🎯 PMF</span>: Deferred rev, EPS surprise, analyst conviction &nbsp;·&nbsp;
    <span style="color:#a371f7">🔍 Discovery</span>: Inst ownership, short squeeze, analyst count<br>
    🚀 Rocket ≥ 70 &nbsp;·&nbsp; 🔥 High ≥ 50 &nbsp;·&nbsp; 📈 Emerging ≥ 35
  </div>
</div></div>""".format(ts=ts,rocket=rocket_c,high=high_c,emerging=emerging_c,rows=rows_html)

    if not top10:
        return """<div id="tab-top10" class="tab-panel" style="display:none;padding:20px">
<p style="color:#8b949e">No Top 10 picks available — not enough stocks meet the quality threshold.</p></div>"""

    cards_html = ""
    for i, r in enumerate(top10, 1):
        medal   = {1: "🥇", 2: "🥈", 3: "🥉"}.get(i, "#{:02d}".format(i))
        action  = r["action"]
        a_color = {"STRONG BUY": "#1f6feb", "BUY": "#238636"}.get(action, "#9e6a03")
        a_bg    = {"STRONG BUY": "#0d2044", "BUY": "#0d2616"}.get(action, "#1e1600")

        upside_s = "{:+.1f}%".format(r["upside"]) if r["upside"] is not None else "N/A"
        upside_color = "#3fb950" if (r["upside"] and r["upside"] >= 0) else "#f85149"
        price_s  = "${:.2f}".format(r["price"])   if r["price"]  is not None else "N/A"
        target_s = "${:.2f}".format(r["target"])  if r["target"] is not None else "N/A"
        roe_s    = "{:.1f}%".format(r["roe"])     if r["roe"]    is not None else "-"
        fcf_s    = "{:.1f}%".format(r["fcf"])     if r["fcf"]    is not None else "-"
        rev_s    = "{:.1f}%".format(r["rev_g"])   if r["rev_g"]  is not None else "-"
        op_s     = "{:.1f}%".format(r["op_m"])    if r["op_m"]   is not None else "-"
        de_s     = "{:.2f}".format(r["de"])        if r["de"]     is not None else "-"
        beta_s   = "{:.2f}".format(r["beta"])      if r["beta"]   is not None else "-"
        peg_s    = "{:.2f}".format(r["peg"])       if r["peg"]    is not None else "-"
        eps_s    = "{:+.1f}%".format(r["eps_s"])  if r["eps_s"]  is not None else "-"
        ac_s     = str(int(r["analyst_count"]))   if r["analyst_count"] is not None else "-"
        moat_s   = "{} ({:.0f})".format(r["moat_label"], r["moat_score"]) if r["moat_score"] is not None else r["moat_label"]

        # Revenue acceleration indicator
        accel_html = ""
        if r["rev_g"] is not None and r["rev_prev"] is not None:
            diff = r["rev_g"] - r["rev_prev"]
            if diff > 5:
                accel_html = ' <span style="color:#3fb950;font-size:11px">▲ +{:.1f}pp</span>'.format(diff)
            elif diff < -5:
                accel_html = ' <span style="color:#f85149;font-size:11px">▼ {:.1f}pp</span>'.format(diff)

        cards_html += """
<div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:16px 20px;margin-bottom:14px">
  <div style="display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:10px">
    <!-- Left: ticker + name -->
    <div>
      <span style="font-size:1.3rem;font-weight:700;margin-right:8px">{medal}</span>
      <span style="font-size:1.15rem;font-weight:700;color:#c9d1d9">{ticker}</span>
      <span style="color:#8b949e;font-size:12px;margin-left:6px">{name}</span>
      <span style="background:{a_bg};color:{a_color};border:1px solid {a_color};border-radius:4px;font-size:11px;font-weight:700;padding:2px 8px;margin-left:8px">{action}</span>
    </div>
    <!-- Right: scores -->
    <div style="text-align:right;font-size:12px">
      <span style="color:#8b949e">Base Score: </span><strong style="color:#58a6ff">{base}</strong>
      &nbsp;·&nbsp;
      <span style="color:#8b949e">Conviction: </span><strong style="color:#f0b429">{conv}</strong>
      &nbsp;·&nbsp;
      <span style="color:#8b949e">{sector}</span>
      &nbsp;·&nbsp;
      <span style="color:#8b949e">{mkt_cap}</span>
    </div>
  </div>

  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:10px;margin-top:14px">
    <!-- Price & Target -->
    <div style="background:#0d1117;border-radius:6px;padding:10px 12px">
      <div style="color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">💰 Price &amp; Target</div>
      <div style="font-size:13px"><span style="color:#8b949e">Now:</span> <strong>{price}</strong></div>
      <div style="font-size:13px"><span style="color:#8b949e">Target:</span> <strong>{target}</strong> <small style="color:#8b949e">({ac} analysts)</small></div>
      <div style="font-size:15px;font-weight:700;color:{upside_color}">{upside}</div>
    </div>
    <!-- Quality -->
    <div style="background:#0d1117;border-radius:6px;padding:10px 12px">
      <div style="color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">📊 Quality</div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">ROE</span><strong>{roe}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">FCF Yield</span><strong>{fcf}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">Op Margin</span><strong>{op}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">Gross Mgn</span><strong>{gross}</strong></div>
    </div>
    <!-- Growth -->
    <div style="background:#0d1117;border-radius:6px;padding:10px 12px">
      <div style="color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">🚀 Growth</div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">Rev Growth</span><strong>{rev}{accel}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">EPS Surprise</span><strong>{eps}</strong></div>
    </div>
    <!-- Risk -->
    <div style="background:#0d1117;border-radius:6px;padding:10px 12px">
      <div style="color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">🛡️ Risk</div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">D/E</span><strong>{de}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">Beta</span><strong>{beta}</strong></div>
      <div style="font-size:12px;display:flex;justify-content:space-between"><span style="color:#8b949e">PEG</span><strong>{peg}</strong></div>
    </div>
    <!-- Moat & Flags -->
    <div style="background:#0d1117;border-radius:6px;padding:10px 12px">
      <div style="color:#8b949e;font-size:10px;text-transform:uppercase;letter-spacing:.5px;margin-bottom:4px">🏰 Moat &amp; Signals</div>
      <div style="font-size:12px;color:#e6b450;margin-bottom:4px">{moat}</div>
      <div style="font-size:11px;color:#8b949e;line-height:1.5">{flags}</div>
    </div>
  </div>
</div>""".format(
            medal=medal, ticker=r["ticker"], name=r["name"][:40],
            action=action, a_color=a_color, a_bg=a_bg,
            base=r["base_score"], conv=r["conv_score"],
            sector=r["sector"], mkt_cap=r["mkt_cap"],
            price=price_s, target=target_s, ac=ac_s,
            upside=upside_s, upside_color=upside_color,
            roe=roe_s, fcf=fcf_s, op=op_s,
            gross="{:.1f}%".format(r["gross_m"]) if r["gross_m"] is not None else "-",
            rev=rev_s, accel=accel_html, eps=eps_s,
            de=de_s, beta=beta_s, peg=peg_s,
            moat=moat_s, flags=r["flags"] if r["flags"] != "—" else "No signal flags",
        )

    return """<div id="tab-top10" class="tab-panel" style="display:none;padding:0 20px 30px">
  <div style="background:#161b22;border:1px solid #30363d;border-radius:10px;padding:14px 20px;margin:16px 0 20px">
    <h2 style="color:#f0b429;margin:0 0 4px;font-size:1.2rem">🏆 Top 10 Stock Picks</h2>
    <p style="color:#8b949e;font-size:12px;margin:0">Conviction-scored rankings · max 2 per sector · analyst coverage ≥ 3 · base score ≥ 55 · Generated: {ts}</p>
    <p style="color:#6e7681;font-size:11px;margin:6px 0 0">⚠️ Quantitative screen only — not financial advice. Always do your own due diligence.</p>
  </div>
{cards}
</div>""".format(ts=ts, cards=cards_html)


_JS = r"""
var sortDir = {};
function toggleAction(btn) { btn.classList.toggle('active'); applyFilters(); }
function toggleAll() {
    document.querySelectorAll('.act-btn[data-action]').forEach(function(b){ b.classList.remove('active'); });
    document.getElementById('btnAll').classList.add('active'); applyFilters();
}
function toggleSector(btn) { btn.classList.toggle('active'); applyFilters(); }
function toggleSecAll() {
    document.querySelectorAll('.sec-btn[data-sector]').forEach(function(b){ b.classList.remove('active'); });
    document.getElementById('btnSecAll').classList.add('active'); applyFilters();
}
function toggleMA(btn) { btn.classList.toggle('active'); applyFilters(); }
function applyFilters() {
    var q = document.getElementById('srch').value.toLowerCase().trim();
    var actBtns = Array.from(document.querySelectorAll('.act-btn[data-action].active'));
    var activeActions = actBtns.map(function(b){ return b.getAttribute('data-action'); });
    var allActions = document.getElementById('btnAll').classList.contains('active') || activeActions.length === 0;
    var secBtns = Array.from(document.querySelectorAll('.sec-btn[data-sector].active'));
    var activeSectors = secBtns.map(function(b){ return b.getAttribute('data-sector'); });
    var allSectors = document.getElementById('btnSecAll').classList.contains('active') || activeSectors.length === 0;
    var maBelow = document.getElementById('btnMABelow') && document.getElementById('btnMABelow').classList.contains('active');
    document.querySelectorAll('#tbody tr').forEach(function(r) {
        var ra = (r.getAttribute('data-action') || '').trim();
        var rs = (r.getAttribute('data-sector') || '').trim();
        var rma = (r.getAttribute('data-ma') || '').trim();
        var textMatch   = (!q || r.innerText.toLowerCase().indexOf(q) !== -1);
        var actionMatch = allActions || activeActions.indexOf(ra) !== -1;
        var sectorMatch = allSectors || activeSectors.indexOf(rs) !== -1;
        var maMatch     = !maBelow || rma === 'below';
        r.style.display = (textMatch && actionMatch && sectorMatch && maMatch) ? '' : 'none';
    });
    updateCount();
}
function sortCol(c) {
    var tbody = document.getElementById('tbody');
    var rows = Array.from(tbody.querySelectorAll('tr'));
    sortDir[c] = -(sortDir[c] || 1);
    rows.sort(function(a, b) {
        var av = a.cells[c] ? (a.cells[c].hasAttribute('data-sort') ? a.cells[c].getAttribute('data-sort') : a.cells[c].innerText.trim()) : '';
        var bv = b.cells[c] ? (b.cells[c].hasAttribute('data-sort') ? b.cells[c].getAttribute('data-sort') : b.cells[c].innerText.trim()) : '';
        var an = parseFloat(av.replace(/[^\d.\-]/g, ''));
        var bn = parseFloat(bv.replace(/[^\d.\-]/g, ''));
        if (!isNaN(an) && !isNaN(bn)) return sortDir[c] * (an - bn);
        return sortDir[c] * av.localeCompare(bv);
    });
    document.querySelectorAll('thead th').forEach(function(th, i) {
        th.classList.remove('asc', 'desc');
        if (i === c) th.classList.add(sortDir[c] === 1 ? 'desc' : 'asc');
    });
    rows.forEach(function(r){ tbody.appendChild(r); });
    updateCount();
}
function updateCount() {
    var vis = 0;
    document.querySelectorAll('#tbody tr').forEach(function(r){ if (r.style.display !== 'none') vis++; });
    document.getElementById('rowcnt').textContent = 'Showing ' + vis + ' of %%TOTAL%% stocks';
}
function exportCSV() {
    var hdrs = Array.from(document.querySelectorAll('thead th')).map(function(h){ return h.innerText.replace(/[▲▼]/g,'').trim(); });
    var rows = Array.from(document.querySelectorAll('#tbody tr')).filter(function(r){ return r.style.display !== 'none'; });
    var csv = hdrs.join(',') + '\n';
    rows.forEach(function(r){ csv += Array.from(r.cells).map(function(c){ return '"' + c.innerText.replace(/[\r\n]+/g,' ').trim() + '"'; }).join(',') + '\n'; });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], {type:'text/csv'}));
    a.download = 'portfolio_analysis.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
}
window.onload = updateCount;
"""

_CSS = """
/* ── Reset & Base ──────────────────────────────────────────────────────── */
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html{-webkit-text-size-adjust:100%}
body{background:#0a0d13;color:#cdd9e5;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;font-size:14px;margin:0;min-height:100vh}

/* ── Header ────────────────────────────────────────────────────────────── */
.site-header{background:linear-gradient(135deg,#0d1117 0%,#161b22 100%);border-bottom:1px solid #21262d;padding:16px 16px 12px;position:sticky;top:0;z-index:100}
.header-row{display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:8px}
.site-title{font-size:1.25rem;font-weight:700;color:#58a6ff;margin:0;letter-spacing:-.3px}
.version-badge{font-size:10px;background:#1f6feb;color:#fff;padding:2px 7px;border-radius:20px;margin-left:6px;vertical-align:middle;letter-spacing:.3px}
.header-meta{font-size:11px;color:#6e7681;margin:4px 0 0}

/* ── Stat row ──────────────────────────────────────────────────────────── */
.stat-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:12px}
.stat-card{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:10px 14px;flex:1 1 70px;min-width:60px;text-align:center}
.stat-card .num{font-size:1.4rem;font-weight:700;line-height:1;display:block}
.stat-card .lbl{font-size:10px;color:#6e7681;text-transform:uppercase;letter-spacing:.6px;margin-top:3px;display:block}

/* ── Tab bar ────────────────────────────────────────────────────────────── */
.tab-bar{display:flex;background:#0d1117;border-bottom:1px solid #21262d;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:none}
.tab-bar::-webkit-scrollbar{display:none}
.tab-btn{flex:0 0 auto;background:none;border:none;border-bottom:2px solid transparent;color:#6e7681;font-size:12px;font-weight:600;padding:12px 14px;cursor:pointer;white-space:nowrap;transition:color .15s,border-color .15s;letter-spacing:.2px}
.tab-btn:hover{color:#cdd9e5}
.tab-btn.active{color:#58a6ff;border-bottom-color:#58a6ff}
.tab-btn.t-top10.active{color:#f0b429;border-bottom-color:#f0b429}
.tab-btn.t-moat.active{color:#e6b450;border-bottom-color:#e6b450}
.tab-btn.t-hg.active{color:#f85149;border-bottom-color:#f85149}
.tab-btn.t-magic.active{color:#a371f7;border-bottom-color:#a371f7}
.tab-panel{display:none}

/* ── Screener toolbar ───────────────────────────────────────────────────── */
.screener-toolbar{padding:12px 16px 8px;display:flex;flex-direction:column;gap:10px}
.filter-group{display:flex;flex-direction:column;gap:6px}
.filter-label{font-size:10px;color:#6e7681;text-transform:uppercase;letter-spacing:.6px;font-weight:600}
.btn-row{display:flex;gap:6px;flex-wrap:wrap}
.act-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:8px;padding:7px 13px;cursor:pointer;font-size:12px;font-weight:600;transition:all .15s;white-space:nowrap;-webkit-appearance:none}
.act-btn:active{opacity:.8}
#btnAll.active{background:#21262d;color:#cdd9e5;border-color:#30363d}
.act-btn[data-action="STRONG BUY"].active{background:#1f4f98;color:#fff;border-color:#1f6feb}
.act-btn[data-action="BUY"].active{background:#1a3f27;color:#3fb950;border-color:#238636}
.act-btn[data-action="HOLD"].active{background:#3d2e00;color:#e3b341;border-color:#9e6a03}
.act-btn[data-action="SELL"].active{background:#3d0f0f;color:#f85149;border-color:#b62324}
.sec-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:6px;padding:5px 10px;cursor:pointer;font-size:11px;font-weight:500;white-space:nowrap;-webkit-appearance:none}
#btnSecAll.active,.sec-btn.active{background:#1c2a3d;color:#58a6ff;border-color:#1f6feb}
.search-row{display:flex;gap:8px;align-items:center}
input#srch{background:#161b22;border:1px solid #21262d;color:#cdd9e5;border-radius:8px;padding:9px 13px;flex:1;min-width:0;outline:none;font-size:14px;-webkit-appearance:none}
input#srch:focus{border-color:#58a6ff;background:#1c2230}
.btn-csv{background:#238636;color:#fff;border:none;border-radius:8px;padding:9px 14px;cursor:pointer;font-size:13px;font-weight:600;white-space:nowrap;-webkit-appearance:none}
#rowcnt{font-size:11px;color:#6e7681;padding:0 16px 6px}

/* ── Data table ─────────────────────────────────────────────────────────── */
.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-top:1px solid #21262d}
table{border-collapse:collapse;width:100%;white-space:nowrap;font-size:13px}
thead th{background:#161b22;color:#6e7681;position:sticky;top:0;z-index:9;padding:10px 8px;cursor:pointer;user-select:none;font-weight:600;font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #21262d}
thead th:hover{color:#58a6ff}
thead th.asc::after{content:" ▲";font-size:8px;opacity:.7}
thead th.desc::after{content:" ▼";font-size:8px;opacity:.7}
td{padding:8px 8px;border-bottom:1px solid #1a1f27;vertical-align:middle}
td small{color:#6e7681;font-size:11px}
.tc{text-align:center}.tr{text-align:right}
.row-green{background:#0b1d10}.row-green:hover{background:#0e2615}
.row-orange{background:#1a1200}.row-orange:hover{background:#221800}
.row-red{background:#160808}.row-red:hover{background:#1e0c0c}
.badge{font-size:11px;padding:3px 8px;border-radius:5px;font-weight:700;letter-spacing:.2px}
.score-strong{background:#1f4f98;color:#79c0ff}
.score-buy{background:#1a3f27;color:#56d364}
.score-hold{background:#3d2e00;color:#e3b341}
.score-sell{background:#3d0f0f;color:#f85149}
.bg-success{background:#1a3f27!important;color:#56d364!important}
.bg-info{background:#1f4f98!important;color:#79c0ff!important}
.bg-warning{background:#3d2e00!important;color:#e3b341!important}
.bg-danger{background:#3d0f0f!important;color:#f85149!important}
.ma-above .ma-val{color:#3fb950;font-weight:600}
.ma-below .ma-val{color:#f85149;font-weight:600}
.ma-near  .ma-val{color:#d29922;font-weight:600}
.div-cell{color:#e6b450;font-weight:600}
.flag-cell{font-size:11px;max-width:180px;white-space:normal;line-height:1.5;color:#8b949e}
.analyst-up{color:#3fb950;font-weight:600}
.analyst-dn{color:#f85149;font-weight:600}
.accel-up{color:#3fb950;font-size:11px}
.accel-dn{color:#f85149;font-size:11px}

/* ── Card panels (Top10, Moat, HG, Magic) ──────────────────────────────── */
.tab-content{padding:16px}
.info-card{background:#161b22;border:1px solid #21262d;border-radius:12px;padding:16px;margin-bottom:16px}
.info-card h2{margin:0 0 6px;font-size:1.05rem;font-weight:700}
.info-card p{color:#6e7681;font-size:12px;margin:0 0 8px;line-height:1.5}
.pill-row{display:flex;gap:8px;flex-wrap:wrap;margin-top:8px}
.pill{display:flex;flex-direction:column;align-items:center;background:#0d1117;border:1px solid #21262d;border-radius:8px;padding:8px 12px;min-width:60px}
.pill .p-num{font-size:1.2rem;font-weight:700;line-height:1}
.pill .p-lbl{font-size:10px;color:#6e7681;text-transform:uppercase;letter-spacing:.4px;margin-top:3px}
.note-card{background:#0d1520;border:1px solid #1f3a5f;border-radius:10px;padding:12px 14px;margin-top:12px;font-size:12px;color:#6e7681;line-height:1.6}
.note-card strong{color:#58a6ff}

/* ── Responsive data tables inside tabs ────────────────────────────────── */
.data-table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border:1px solid #21262d;border-radius:10px;margin-top:4px}
.data-table{border-collapse:collapse;width:100%;white-space:nowrap;font-size:12px}
.data-table thead th{background:#161b22;color:#6e7681;padding:10px 10px;font-size:10px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;border-bottom:1px solid #21262d;white-space:nowrap}
.data-table tbody td{padding:9px 10px;border-bottom:1px solid #1a1f27;vertical-align:middle}
.data-table tbody tr:last-child td{border-bottom:none}
.data-table tbody tr:hover{background:#1a1f2a}

/* ── Top10 pick cards ───────────────────────────────────────────────────── */
.pick-card{background:#161b22;border:1px solid #21262d;border-radius:12px;padding:16px;margin-bottom:12px;transition:border-color .15s}
.pick-card:hover{border-color:#30363d}
.pick-header{display:flex;align-items:flex-start;justify-content:space-between;flex-wrap:wrap;gap:8px;margin-bottom:14px}
.pick-title{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.pick-medal{font-size:1.3rem;line-height:1}
.pick-ticker{font-size:1.1rem;font-weight:700;color:#cdd9e5}
.pick-name{font-size:11px;color:#6e7681;margin-top:1px}
.pick-meta{display:flex;flex-direction:column;align-items:flex-end;gap:4px;flex-shrink:0}
.pick-scores{font-size:11px;color:#6e7681}
.pick-scores strong{color:#58a6ff}
.action-badge{font-size:10px;font-weight:700;padding:3px 9px;border-radius:5px;letter-spacing:.3px}
.pick-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:10px}
.pick-box{background:#0d1117;border:1px solid #1a1f27;border-radius:8px;padding:10px 12px}
.pick-box-title{font-size:9px;color:#6e7681;text-transform:uppercase;letter-spacing:.6px;font-weight:700;margin-bottom:7px}
.pick-kv{display:flex;justify-content:space-between;align-items:center;font-size:12px;margin-bottom:3px}
.pick-kv .k{color:#6e7681}
.pick-kv .v{font-weight:600;color:#cdd9e5}
.pick-upside{font-size:1.1rem;font-weight:700;margin-top:4px}

/* ── Pillar mini-bars ────────────────────────────────────────────────────── */
.bar-wrap{margin-top:4px;height:4px;background:#1a1f27;border-radius:2px;overflow:hidden}
.bar-fill{height:4px;border-radius:2px;transition:width .3s}

/* ── Disclaimer ─────────────────────────────────────────────────────────── */
.disclaimer{font-size:11px;color:#6e7681;padding:10px 16px 20px;line-height:1.5}

/* ── Mobile overrides ───────────────────────────────────────────────────── */
@media(max-width:600px){
  .site-title{font-size:1.05rem}
  .stat-card .num{font-size:1.1rem}
  .stat-card{padding:8px 10px}
  .tab-btn{font-size:11px;padding:10px 11px}
  .pick-grid{grid-template-columns:1fr 1fr}
  input#srch{font-size:16px}/* prevents iOS zoom */
  .data-table{font-size:11px}
  .data-table thead th{padding:8px 7px}
  .data-table tbody td{padding:7px 7px}
}
"""


def _ma200_cell(row):
    ma = row.get("MA200"); vs = row.get("Vs_MA200")
    if ma is None or vs is None: return '<td class="tc" data-sort="">-</td>'
    ms = "${:.2f}".format(ma); vs_s = "{:+.1f}%".format(vs)
    sort_attr = ' data-sort="{}"'.format(vs)
    if vs < -5:
        return '<td class="tc ma-below"{}><span class="ma-val">{}</span><br><small>{}</small></td>'.format(sort_attr, ms, vs_s)
    elif vs < 0:
        return '<td class="tc ma-near"{}><span class="ma-val">{}</span><br><small>{}</small></td>'.format(sort_attr, ms, vs_s)
    return '<td class="tc ma-above"{}><span class="ma-val">{}</span><br><small>{}</small></td>'.format(sort_attr, ms, vs_s)


def _analyst_cell(row):
    au = row.get("Analyst_Upside"); tgt = row.get("Analyst_Target"); ac = row.get("Analyst_Count")
    if tgt is None: return '<td class="tc">-</td>', '<td class="tc">-</td>'
    tgt_str = "${:.2f}".format(tgt)
    ac_str  = " <small>({} analysts)</small>".format(int(ac)) if (ac and not _is_nan(ac)) else ""
    if au is None: return '<td class="tc">{}{}</td>'.format(tgt_str, ac_str), '<td class="tc">-</td>'
    cls = "analyst-up" if au >= 0 else "analyst-dn"
    return ('<td class="tc">{}{}</td>'.format(tgt_str, ac_str),
            '<td class="tc"><span class="{}">{:+.1f}%</span></td>'.format(cls, au))


def _rev_growth_cell(row):
    rg = row.get("Rev_Growth"); rg_prev = row.get("Rev_Growth_Prev")
    if rg is None or _is_nan(rg): return '<td class="tc">-</td>'
    s = "{:.1f}%".format(rg)
    if rg_prev is not None and not _is_nan(rg_prev):
        accel = rg - rg_prev
        arrow = "▲" if accel > 2 else ("▼" if accel < -2 else "→")
        cls   = "accel-up" if accel > 2 else ("accel-dn" if accel < -2 else "")
        s += '<br><small class="{}">{} {:.1f}pp</small>'.format(cls, arrow, accel)
    return '<td class="tc">' + s + '</td>'


def _build_rows(df):
    rows_html = ""
    for _, row in df.iterrows():
        rec, badge_cls, color_group = get_recommendation(row["Score"])
        row_cls = {"GREEN": "row-green", "ORANGE": "row-orange", "RED": "row-red"}[color_group]
        s       = row["Score"]
        sector_val = str(row.get("Sector") or "")
        ticker_val = str(row["Ticker"])
        name_val   = str(row.get("Name") or ticker_val)
        flag_val   = str(row.get("Composite_Flag") or "—")
        vs200_val  = row.get("Vs_MA200")
        ma_state   = "below" if (vs200_val is not None and vs200_val < -5) else "above"

        if   s >= 78: sc = '<span class="badge score-strong">' + str(s) + "</span>"
        elif s >= 62: sc = '<span class="badge score-buy">'    + str(s) + "</span>"
        elif s >= 44: sc = '<span class="badge score-hold">'   + str(s) + "</span>"
        else:         sc = '<span class="badge score-sell">'   + str(s) + "</span>"
        ac_badge = '<span class="badge {}">{}</span>'.format(badge_cls, rec)

        div_val  = row.get("Div_Yield")
        div_str  = fmt(div_val, suffix="%", decimals=2)
        div_cell = ('<td class="tc div-cell">' if (div_val and div_val > 0) else '<td class="tc">') + div_str + "</td>"

        eps_s     = row.get("EPS_Surprise")
        eps_s_str = ('{:+.1f}%'.format(eps_s) if (eps_s is not None and not _is_nan(eps_s)) else '-')
        eps_s_cls = ("analyst-up" if (eps_s and not _is_nan(eps_s) and eps_s > 0) else
                     "analyst-dn" if (eps_s and not _is_nan(eps_s) and eps_s < 0) else "")
        eps_s_cell = '<td class="tc"><span class="{}">{}</span></td>'.format(eps_s_cls, eps_s_str)

        analyst_tgt_cell, analyst_up_cell = _analyst_cell(row)

        cells = (
            "<td><strong>{}</strong><br><small>{}</small></td>".format(ticker_val, name_val)
            + '<td class="tc">' + sc + "</td>"
            + '<td class="tc">' + ac_badge + "</td>"
            + '<td class="tc">' + fmt(row.get("Price"), prefix="$") + "</td>"
            + analyst_tgt_cell + analyst_up_cell
            + '<td class="tc">' + fmt(row.get("Mkt_Cap")) + "</td>"
            + '<td class="tc">' + fmt(row.get("PE_Fwd")) + "</td>"
            + '<td class="tc">' + fmt(row.get("EV_EBITDA")) + "</td>"
            + '<td class="tc">' + fmt(row.get("PEG")) + "</td>"
            + '<td class="tc">' + fmt(row.get("ROE"), suffix="%") + "</td>"
            + _rev_growth_cell(row)
            + eps_s_cell
            + '<td class="tc">' + fmt(row.get("Gross_Margin"), suffix="%") + "</td>"
            + '<td class="tc">' + fmt(row.get("Op_Margin"), suffix="%") + "</td>"
            + '<td class="tc">' + fmt(row.get("FCF_Yield"), suffix="%") + "</td>"
            + '<td class="tc">' + fmt(row.get("Debt_Equity")) + "</td>"
            + '<td class="tc">' + fmt(row.get("Beta")) + "</td>"
            + div_cell
            + _ma200_cell(row)
            + '<td class="tc">' + fmt(row.get("From_Low_Pct"), suffix="%") + "</td>"
            + '<td class="tc">' + fmt(row.get("Short_Float"), suffix="%") + "</td>"
            + '<td class="tc flag-cell">' + flag_val + "</td>"
            + '<td class="tc">' + sector_val + "</td>"
        )
        rows_html += '<tr class="{}" data-action="{}" data-sector="{}" data-ma="{}">{}</tr>\n'.format(
            row_cls, rec, sector_val, ma_state, cells)
    return rows_html


def generate_html_report(df):
    total   = len(df)
    ts      = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sb      = int((df["Action"] == "STRONG BUY").sum())
    b       = int((df["Action"] == "BUY").sum())
    h       = int((df["Action"] == "HOLD").sum())
    s       = int((df["Action"] == "SELL").sum())
    avg_sc  = round(df["Score"].mean(), 1)
    sectors = sorted(df["Sector"].dropna().unique().tolist())

    sec_btns = '<button id="btnSecAll" class="sec-btn" onclick="toggleSecAll()">All</button>\n'
    for sec in sectors:
        sec_btns += '<button class="sec-btn" data-sector="{}" onclick="toggleSector(this)">{}</button>\n'.format(sec, sec)

    rows_html = _build_rows(df)
    js = _JS.replace("%%TOTAL%%", str(total))

    headers = [
        "Ticker", "Score", "Action", "Price", "Analyst Target", "Upside",
        "Mkt Cap", "Fwd P/E", "EV/EBITDA", "PEG", "ROE%",
        "Rev Growth", "EPS Surprise", "Gross Mgn%", "Op Mgn%", "FCF Yield%",
        "D/E", "Beta", "Div Yield%", "200 DMA", "From Low%",
        "Short Float%", "Flags", "Sector",
    ]
    th_row = "".join('<th onclick="sortCol({})">{}</th>'.format(i, hdr) for i, hdr in enumerate(headers))

    top10_tab_html = _build_top10_tab(df)
    moat_tab_html  = _build_moat_tab(df)
    hg_tab_html    = _build_hypergrowth_tab(df)
    magic_tab_html = _build_magic_formula_tab(df)

    tab_js = """
function switchTab(id,btn){
  document.querySelectorAll('.tab-panel').forEach(function(p){p.style.display='none';});
  document.querySelectorAll('.tab-btn').forEach(function(b){b.classList.remove('active');});
  var p=document.getElementById(id);if(p)p.style.display='block';
  btn.classList.add('active');
  window.scrollTo({top:0,behavior:'smooth'});
}
"""

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0a0d13">
<meta name="apple-mobile-web-app-capable" content="yes">
<title>Portfolio Analysis</title>
<style>{css}</style>
</head>
<body>

<!-- ══ STICKY HEADER ══════════════════════════════════════════════════════ -->
<header class="site-header">
  <div class="header-row">
    <h1 class="site-title">📊 Portfolio Analysis <span class="version-badge">v3</span></h1>
    <div style="font-size:11px;color:#6e7681">{ts}</div>
  </div>
  <div class="stat-row">
    <div class="stat-card"><span class="num" style="color:#79c0ff">{sb}</span><span class="lbl">Strong Buy</span></div>
    <div class="stat-card"><span class="num" style="color:#56d364">{b}</span><span class="lbl">Buy</span></div>
    <div class="stat-card"><span class="num" style="color:#e3b341">{h}</span><span class="lbl">Hold</span></div>
    <div class="stat-card"><span class="num" style="color:#f85149">{s}</span><span class="lbl">Sell</span></div>
    <div class="stat-card"><span class="num">{avg_sc}</span><span class="lbl">Avg Score</span></div>
    <div class="stat-card"><span class="num">{total}</span><span class="lbl">Stocks</span></div>
  </div>
</header>

<!-- ══ TAB BAR ════════════════════════════════════════════════════════════ -->
<nav class="tab-bar" role="tablist">
  <button class="tab-btn active"   role="tab" onclick="switchTab('tab-screener',this)">📊 Screener</button>
  <button class="tab-btn t-top10"  role="tab" onclick="switchTab('tab-top10',this)">🏆 Top 10</button>
  <button class="tab-btn t-moat"   role="tab" onclick="switchTab('tab-moat',this)">🏰 Moat</button>
  <button class="tab-btn t-hg"     role="tab" onclick="switchTab('tab-hg',this)">🚀 Hypergrowth</button>
  <button class="tab-btn t-magic"  role="tab" onclick="switchTab('tab-magic',this)">🧙 Magic Formula</button>
</nav>

<!-- ══ TAB 1: FULL SCREENER ══════════════════════════════════════════════ -->
<div id="tab-screener" class="tab-panel" style="display:block">
  <div class="screener-toolbar">
    <div class="filter-group">
      <div class="filter-label">Action</div>
      <div class="btn-row">
        <button id="btnAll" class="act-btn active" onclick="toggleAll()">All</button>
        <button class="act-btn" data-action="STRONG BUY" onclick="toggleAction(this)">⭐ Strong Buy</button>
        <button class="act-btn" data-action="BUY" onclick="toggleAction(this)">✅ Buy</button>
        <button class="act-btn" data-action="HOLD" onclick="toggleAction(this)">⏸ Hold</button>
        <button class="act-btn" data-action="SELL" onclick="toggleAction(this)">🔴 Sell</button>
      </div>
    </div>
    <div class="filter-group">
      <div class="filter-label">Sector</div>
      <div class="btn-row">{sec_btns}</div>
    </div>
    <div class="search-row">
      <input id="srch" type="search" autocomplete="off" autocorrect="off" spellcheck="false"
             placeholder="Search ticker or name…" oninput="applyFilters()">
      <button class="btn-csv" onclick="exportCSV()">⬇ CSV</button>
    </div>
  </div>
  <div id="rowcnt" style="font-size:11px;color:#6e7681;padding:0 16px 6px"></div>
  <div class="table-wrap">
  <table>
    <thead><tr>{th_row}</tr></thead>
    <tbody id="tbody">{rows_html}</tbody>
  </table>
  </div>
  <p class="disclaimer">⚠️ Scores are a quantitative screening tool only — not financial advice. Always do qualitative due diligence before investing.</p>
</div>

<!-- ══ TAB 2–5 ════════════════════════════════════════════════════════════ -->
{top10_tab_html}
{moat_tab_html}
{hg_tab_html}
{magic_tab_html}

<script>{js}{tab_js}</script>
</body>
</html>""".format(
        css=_CSS, ts=ts, total=total, sb=sb, b=b, h=h, s=s, avg_sc=avg_sc,
        sec_btns=sec_btns, th_row=th_row, rows_html=rows_html,
        js=js, tab_js=tab_js,
        top10_tab_html=top10_tab_html, moat_tab_html=moat_tab_html,
        hg_tab_html=hg_tab_html, magic_tab_html=magic_tab_html,
    )


# =============================================================================
# EMAIL — BUILD & SEND
# =============================================================================
def build_email(success: bool, top10_text: str = "") -> MIMEMultipart:
    today     = datetime.now().strftime("%A, %B %d, %Y")
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    subject   = "{} Portfolio Analysis — {}".format("✅" if success else "❌", today)

    msg = MIMEMultipart("mixed")
    msg["From"]    = GMAIL_SENDER
    msg["To"]      = ", ".join(EMAIL_RECIPIENTS)
    msg["Subject"] = subject

    if not success:
        msg.attach(MIMEText(
            "Portfolio Analyzer run FAILED on {}.\nCheck {} for details.\n".format(today, LOG_FILE),
            "plain"
        ))
        return msg

    alt = MIMEMultipart("alternative")

    # Plain-text fallback
    plain_lines = ["Portfolio Analysis Report — {}".format(today), "=" * 60, ""]
    if INLINE_TOP10 and top10_text:
        plain_lines.append(top10_text)
    else:
        plain_lines.append("See the attached HTML report for full results.")
    plain_lines += ["", "─" * 60,
                    "NOTE: Scores are a screening tool, not financial advice.",
                    "Generated: {}".format(timestamp)]
    alt.attach(MIMEText("\n".join(plain_lines), "plain"))

    # HTML body
    if INLINE_HTML_BODY and HTML_FILE.exists():
        html_body = HTML_FILE.read_text(encoding="utf-8")
        log.info("   🌐 Inlining full HTML report as email body.")
    else:
        escaped = (top10_text or "No report generated.").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        html_body = """<!DOCTYPE html><html><head><meta charset="utf-8">
<style>body{{font-family:monospace;background:#0f1117;color:#e0e0e0;padding:24px}}
pre{{white-space:pre-wrap;font-size:13px;line-height:1.5}}
.note{{color:#888;font-size:12px;margin-top:24px;border-top:1px solid #333;padding-top:12px}}
</style></head><body>
<h2 style="color:#7ec8e3;">📊 Portfolio Analysis — {today}</h2>
<pre>{escaped}</pre>
<p class="note">Scores are a screening tool, not financial advice.<br>Generated: {ts}</p>
</body></html>""".format(today=today, escaped=escaped, ts=timestamp)
    alt.attach(MIMEText(html_body, "html"))
    msg.attach(alt)

    # HTML attachment
    if ATTACH_HTML and HTML_FILE.exists():
        dated_name = "portfolio_analysis_{}.html".format(datetime.now().strftime("%Y%m%d"))
        with open(HTML_FILE, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
        encoders.encode_base64(part)
        part.add_header("Content-Disposition", 'attachment; filename="{}"'.format(dated_name))
        msg.attach(part)
        log.info("   📎 Attached: %s", dated_name)

    return msg


def send_email(msg: MIMEMultipart) -> bool:
    log.info("📧  Sending email to: %s", ", ".join(EMAIL_RECIPIENTS))
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(GMAIL_SENDER, GMAIL_APP_PASSWORD)
            server.sendmail(GMAIL_SENDER, EMAIL_RECIPIENTS, msg.as_string())
        log.info("✅  Email sent successfully.")
        return True
    except smtplib.SMTPAuthenticationError:
        log.error("❌  Gmail authentication failed. Check GMAIL_SENDER and GMAIL_APP_PASSWORD.")
        log.error("    Generate an App Password at: https://myaccount.google.com/apppasswords")
        return False
    except Exception as e:
        log.error("❌  Failed to send email: %s", e)
        traceback.print_exc()
        return False


# =============================================================================
# CORE RUN FUNCTION
# =============================================================================
def run(send_email_after: bool = True) -> bool:
    """
    Full pipeline: fetch → score → outputs → (optional) email.
    Returns True on success.
    """
    n = len(PORTFOLIO_TICKERS)
    print("\n" + "=" * 64)
    print("  PORTFOLIO MASTER — {} STOCKS".format(n))
    print("  Weighted · Sector-Relative · Analyst Consensus · Email-Ready")
    print("=" * 64)
    print("  Started : {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("  Workers : 8 parallel threads (rate-limit safe)\n")

    success = False
    top10_text = ""
    try:
        # ── 1. Fetch (pass 1) ─────────────────────────────────────────────
        print("  Fetching market data (pass 1)...")
        records = fetch_all_parallel(PORTFOLIO_TICKERS, max_workers=8)

        # ── 2. Retry empty tickers (pass 2) ──────────────────────────────
        empty_tickers = [r["Ticker"] for r in records if r.get("Price") is None]
        if empty_tickers:
            print("  Retrying {} tickers with no data (pass 2, slower)...".format(len(empty_tickers)))
            time.sleep(3)
            retry_records = fetch_all_parallel(empty_tickers, max_workers=4)
            retry_map = {r["Ticker"]: r for r in retry_records if r.get("Price") is not None}
            records   = [retry_map.get(r["Ticker"], r) for r in records]
            print("  Recovered: {}  |  Still empty: {}".format(
                len(retry_map), len(empty_tickers) - len(retry_map)))

        # ── 3. Sector medians & scoring ───────────────────────────────────
        print("\n  Computing sector medians...")
        df = pd.DataFrame(records)
        sector_medians = compute_sector_medians(df)

        print("  Scoring & sorting...")
        df["Score"] = df.apply(lambda r: calculate_weighted_score(r, sector_medians), axis=1)

        no_data = df["Score"].isna().sum()
        df = df[df["Score"].notna()].copy()
        if no_data > 0:
            print("  ⚠️  Dropped {} tickers with insufficient data".format(int(no_data)))

        df["Action"]         = df["Score"].apply(lambda s: get_recommendation(s)[0])
        df["Target"]         = df["Analyst_Target"]
        df["Upside"]         = df["Analyst_Upside"]
        df["Composite_Flag"] = df.apply(assign_composite_flag, axis=1)

        # ── Moat scoring ─────────────────────────────────────────────────
        print("  Computing moat scores (brand · switching costs · network effects)...")
        moat_results = df.apply(lambda r: calculate_moat_score(r, sector_medians), axis=1)
        df["Moat_Score"]     = moat_results.apply(lambda x: x[0])
        df["Moat_Label"]     = moat_results.apply(lambda x: x[1])
        df["Moat_Brand"]     = moat_results.apply(lambda x: x[2].get("brand"))
        df["Moat_Switching"] = moat_results.apply(lambda x: x[2].get("switching"))
        df["Moat_Network"]   = moat_results.apply(lambda x: x[2].get("network"))

        # Prepend moat badge into Composite_Flag
        def _append_moat_flag(row):
            existing = str(row.get("Composite_Flag") or "—")
            mf = assign_moat_flag(row)
            if mf:
                return (mf + " · " + existing) if existing != "—" else mf
            return existing
        df["Composite_Flag"] = df.apply(_append_moat_flag, axis=1)

        # ── Hypergrowth scoring ───────────────────────────────────────────
        print("  Computing hypergrowth scores (growth · leverage · PMF · discovery)...")
        # Fill in Sector_Rev_Growth_Med from sector medians
        df["Sector_Rev_Growth_Med"] = df["Sector"].apply(
            lambda s: sector_medians.get(s, {}).get("Rev_Growth"))

        hg_results = df.apply(lambda r: calculate_hypergrowth_score(r, sector_medians), axis=1)
        df["HG_Score"]    = hg_results.apply(lambda x: x[0])
        df["HG_Label"]    = hg_results.apply(lambda x: x[1])
        df["HG_Growth"]   = hg_results.apply(lambda x: x[2].get("growth"))
        df["HG_Leverage"] = hg_results.apply(lambda x: x[2].get("leverage"))
        df["HG_PMF"]      = hg_results.apply(lambda x: x[2].get("pmf"))
        df["HG_Discovery"]= hg_results.apply(lambda x: x[2].get("discovery"))

        # Append hypergrowth flag to Composite_Flag
        def _append_hg_flag(row):
            existing = str(row.get("Composite_Flag") or "—")
            hf = assign_hypergrowth_flag(row)
            if hf:
                return (hf + " · " + existing) if existing != "—" else hf
            return existing
        df["Composite_Flag"] = df.apply(_append_hg_flag, axis=1)

        df.sort_values("Score", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # ── 4. Save CSV ───────────────────────────────────────────────────
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
        df[csv_cols].to_csv(str(CSV_FILE), index=False)
        print("  ✅  CSV  → {}".format(CSV_FILE))

        # ── 5. Save HTML ──────────────────────────────────────────────────
        with open(str(HTML_FILE), "w", encoding="utf-8") as f:
            f.write(generate_html_report(df))
        print("  ✅  HTML → {}".format(HTML_FILE))

        # ── 6. Console summary ────────────────────────────────────────────
        sep = "─" * 80
        print("\n" + sep)
        print("  {:<2}  {:<7}  {:<22}  {:>5}  {:<12}  {:>8}  {:>7}  {:<20}".format(
              "#", "TICKER", "NAME", "SCORE", "ACTION", "PRICE", "UPSIDE", "FLAGS"))
        print(sep)
        for i, (_, r) in enumerate(df.head(20).iterrows(), 1):
            price_s  = "${:.2f}".format(r["Price"]) if r["Price"] else "  N/A"
            upside_s = "{:+.1f}%".format(r["Upside"]) if pd.notna(r.get("Upside")) and r.get("Upside") is not None else "  N/A"
            flag_s   = str(r.get("Composite_Flag") or "")[:30]
            print("  {:<2}  {:<7}  {:<22}  {:>5}  {:<12}  {:>8}  {:>7}  {:<20}".format(
                  i, r["Ticker"], str(r["Name"])[:21], r["Score"],
                  r["Action"], price_s, upside_s, flag_s))
        print(sep)

        sb_c = int((df["Action"] == "STRONG BUY").sum())
        b_c  = int((df["Action"] == "BUY").sum())
        h_c  = int((df["Action"] == "HOLD").sum())
        s_c  = int((df["Action"] == "SELL").sum())
        print("\n  📊  {} stocks  |  Avg Score: {:.1f}".format(n, df["Score"].mean()))
        print("  ⭐  STRONG BUY: {}  |  BUY: {}  |  HOLD: {}  |  SELL: {}\n".format(sb_c, b_c, h_c, s_c))
        print("  NOTE: Scores are a screening tool, not financial advice.\n")

        # ── 7. Top 10 ─────────────────────────────────────────────────────
        print("\n  Generating Top 10 recommendations...")
        top10 = generate_top10_recommendations(df, n=10)
        top10_text = print_top10_report(top10, output_file=str(TOP10_FILE))
        success = True

    except Exception as e:
        log.error("❌  Pipeline error: %s", e)
        traceback.print_exc()

    # ── 8. Email ──────────────────────────────────────────────────────────
    if send_email_after:
        msg      = build_email(success, top10_text)
        email_ok = send_email(msg)
        if not email_ok:
            success = False

    return success


# =============================================================================
# SCHEDULER
# =============================================================================
def start_scheduler():
    try:
        import schedule
    except ImportError:
        print("⚠️  'schedule' package not found. Install it with: pip install schedule")
        sys.exit(1)

    def job():
        # Only run on weekdays (Mon–Fri)
        if datetime.now().weekday() >= 5:
            log.info("⏭️  Skipping — today is a weekend.")
            return
        log.info("🕘  Scheduled run triggered.")
        run(send_email_after=True)

    schedule.every().day.at(SCHEDULE_TIME).do(job)
    log.info("🗓️  Scheduler running — will execute on weekdays at %s.", SCHEDULE_TIME)
    log.info("   Press Ctrl+C to stop.\n")

    while True:
        schedule.run_pending()
        time.sleep(30)


# =============================================================================
# ENTRY POINT
# =============================================================================
def main():
    parser = argparse.ArgumentParser(
        description="Portfolio Master — analyze 344 stocks, generate reports, email results."
    )
    parser.add_argument(
        "--schedule", action="store_true",
        help="Run on a weekday schedule at SCHEDULE_TIME (default: run once now)"
    )
    parser.add_argument(
        "--no-email", action="store_true",
        help="Run the analyzer but skip sending the email"
    )
    args = parser.parse_args()

    # Validate email config unless skipping
    if not args.no_email:
        if "your_email" in GMAIL_SENDER or "xxxx" in GMAIL_APP_PASSWORD:
            print("❌  Please set GMAIL_SENDER and GMAIL_APP_PASSWORD in the CONFIG section.")
            sys.exit(1)

    log.info("=" * 64)
    log.info("  PORTFOLIO MASTER STARTED")
    log.info("=" * 64)

    if args.schedule:
        start_scheduler()
    else:
        ok = run(send_email_after=not args.no_email)
        if not ok:
            sys.exit(1)
        log.info("🎉  All done.\n")


if __name__ == "__main__":
    main()

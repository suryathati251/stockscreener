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
        "Price_3M_Return", "Price_6M_Return", "Price_12M_Return",
        "FCF_vs_NetIncome", "Buyback_Yield", "Shareholder_Yield",
        "Margin_Trend", "EPS_Revision", "Sector_RS",
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

            # ── Price momentum: 3m, 6m, 12m returns ──────────────────────
            price_3m = price_6m = price_12m = None
            try:
                hist = t.history(period="1y", interval="1mo", auto_adjust=True)
                if hist is not None and len(hist) >= 3:
                    p_now = float(hist["Close"].iloc[-1])
                    if len(hist) >= 3:
                        p3  = float(hist["Close"].iloc[-3])
                        price_3m  = round(((p_now - p3)  / p3)  * 100, 1) if p3  else None
                    if len(hist) >= 7:
                        p6  = float(hist["Close"].iloc[-7])
                        price_6m  = round(((p_now - p6)  / p6)  * 100, 1) if p6  else None
                    if len(hist) >= 13:
                        p12 = float(hist["Close"].iloc[-13])
                        price_12m = round(((p_now - p12) / p12) * 100, 1) if p12 else None
            except Exception:
                pass

            # ── FCF quality: FCF vs Net Income ────────────────────────────
            # Ratio > 1.0 = FCF exceeds net income = very clean/conservative accounting
            # Ratio < 0.5 = earnings are not converting to cash = red flag
            fcf_vs_ni = None
            try:
                ni = info.get("netIncomeToCommon") or info.get("netIncome")
                if fcf and ni and ni != 0:
                    fcf_vs_ni = round(fcf / abs(ni), 2)
            except Exception:
                pass

            # ── Buyback Yield ─────────────────────────────────────────────
            # Negative "repurchaseOfStock" in cash flow = buying back shares
            buyback_yield = None
            try:
                cf = t.cashflow
                if cf is not None and not cf.empty:
                    for label in ["Repurchase Of Capital Stock", "RepurchaseOfCapitalStock",
                                  "repurchaseOfStock", "Common Stock Repurchased"]:
                        if label in cf.index:
                            rb = cf.loc[label].iloc[0]
                            if rb is not None and not np.isnan(float(rb)) and mkt_cap and mkt_cap > 0:
                                # Repurchases are typically negative in cash flow statements
                                buyback_yield = round((abs(float(rb)) / mkt_cap) * 100, 2)
                            break
            except Exception:
                pass

            # ── Shareholder Yield = FCF yield + div yield + buyback yield ─
            shareholder_yield = None
            try:
                components = [v for v in [fcf_yield, div_yield, buyback_yield] if v is not None]
                if components:
                    shareholder_yield = round(sum(components), 2)
            except Exception:
                pass

            # ── Operating Margin Trend (current year vs prior year) ───────
            margin_trend = None
            try:
                ann = t.financials   # annual income statement
                if ann is not None and not ann.empty:
                    for rev_label in ["Total Revenue", "Revenue"]:
                        for om_label in ["Operating Income", "Operating Income Loss"]:
                            if rev_label in ann.index and om_label in ann.index:
                                revs = ann.loc[rev_label].dropna()
                                oms  = ann.loc[om_label].dropna()
                                if len(revs) >= 2 and len(oms) >= 2:
                                    om_curr = float(oms.iloc[0]) / float(revs.iloc[0]) * 100 if revs.iloc[0] != 0 else None
                                    om_prev = float(oms.iloc[1]) / float(revs.iloc[1]) * 100 if revs.iloc[1] != 0 else None
                                    if om_curr is not None and om_prev is not None:
                                        margin_trend = round(om_curr - om_prev, 1)
                                break
            except Exception:
                pass

            # ── EPS Revision: estimate revisions direction ─────────────────
            # +1 = estimates being raised, -1 = being cut, 0 = stable
            eps_revision = None
            try:
                # Compare current mean EPS estimate vs 30d ago
                ae = t.analyst_price_targets if hasattr(t, "analyst_price_targets") else None
                # Fallback: use upgradesDowngrades as proxy
                updown = t.upgrades_downgrades
                if updown is not None and not updown.empty:
                    recent_ud = updown.head(10)
                    upgrades   = (recent_ud["ToGrade"].str.contains("Buy|Outperform|Overweight|Strong Buy",
                                  case=False, na=False)).sum()
                    downgrades = (recent_ud["ToGrade"].str.contains("Sell|Underperform|Underweight|Reduce",
                                  case=False, na=False)).sum()
                    if   upgrades > downgrades:   eps_revision =  1
                    elif downgrades > upgrades:   eps_revision = -1
                    else:                         eps_revision =  0
            except Exception:
                pass

            if mkt_cap:
                if   mkt_cap >= 1e12: mkt_cap_fmt = "${:.1f}T".format(mkt_cap / 1e12)
                elif mkt_cap >= 1e9:  mkt_cap_fmt = "${:.1f}B".format(mkt_cap / 1e9)
                else:                 mkt_cap_fmt = "${:.0f}M".format(mkt_cap / 1e6)
            else:
                mkt_cap_fmt = None

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
                # v3 new fields
                "Price_3M_Return":   price_3m,
                "Price_6M_Return":   price_6m,
                "Price_12M_Return":  price_12m,
                "FCF_vs_NetIncome":  fcf_vs_ni,
                "Buyback_Yield":     buyback_yield,
                "Shareholder_Yield": shareholder_yield,
                "Margin_Trend":      margin_trend,
                "EPS_Revision":      eps_revision,
                "Sector_RS":         None,   # computed post-fetch from sector medians
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
    metrics = ["PE_Fwd", "PS", "EV_EBITDA", "Gross_Margin", "Op_Margin", "Price_6M_Return"]
    sector_medians = {}
    for sector, group in df.groupby("Sector"):
        sector_medians[sector] = {}
        for m in metrics:
            if m not in df.columns:
                continue
            vals = group[m].dropna()
            sector_medians[sector][m] = float(vals.median()) if len(vals) >= 3 else None
    return sector_medians


# =============================================================================
# SCORING ENGINE — WEIGHTED, SECTOR-RELATIVE
# =============================================================================
WEIGHTS = {
    # ── Quality / profitability (core compounder signals) ─────────────────
    "fcf_yield":        8,
    "roe":              7,
    "op_margin":        5,
    "roa":              4,
    "gross_margin_rel": 4,
    "current_ratio":    3,
    # ── Growth ────────────────────────────────────────────────────────────
    "rev_growth":       7,
    "rev_accel":        5,
    "eps_growth":       5,
    "eps_surprise":     4,
    # ── Valuation — relative to sector ────────────────────────────────────
    "pe_rel":           5,
    "ps_rel":           4,
    "ev_ebitda_rel":    4,
    "peg":              5,
    # ── Technical / momentum ──────────────────────────────────────────────
    "vs_ma200":         4,
    "price_momentum":   6,   # NEW: 6m price momentum
    "sector_rs":        4,   # NEW: relative strength vs sector peers
    "from_low":         2,
    # ── Analyst signal ────────────────────────────────────────────────────
    "analyst_upside":   6,
    "analyst_count":    2,
    "eps_revision":     4,   # NEW: estimate revision direction
    # ── Capital allocation / cash quality ─────────────────────────────────
    "shareholder_yield":5,   # NEW: FCF + div + buyback yield combined
    "fcf_quality":      4,   # NEW: FCF vs net income ratio
    "margin_trend":     4,   # NEW: operating margin expanding/contracting
    # ── Risk / sentiment ──────────────────────────────────────────────────
    "debt_equity":      4,
    "short_float":      3,
    "inst_own":         2,
    "insider_buy":      3,
    "beta":             2,
    # ── Dividend / stability ──────────────────────────────────────────────
    "div_yield":        2,
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

    # ── Price Momentum: 6-month return (weight 6) ────────────────────────
    p6m = row.get("Price_6M_Return")
    if p6m is not None:
        if   p6m > 50:   total += w("price_momentum") * 1.0
        elif p6m > 25:   total += w("price_momentum") * 0.7
        elif p6m > 10:   total += w("price_momentum") * 0.4
        elif p6m > 0:    total += w("price_momentum") * 0.1
        elif p6m < -30:  total += w("price_momentum") * -1.0
        elif p6m < -15:  total += w("price_momentum") * -0.6
        elif p6m < 0:    total += w("price_momentum") * -0.2

    # ── Sector Relative Strength (weight 4) ───────────────────────────────
    srs = row.get("Sector_RS")
    if srs is not None:
        if   srs > 20:  total += w("sector_rs") * 1.0
        elif srs > 10:  total += w("sector_rs") * 0.6
        elif srs > 0:   total += w("sector_rs") * 0.2
        elif srs < -20: total += w("sector_rs") * -1.0
        elif srs < -10: total += w("sector_rs") * -0.5
        else:           total += w("sector_rs") * -0.1

    # ── EPS Revision Direction (weight 4) ────────────────────────────────
    er = row.get("EPS_Revision")
    if er is not None:
        if   er > 0:  total += w("eps_revision") * 1.0
        elif er < 0:  total += w("eps_revision") * -1.0

    # ── FCF Quality: FCF vs Net Income ratio (weight 4) ───────────────────
    fcf_ni = row.get("FCF_vs_NetIncome")
    if fcf_ni is not None:
        if   fcf_ni > 1.5:  total += w("fcf_quality") * 1.0
        elif fcf_ni > 1.0:  total += w("fcf_quality") * 0.6
        elif fcf_ni > 0.7:  total += w("fcf_quality") * 0.2
        elif fcf_ni > 0.3:  total += w("fcf_quality") * -0.3
        elif fcf_ni < 0:    total += w("fcf_quality") * -1.0
        else:               total += w("fcf_quality") * -0.7

    # ── Shareholder Yield (weight 5) ─────────────────────────────────────
    shy = row.get("Shareholder_Yield")
    if shy is not None:
        if   shy > 15:  total += w("shareholder_yield") * 1.0
        elif shy > 8:   total += w("shareholder_yield") * 0.7
        elif shy > 4:   total += w("shareholder_yield") * 0.3
        elif shy > 1:   total += w("shareholder_yield") * 0.1
        elif shy < 0:   total += w("shareholder_yield") * -0.5

    # ── Operating Margin Trend (weight 4) ────────────────────────────────
    mt = row.get("Margin_Trend")
    if mt is not None:
        if   mt > 5:    total += w("margin_trend") * 1.0
        elif mt > 2:    total += w("margin_trend") * 0.6
        elif mt > 0:    total += w("margin_trend") * 0.2
        elif mt < -5:   total += w("margin_trend") * -1.0
        elif mt < -2:   total += w("margin_trend") * -0.5
        else:           total += w("margin_trend") * -0.1

    # ── Normalize to 0–100 ────────────────────────────────────────────────
    max_possible = float(TOTAL_WEIGHT)
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
    p6m   = row.get("Price_6M_Return")
    srs   = row.get("Sector_RS")
    fcf_ni= row.get("FCF_vs_NetIncome")
    shy   = row.get("Shareholder_Yield")
    mt    = row.get("Margin_Trend")
    er    = row.get("EPS_Revision")
    accel = None
    if row.get("Rev_Growth") and row.get("Rev_Growth_Prev"):
        accel = row["Rev_Growth"] - row["Rev_Growth_Prev"]

    # ── Quality / compounder signals ──────────────────────────────────────
    if roe and roe > 20 and fcf and fcf > 4 and (de is None or de < 1.0):
        flags.append("⭐ Compounder")

    # ── Growth signals ────────────────────────────────────────────────────
    if accel and accel > 10 and rg and rg > 15:
        flags.append("🚀 Accel Growth")

    # ── Valuation signals ─────────────────────────────────────────────────
    if peg and 0 < peg < 1.0 and fcf and fcf > 3:
        flags.append("💎 Deep Value")

    # ── Analyst signals ───────────────────────────────────────────────────
    ac = row.get("Analyst_Count")
    if au and not _is_nan(au) and au > 25 and ac and not _is_nan(ac) and int(ac) >= 15:
        flags.append("📈 Analyst Conviction")
    if er is not None and er > 0:
        flags.append("📊 Est. Rising")      # analysts raising estimates

    # ── Income signals ────────────────────────────────────────────────────
    if dy and dy > 3 and (not de or de < 1.5):
        flags.append("💰 Income")

    # ── NEW: Capital allocator ────────────────────────────────────────────
    if shy and shy > 8 and (de is None or de < 1.0):
        flags.append("🔄 Capital Allocator")  # returning lots of cash to shareholders

    # ── NEW: Clean earnings (FCF quality) ─────────────────────────────────
    if fcf_ni and fcf_ni > 1.2 and fcf and fcf > 4:
        flags.append("✅ Clean Earnings")    # FCF > net income = high earnings quality

    # ── NEW: Margin expansion ─────────────────────────────────────────────
    if mt and mt > 2 and roe and roe > 12:
        flags.append("📐 Margin Expand")    # operating margins expanding

    # ── NEW: Price momentum leader ────────────────────────────────────────
    if p6m and p6m > 20 and srs and srs > 10:
        flags.append("🔥 Momentum")         # beating sector peers on price

    # ── NEW: Turnaround candidate ─────────────────────────────────────────
    # Beaten-down price + positive estimate revisions + improving margins = potential turn
    if (vs200 and vs200 < -15 and er is not None and er > 0
            and mt is not None and mt > 0 and fcf and fcf > 2):
        flags.append("🔃 Turnaround")

    # ── Risk flags ────────────────────────────────────────────────────────
    if sf and sf > 20:
        flags.append("⚠️ High Short")
    if beta and beta > 2.0:
        flags.append("⚠️ High Beta")
    if de and de > 2.5:
        flags.append("⚠️ High Leverage")
    if vs200 and vs200 < -20:
        flags.append("⚠️ Below 200 DMA")
    if fcf_ni is not None and fcf_ni < 0.4 and fcf_ni > -1:
        flags.append("⚠️ Weak FCF Quality")  # earnings not converting to cash
    if er is not None and er < 0:
        flags.append("⚠️ Est. Cut")           # analysts cutting estimates

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
*{box-sizing:border-box}
body{background:#0d1117;color:#c9d1d9;font-family:"Segoe UI",sans-serif;font-size:13px;margin:0}
h1{font-size:1.45rem;color:#58a6ff;margin-bottom:.4rem}
.version-badge{font-size:11px;background:#1f6feb;color:#fff;padding:2px 8px;border-radius:10px;margin-left:8px;vertical-align:middle}
.stat-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:10px 16px;display:inline-block;margin:0 6px 8px 0;min-width:90px}
.stat-card .num{font-size:1.5rem;font-weight:700;line-height:1.1}
.stat-card .lbl{font-size:.68rem;color:#8b949e;text-transform:uppercase;letter-spacing:.5px}
.toolbar{display:flex;align-items:flex-start;gap:14px;flex-wrap:wrap;margin-bottom:10px}
.filter-group{display:flex;flex-direction:column;gap:5px}
.filter-label{font-size:.7rem;color:#8b949e;text-transform:uppercase;letter-spacing:.5px;margin-bottom:1px}
.btn-row{display:flex;gap:5px;flex-wrap:wrap}
.act-btn{background:#161b22;border:1px solid #30363d;color:#8b949e;border-radius:6px;padding:5px 13px;cursor:pointer;font-size:12px;font-weight:600;transition:all .15s;white-space:nowrap}
.act-btn:hover{border-color:#58a6ff;color:#c9d1d9}
#btnAll.active{background:#30363d;color:#c9d1d9;border-color:#8b949e}
.act-btn[data-action="STRONG BUY"].active{background:#1f6feb;color:#fff;border-color:#1f6feb}
.act-btn[data-action="BUY"].active{background:#238636;color:#fff;border-color:#238636}
.act-btn[data-action="HOLD"].active{background:#9e6a03;color:#fff;border-color:#9e6a03}
.act-btn[data-action="SELL"].active{background:#b62324;color:#fff;border-color:#b62324}
.sec-btn{background:#161b22;border:1px solid #30363d;color:#8b949e;border-radius:6px;padding:4px 10px;cursor:pointer;font-size:11px;font-weight:500;transition:all .15s;white-space:nowrap}
.sec-btn:hover{border-color:#58a6ff;color:#c9d1d9}
#btnSecAll.active,.sec-btn[data-sector].active{background:#388bfd22;color:#58a6ff;border-color:#388bfd}
input#srch{background:#161b22;border:1px solid #30363d;color:#c9d1d9;border-radius:6px;padding:6px 12px;width:220px;outline:none}
input#srch:focus{border-color:#58a6ff}
.btn-csv{background:#238636;color:#fff;border:none;border-radius:6px;padding:6px 14px;cursor:pointer;font-size:13px;align-self:flex-end}
.wrap{overflow-x:auto;max-height:80vh;border:1px solid #21262d;border-radius:8px}
table{border-collapse:collapse;width:100%;white-space:nowrap}
thead th{background:#161b22;color:#8b949e;position:sticky;top:0;z-index:9;padding:8px 7px;cursor:pointer;user-select:none;font-weight:600}
thead th:hover{color:#58a6ff}
thead th.asc::after{content:" ▲";font-size:9px}
thead th.desc::after{content:" ▼";font-size:9px}
.ma-above .ma-val{color:#3fb950;font-weight:600}
.ma-below .ma-val{color:#f85149;font-weight:600}
.ma-near  .ma-val{color:#d29922;font-weight:600}
td{padding:6px 7px;border-bottom:1px solid #21262d;vertical-align:middle}
td small{color:#8b949e;font-size:11px}
.tc{text-align:center}.tr{text-align:right}
.row-green{background:#0d1f0f}.row-green:hover{background:#0f2a14!important}
.row-orange{background:#1e1600}.row-orange:hover{background:#2a1f00!important}
.row-red{background:#1c0707}.row-red:hover{background:#2a0d0d!important}
.badge{font-size:11px;padding:3px 7px;border-radius:4px;font-weight:600}
.score-strong{background:#1f6feb;color:#fff}
.score-buy{background:#238636;color:#fff}
.score-hold{background:#9e6a03;color:#fff}
.score-sell{background:#b62324;color:#fff}
.bg-success{background:#238636!important;color:#fff}
.bg-info{background:#1f6feb!important;color:#fff}
.bg-warning{background:#9e6a03!important;color:#fff}
.bg-danger{background:#b62324!important;color:#fff}
.div-cell{color:#e6b450;font-weight:600}
.flag-cell{font-size:11px;max-width:200px;white-space:normal;line-height:1.4}
.analyst-up{color:#3fb950;font-weight:600}
.analyst-dn{color:#f85149;font-weight:600}
.accel-up{color:#3fb950;font-size:11px}
.accel-dn{color:#f85149;font-size:11px}
#rowcnt{font-size:12px;color:#8b949e;margin-top:6px}
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
    th_row = "".join('<th onclick="sortCol({})">{}</th>'.format(i, h) for i, h in enumerate(headers))

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Portfolio Analysis — {ts}</title>
<style>{css}</style>
</head>
<body>
<div style="padding:16px 20px 0">
<h1>📊 Portfolio Analysis <span class="version-badge">Master v3</span></h1>
<p style="color:#8b949e;font-size:12px;margin:0 0 12px">Generated: {ts} &nbsp;·&nbsp; {total} stocks screened</p>
<div>
  <div class="stat-card"><div class="num" style="color:#1f6feb">{sb}</div><div class="lbl">Strong Buy</div></div>
  <div class="stat-card"><div class="num" style="color:#238636">{b}</div><div class="lbl">Buy</div></div>
  <div class="stat-card"><div class="num" style="color:#9e6a03">{h}</div><div class="lbl">Hold</div></div>
  <div class="stat-card"><div class="num" style="color:#f85149">{s}</div><div class="lbl">Sell</div></div>
  <div class="stat-card"><div class="num">{avg_sc}</div><div class="lbl">Avg Score</div></div>
</div>
<div class="toolbar">
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
  <div class="filter-group" style="margin-top:auto">
    <input id="srch" type="text" placeholder="Search ticker / name…" oninput="applyFilters()">
  </div>
  <button class="btn-csv" onclick="exportCSV()">⬇ Export CSV</button>
</div>
<p id="rowcnt"></p>
<p style="font-size:11px;color:#8b949e">⚠️ Scores are a quantitative screening tool only — not financial advice. Always do qualitative due diligence.</p>
</div>
<div class="wrap" style="margin:0 20px 20px">
<table>
<thead><tr>{th_row}</tr></thead>
<tbody id="tbody">
{rows_html}
</tbody>
</table>
</div>
<script>{js}</script>
</body>
</html>""".format(
        ts=ts, css=_CSS, total=total, sb=sb, b=b, h=h, s=s, avg_sc=avg_sc,
        sec_btns=sec_btns, th_row=th_row, rows_html=rows_html, js=js,
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

        df.sort_values("Score", ascending=False, inplace=True)
        df.reset_index(drop=True, inplace=True)

        # ── 4. Save CSV ───────────────────────────────────────────────────
        csv_cols = [
            "Ticker", "Name", "Sector", "Score", "Action", "Composite_Flag",
            "Moat_Score", "Moat_Label", "Moat_Brand", "Moat_Switching", "Moat_Network",
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

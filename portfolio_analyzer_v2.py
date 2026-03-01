#!/usr/bin/env python3
"""
Professional Stock Portfolio Analyzer v2 — 344 STOCKS
Improvements over v1:
  - Sector-relative scoring (P/E, P/S, margins vs sector median)
  - Analyst consensus price target (targetMeanPrice) used directly
  - Added EV/EBITDA, earnings surprise, revenue growth acceleration
  - Added insider buying signal, analyst rating count
  - Weighted scoring engine (FCF, ROE, growth carry more weight)
  - Composite signal flags: "Compounding Quality", "Deep Value", "High Risk"
"""

import yfinance as yf
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import warnings
import sys

warnings.filterwarnings("ignore")

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
    'WMT', 'WOOF', 'XOM', 'XPEV', 'YUMC', 'ZBRA', 'ZM', 'ZS', 'CLOV','FISV','ZVRA','PLTK',
    'HUBS', 'TTD',  'GTLB', 'BILL', 'PAYC', 'PCTY', 'VEEV', 'INTU',
    'CDNS', 'SNPS', 'NTNX', 'ESTC', 'CFLT', 'DT',   'MNDY', 'BRZE', 'TOST',
    'APP',  'IOT',  'MSI',  'CHKP', 'FICO', 'TYL',  'CIEN', 'BSY', 'ZETA', 'NKE', 'HIMS'
]

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
    ]
    return {k: (ticker if k in ("Ticker", "Name") else None) for k in keys}

# =============================================================================
# DATA FETCHING
# =============================================================================
def fetch_ticker_data(ticker, _retries=5, _delay=2.0):
    import random as _r
    ticker = ticker.upper().strip()
    for _attempt in range(_retries):
      try:
        t    = yf.Ticker(ticker)
        info = t.info
        # Need at least a price to be useful
        has_price = (info.get("currentPrice") or info.get("regularMarketPrice")
                     or info.get("previousClose")) if info else None
        if not info or len(info) < 5 or not has_price:
            if _attempt < _retries - 1:
                wait = _delay * (2 ** _attempt) + _r.uniform(0.5, 2.0)  # exponential backoff
                time.sleep(wait)
                continue
            return _empty_row(ticker)

        # ── Price & 52-week range ──────────────────────────────────────────
        price  = (info.get("currentPrice") or info.get("regularMarketPrice")
                  or info.get("previousClose"))
        high52 = info.get("fiftyTwoWeekHigh")
        low52  = info.get("fiftyTwoWeekLow")
        pct_from_low  = round(((price - low52)  / low52)  * 100, 2) if price and low52  else None
        pct_from_high = round(((price - high52) / high52) * 100, 2) if price and high52 else None

        # ── Valuation ─────────────────────────────────────────────────────
        pe_fwd    = safe_float(info.get("forwardPE"))
        ps        = safe_float(info.get("priceToSalesTrailing12Months"))
        pb        = safe_float(info.get("priceToBook"))
        peg       = safe_float(info.get("pegRatio"))
        ev_ebitda = safe_float(info.get("enterpriseToEbitda"))

        # ── Quality ───────────────────────────────────────────────────────
        eps_growth_raw = info.get("earningsQuarterlyGrowth")
        eps_growth     = safe_pct(eps_growth_raw)

        # Recalculate PEG if missing
        if (peg is None or peg <= 0) and pe_fwd and eps_growth_raw and eps_growth_raw > 0:
            try:
                peg = round(pe_fwd / (eps_growth_raw * 100), 2)
            except Exception:
                peg = None

        # Earnings surprise (% beat vs estimate)
        eps_surprise = None
        try:
            cal = t.calendar
            if cal is not None and not cal.empty:
                eps_est = cal.get("Earnings Average") if "Earnings Average" in cal.index else None
                eps_act = info.get("trailingEps")
                if eps_est is not None and eps_act is not None and float(eps_est) != 0:
                    eps_surprise = round(((float(eps_act) - float(eps_est)) / abs(float(eps_est))) * 100, 2)
        except Exception:
            pass

        roe = safe_float(info.get("returnOnEquity"))
        if roe is not None:
            roe = round(roe * 100, 2)
        de = safe_float(info.get("debtToEquity"))
        if de is not None:
            de = round(de / 100, 2)

        mkt_cap = info.get("marketCap")
        fcf     = info.get("freeCashflow")
        fcf_yield = round((fcf / mkt_cap) * 100, 2) if fcf and mkt_cap else None

        rev_growth      = safe_pct(info.get("revenueGrowth"))
        gross_margin    = safe_pct(info.get("grossMargins"))
        profit_margin   = safe_pct(info.get("profitMargins"))
        op_margin       = safe_pct(info.get("operatingMargins"))
        beta            = safe_float(info.get("beta"))
        short_float     = safe_pct(info.get("shortPercentOfFloat"))
        inst_own        = safe_pct(info.get("heldPercentInstitutions"))

        # ── Revenue growth acceleration (current vs prior quarter) ────────
        rev_growth_prev = None
        try:
            qfin = t.quarterly_financials
            if qfin is not None and not qfin.empty and "Total Revenue" in qfin.index:
                revs = qfin.loc["Total Revenue"].dropna()
                if len(revs) >= 4:
                    # YoY growth for most recent quarter vs quarter before
                    g1 = (revs.iloc[0] - revs.iloc[2]) / abs(revs.iloc[2]) * 100 if revs.iloc[2] != 0 else None
                    g2 = (revs.iloc[1] - revs.iloc[3]) / abs(revs.iloc[3]) * 100 if revs.iloc[3] != 0 else None
                    if g1 is not None and g2 is not None:
                        rev_growth_prev = round(float(g2), 2)
                        if rev_growth is None:
                            rev_growth = round(float(g1), 2)
        except Exception:
            pass

        # ── Insider buying signal ─────────────────────────────────────────
        insider_buy_pct = None
        try:
            insider = t.insider_transactions
            if insider is not None and not insider.empty:
                recent = insider.head(20)
                buys  = (recent["Shares"] > 0).sum() if "Shares" in recent.columns else 0
                total_tx = len(recent)
                if total_tx > 0:
                    insider_buy_pct = round((buys / total_tx) * 100, 1)
        except Exception:
            pass

        # ── Dividends ─────────────────────────────────────────────────────
        div_yield_raw = info.get("dividendYield")
        div_yield     = round(div_yield_raw * 100, 2) if div_yield_raw else None
        payout_raw    = info.get("payoutRatio")
        payout_ratio  = round(payout_raw * 100, 2) if payout_raw else None

        roa_raw       = info.get("returnOnAssets")
        roa           = round(roa_raw * 100, 2) if roa_raw is not None else None
        current_ratio = safe_float(info.get("currentRatio"))

        # ── Technical ────────────────────────────────────────────────────
        ma200    = safe_float(info.get("twoHundredDayAverage"))
        vs_ma200 = round(((price - ma200) / ma200) * 100, 2) if price and ma200 else None

        # ── Analyst consensus (NEW — replaces fabricated target) ──────────
        analyst_target = safe_float(info.get("targetMeanPrice"))
        analyst_count  = info.get("numberOfAnalystOpinions")
        analyst_upside = None
        if analyst_target and price and price > 0:
            analyst_upside = round(((analyst_target - price) / price) * 100, 2)

        # ── Market cap formatting ──────────────────────────────────────────
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
            "Composite_Flag": None,  # filled in after sector stats computed
        }
      except Exception:
        if _attempt < _retries - 1:
            time.sleep(_delay * (_attempt + 1))
            continue
        return _empty_row(ticker)
    return _empty_row(ticker)


def fetch_all_parallel(tickers, max_workers=8):
    results, completed, total, start = [], 0, len(tickers), time.time()
    import random as _rj
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
    """
    Returns a dict: { sector -> { 'PE_Fwd': median, 'PS': median, ... } }
    Used to score each stock relative to its sector peers.
    """
    metrics = ["PE_Fwd", "PS", "EV_EBITDA", "Gross_Margin", "Op_Margin"]
    sector_medians = {}
    for sector, group in df.groupby("Sector"):
        sector_medians[sector] = {}
        for m in metrics:
            vals = group[m].dropna()
            if len(vals) >= 3:
                sector_medians[sector][m] = float(vals.median())
            else:
                sector_medians[sector][m] = None
    return sector_medians

# =============================================================================
# SCORING ENGINE v2 — WEIGHTED, SECTOR-RELATIVE
# =============================================================================

# Weights reflect research on which factors are most predictive of returns:
#   FCF yield, ROE, growth quality > valuation multiples > sentiment
WEIGHTS = {
    # Quality / profitability (highest weight)
    "fcf_yield":        8,
    "roe":              7,
    "op_margin":        5,
    "roa":              4,
    "gross_margin_rel": 4,   # relative to sector
    "current_ratio":    3,

    # Growth (high weight)
    "rev_growth":       7,
    "rev_accel":        5,   # acceleration bonus
    "eps_growth":       5,
    "eps_surprise":     4,

    # Valuation — relative to sector (medium weight)
    "pe_rel":           5,
    "ps_rel":           4,
    "ev_ebitda_rel":    4,
    "peg":              5,

    # Technical / momentum (medium weight)
    "vs_ma200":         5,
    "from_low":         3,

    # Analyst signal (medium weight)
    "analyst_upside":   6,
    "analyst_count":    2,

    # Sentiment / risk (lower weight)
    "debt_equity":      4,
    "short_float":      3,
    "inst_own":         2,
    "insider_buy":      3,
    "beta":             2,

    # Dividend / stability (lower weight — not relevant for growth stocks)
    "div_yield":        3,
    "payout_ratio":     2,
}

TOTAL_WEIGHT = sum(WEIGHTS.values())  # normalize to 0–100


def _score_metric(value, thresholds, scores):
    """
    Generic scorer: given sorted thresholds and corresponding scores,
    returns the score for the given value.
    thresholds: list of (condition_fn, score_fraction) tuples
    Returns a fraction in [-1, 1].
    """
    if value is None or (isinstance(value, float) and np.isnan(value)):
        return 0.0
    for cond_fn, frac in thresholds:
        if cond_fn(value):
            return frac
    return 0.0


def calculate_weighted_score(row, sector_medians):
    """
    Returns a score 0–100.
    Each sub-score is a fraction in [-1, 1] × its weight.
    Final = (sum of weighted scores normalized from min-possible to max-possible) mapped to 0–100.
    """
    # Data quality gate:
    # Must have a price, AND at least 2 financial metrics from the broader set
    def _has(m):
        v = row.get(m)
        return v is not None and not (isinstance(v, float) and np.isnan(v))

    if not _has("Price"):
        return None  # No price at all — skip

    financial_metrics = [
        "PE_Fwd", "PS", "PB", "ROE", "FCF_Yield", "Rev_Growth",
        "Gross_Margin", "Op_Margin", "Profit_Margin", "Debt_Equity",
        "EV_EBITDA", "Beta", "ROA", "Current_Ratio", "EPS_Growth"
    ]
    n_financial = sum(1 for m in financial_metrics if _has(m))
    if n_financial < 2:
        return None  # Truly empty — ETF or delisted

    sector = row.get("Sector", "Unknown")
    sm     = sector_medians.get(sector, {})
    total  = 0.0

    def w(key): return WEIGHTS.get(key, 0)

    # ── FCF Yield (weight 8) ──────────────────────────────────────────────
    fcf = row["FCF_Yield"]
    if fcf is not None:
        if   fcf > 10:  total += w("fcf_yield") * 1.0
        elif fcf > 7:   total += w("fcf_yield") * 0.8
        elif fcf > 4:   total += w("fcf_yield") * 0.5
        elif fcf > 1:   total += w("fcf_yield") * 0.2
        elif fcf < -2:  total += w("fcf_yield") * -0.8
        elif fcf < 0:   total += w("fcf_yield") * -0.3

    # ── ROE (weight 7) ───────────────────────────────────────────────────
    roe = row["ROE"]
    if roe is not None:
        if   roe > 40:  total += w("roe") * 1.0
        elif roe > 25:  total += w("roe") * 0.8
        elif roe > 15:  total += w("roe") * 0.5
        elif roe > 8:   total += w("roe") * 0.2
        elif roe < 0:   total += w("roe") * -0.8
        else:           total += w("roe") * -0.1

    # ── Operating Margin (weight 5) — absolute ────────────────────────────
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
        rel = (gm - gm_med) / gm_med      # % deviation from sector median
        total += w("gross_margin_rel") * max(-1.0, min(1.0, rel * 2))
    elif gm is not None:
        # Absolute fallback if no sector median
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

    # ── Revenue Growth Acceleration (weight 5) — NEW ─────────────────────
    rg_prev = row["Rev_Growth_Prev"]
    if rg is not None and rg_prev is not None:
        accel = rg - rg_prev   # positive = accelerating
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

    # ── Earnings Surprise (weight 4) — NEW ───────────────────────────────
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
            rel = (pe_med - pe) / pe_med   # positive = cheaper than sector
            total += w("pe_rel") * max(-1.0, min(1.0, rel * 1.5))
        else:
            # Absolute fallback
            if   pe < 12:  total += w("pe_rel") * 0.8
            elif pe < 20:  total += w("pe_rel") * 0.5
            elif pe > 60:  total += w("pe_rel") * -0.8
            elif pe > 40:  total += w("pe_rel") * -0.4
    elif pe is not None and pe < 0:
        total += w("pe_rel") * -0.6   # negative earnings

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

    # ── EV/EBITDA RELATIVE to sector (weight 4) — NEW ────────────────────
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
        if   vs200 > 30:   total += w("vs_ma200") * 0.5    # extended, some risk
        elif vs200 > 10:   total += w("vs_ma200") * 1.0    # healthy uptrend
        elif vs200 > 0:    total += w("vs_ma200") * 0.5    # just above
        elif vs200 > -10:  total += w("vs_ma200") * -0.3   # slightly below
        elif vs200 > -25:  total += w("vs_ma200") * -0.6   # below
        else:              total += w("vs_ma200") * -1.0   # well below

    # ── From 52-week low (weight 3) ───────────────────────────────────────
    fl = row["From_Low_Pct"]
    if fl is not None:
        if   fl < 10:   total += w("from_low") * 1.0    # near lows = opportunity
        elif fl < 25:   total += w("from_low") * 0.5
        elif fl > 200:  total += w("from_low") * -0.5   # extended run

    # ── Analyst Upside (weight 6) — NEW, uses real consensus target ───────
    au = row["Analyst_Upside"]
    if au is not None:
        if   au > 40:   total += w("analyst_upside") * 1.0
        elif au > 25:   total += w("analyst_upside") * 0.8
        elif au > 15:   total += w("analyst_upside") * 0.5
        elif au > 5:    total += w("analyst_upside") * 0.2
        elif au < -10:  total += w("analyst_upside") * -1.0
        elif au < 0:    total += w("analyst_upside") * -0.5

    # ── Analyst Count — conviction signal (weight 2) ─────────────────────
    ac = row["Analyst_Count"]
    if ac is not None and not (isinstance(ac, float) and np.isnan(ac)):
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

    # ── Insider Buying (weight 3) — NEW ──────────────────────────────────
    ib = row["Insider_Buy_Pct"]
    if ib is not None:
        if   ib > 70:   total += w("insider_buy") * 1.0
        elif ib > 50:   total += w("insider_buy") * 0.5
        elif ib < 20:   total += w("insider_buy") * -0.3

    # ── Beta / volatility (weight 2) ─────────────────────────────────────
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

    # ── Normalize to 0–100 ────────────────────────────────────────────────
    # max possible = sum of all weights (every metric at +1.0)
    # min possible = -sum of all weights
    max_possible = float(TOTAL_WEIGHT)
    score = ((total + max_possible) / (2 * max_possible)) * 100
    return min(max(round(score, 1), 0), 100)


def get_recommendation(score):
    if score >= 78: return "STRONG BUY", "bg-success", "GREEN"
    if score >= 62: return "BUY",         "bg-info",    "GREEN"
    if score >= 44: return "HOLD",        "bg-warning", "ORANGE"
    return                  "SELL",        "bg-danger",  "RED"


# =============================================================================
# COMPOSITE FLAGS — qualitative signal labels added to each stock
# =============================================================================
def assign_composite_flag(row):
    """
    Returns a short label describing the dominant signal profile.
    These are educational labels, not buy/sell signals.
    """
    flags = []

    roe   = row.get("ROE")
    fcf   = row.get("FCF_Yield")
    rg    = row.get("Rev_Growth")
    de    = row.get("Debt_Equity")
    dy    = row.get("Div_Yield")
    au    = row.get("Analyst_Upside")
    beta  = row.get("Beta")
    sf    = row.get("Short_Float")
    vs200 = row.get("Vs_MA200")
    peg   = row.get("PEG")
    accel = None
    if row.get("Rev_Growth") and row.get("Rev_Growth_Prev"):
        accel = row["Rev_Growth"] - row["Rev_Growth_Prev"]

    # Compounding quality: high ROE, positive FCF, manageable debt
    if (roe and roe > 20 and fcf and fcf > 4 and (de is None or de < 1.0)):
        flags.append("⭐ Compounder")

    # Accelerating growth
    if accel and accel > 10 and rg and rg > 15:
        flags.append("🚀 Accel Growth")

    # Deep value: cheap vs peers + positive FCF
    if (peg and 0 < peg < 1.0 and fcf and fcf > 3):
        flags.append("💎 Deep Value")

    # Analyst conviction: large upside + many analysts
    ac = row.get("Analyst_Count")
    if au and not _is_nan(au) and au > 25 and ac and not _is_nan(ac) and int(ac) >= 15:
        flags.append("📈 Analyst Conviction")

    # Income / dividend
    if dy and dy > 3 and (not de or de < 1.5):
        flags.append("💰 Income")

    # High risk flags
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
# HTML REPORT
# =============================================================================
_JS = r"""
var sortDir = {};
function toggleAction(btn) {
    btn.classList.toggle('active');
    var anyActive = document.querySelectorAll('.act-btn[data-action].active').length > 0;
    document.getElementById('btnAll').classList.toggle('active', !anyActive);
    applyFilters();
}
function toggleAll() {
    document.querySelectorAll('.act-btn[data-action]').forEach(function(b){ b.classList.remove('active'); });
    document.getElementById('btnAll').classList.add('active');
    applyFilters();
}
function toggleSector(btn) {
    btn.classList.toggle('active');
    var anyActive = document.querySelectorAll('.sec-btn[data-sector].active').length > 0;
    document.getElementById('btnSecAll').classList.toggle('active', !anyActive);
    applyFilters();
}
function toggleSecAll() {
    document.querySelectorAll('.sec-btn[data-sector]').forEach(function(b){ b.classList.remove('active'); });
    document.getElementById('btnSecAll').classList.add('active');
    applyFilters();
}
function toggleMA(btn) {
    btn.classList.toggle('active');
    applyFilters();
}
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
        var ra  = (r.getAttribute('data-action')  || '').trim();
        var rs  = (r.getAttribute('data-sector')  || '').trim();
        var rma = (r.getAttribute('data-ma')       || '').trim();
        var textMatch   = (!q || r.innerText.toLowerCase().indexOf(q) !== -1);
        var actionMatch = allActions || activeActions.indexOf(ra) !== -1;
        var sectorMatch = allSectors || activeSectors.indexOf(rs) !== -1;
        var maMatch     = !maBelow   || rma === 'below';
        r.style.display = (textMatch && actionMatch && sectorMatch && maMatch) ? '' : 'none';
    });
    updateCount();
}
function sortCol(c) {
    var tbody = document.getElementById('tbody');
    var rows  = Array.from(tbody.querySelectorAll('tr'));
    sortDir[c] = -(sortDir[c] || 1);
    var dir   = sortDir[c];
    rows.sort(function(a, b) {
        var ac = a.cells[c]; var bc = b.cells[c];
        // Prefer data-sort attribute (set on cells where innerText would be misleading)
        var av = (ac && ac.hasAttribute('data-sort')) ? ac.getAttribute('data-sort') : (ac ? ac.innerText.trim() : '');
        var bv = (bc && bc.hasAttribute('data-sort')) ? bc.getAttribute('data-sort') : (bc ? bc.innerText.trim() : '');
        var an = parseFloat(av.replace(/[^\d.\-]/g, ''));
        var bn = parseFloat(bv.replace(/[^\d.\-]/g, ''));
        if (!isNaN(an) && !isNaN(bn)) { return dir * (an - bn); }
        return dir * av.localeCompare(bv);
    });
    var ths = document.querySelectorAll('thead th');
    for (var i = 0; i < ths.length; i++) {
        ths[i].classList.remove('asc', 'desc');
        if (i === c) { ths[i].classList.add(dir === 1 ? 'desc' : 'asc'); }
    }
    rows.forEach(function(r){ tbody.appendChild(r); });
    updateCount();
}
function updateCount() {
    var vis = 0;
    document.querySelectorAll('#tbody tr').forEach(function(r){
        if (r.style.display !== 'none') { vis++; }
    });
    document.getElementById('rowcnt').textContent = 'Showing ' + vis + ' of %%TOTAL%% stocks';
}
function exportCSV() {
    var hdrs = Array.from(document.querySelectorAll('thead th')).map(function(h){
        return h.innerText.replace(/[\u25b2\u25bc]/g, '').trim();
    });
    var rows = Array.from(document.querySelectorAll('#tbody tr')).filter(function(r){
        return r.style.display !== 'none';
    });
    var NL = String.fromCharCode(10);
    var csv = hdrs.join(',') + NL;
    rows.forEach(function(r){
        csv += Array.from(r.cells).map(function(c){
            return '"' + c.innerText.replace(/[\r\n]+/g, ' ').trim() + '"';
        }).join(',') + NL;
    });
    var a = document.createElement('a');
    a.href = URL.createObjectURL(new Blob([csv], {type:'text/csv'}));
    a.download = 'portfolio_analysis_v2.csv';
    document.body.appendChild(a); a.click(); document.body.removeChild(a);
}
window.onload = updateCount;
"""

_CSS = """
* { box-sizing: border-box; }
body { background: #0d1117; color: #c9d1d9; font-family: "Segoe UI", sans-serif;
       font-size: 13px; margin: 0; }
h1 { font-size: 1.45rem; color: #58a6ff; margin-bottom: .4rem; }
.version-badge { font-size: 11px; background: #1f6feb; color: #fff; padding: 2px 8px;
                 border-radius: 10px; margin-left: 8px; vertical-align: middle; }
.stat-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
             padding: 10px 16px; display: inline-block; margin: 0 6px 8px 0; min-width: 90px; }
.stat-card .num { font-size: 1.5rem; font-weight: 700; line-height: 1.1; }
.stat-card .lbl { font-size: .68rem; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.toolbar { display: flex; align-items: flex-start; gap: 14px; flex-wrap: wrap; margin-bottom: 10px; }
.filter-group { display: flex; flex-direction: column; gap: 5px; }
.filter-label { font-size: .7rem; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; margin-bottom: 1px; }
.btn-row { display: flex; gap: 5px; flex-wrap: wrap; }
.act-btn { background: #161b22; border: 1px solid #30363d; color: #8b949e;
    border-radius: 6px; padding: 5px 13px; cursor: pointer;
    font-size: 12px; font-weight: 600; transition: all .15s; white-space: nowrap; }
.act-btn:hover { border-color: #58a6ff; color: #c9d1d9; }
#btnAll.active                          { background: #30363d; color: #c9d1d9; border-color: #8b949e; }
.act-btn[data-action="STRONG BUY"].active { background: #1f6feb; color: #fff; border-color: #1f6feb; }
.act-btn[data-action="BUY"].active        { background: #238636; color: #fff; border-color: #238636; }
.act-btn[data-action="HOLD"].active       { background: #9e6a03; color: #fff; border-color: #9e6a03; }
.act-btn[data-action="SELL"].active       { background: #b62324; color: #fff; border-color: #b62324; }
.sec-btn { background: #161b22; border: 1px solid #30363d; color: #8b949e;
    border-radius: 6px; padding: 4px 10px; cursor: pointer;
    font-size: 11px; font-weight: 500; transition: all .15s; white-space: nowrap; }
.sec-btn:hover { border-color: #58a6ff; color: #c9d1d9; }
#btnSecAll.active { background: #30363d; color: #c9d1d9; border-color: #8b949e; }
.sec-btn[data-sector].active { background: #388bfd22; color: #58a6ff; border-color: #388bfd; }
input#srch { background: #161b22; border: 1px solid #30363d; color: #c9d1d9;
             border-radius: 6px; padding: 6px 12px; width: 220px; outline: none; }
input#srch::placeholder { color: #8b949e; }
input#srch:focus { border-color: #58a6ff; }
.btn-csv { background: #238636; color: #fff; border: none; border-radius: 6px;
           padding: 6px 14px; cursor: pointer; font-size: 13px; align-self: flex-end; }
.wrap { overflow-x: auto; max-height: 80vh; border: 1px solid #21262d; border-radius: 8px; }
table { border-collapse: collapse; width: 100%; white-space: nowrap; }
thead th { background: #161b22; color: #8b949e; position: sticky; top: 0; z-index: 9;
           padding: 8px 7px; cursor: pointer; user-select: none; font-weight: 600; }
thead th:hover { color: #58a6ff; }
thead th.asc::after  { content: " ▲"; font-size: 9px; }
thead th.desc::after { content: " ▼"; font-size: 9px; }
thead th.div-col  { color: #e6b450; }
thead th.ma-col   { color: #58a6ff; }
thead th.new-col  { color: #bc8cff; }
.ma-above .ma-val { color: #3fb950; font-weight: 600; }
.ma-above .ma-pct-above { color: #3fb950; font-size: 11px; }
.ma-near  .ma-val { color: #d29922; font-weight: 600; }
.ma-near  .ma-pct-near  { color: #d29922; font-size: 11px; }
.ma-below .ma-val { color: #f85149; font-weight: 600; }
.ma-below .ma-pct-below { color: #f85149; font-size: 11px; }
td { padding: 6px 7px; border-bottom: 1px solid #21262d; vertical-align: middle; }
td small { color: #8b949e; font-size: 11px; }
.tc { text-align: center; } .tr { text-align: right; }
.row-green  { background: #0d1f0f; } .row-green:hover  { background: #0f2a14 !important; }
.row-orange { background: #1e1600; } .row-orange:hover { background: #2a1f00 !important; }
.row-red    { background: #1c0707; } .row-red:hover    { background: #2a0d0d !important; }
.badge { font-size: 11px; padding: 3px 7px; border-radius: 4px; font-weight: 600; }
.score-strong { background: #1f6feb; color: #fff; }
.score-buy    { background: #238636; color: #fff; }
.score-hold   { background: #9e6a03; color: #fff; }
.score-sell   { background: #b62324; color: #fff; }
.bg-success { background: #238636 !important; color: #fff; }
.bg-info    { background: #1f6feb !important; color: #fff; }
.bg-warning { background: #9e6a03 !important; color: #fff; }
.bg-danger  { background: #b62324 !important; color: #fff; }
.div-cell   { color: #e6b450; font-weight: 600; }
.flag-cell  { font-size: 11px; max-width: 200px; white-space: normal; line-height: 1.4; }
.analyst-up { color: #3fb950; font-weight: 600; }
.analyst-dn { color: #f85149; font-weight: 600; }
.accel-up   { color: #3fb950; font-size: 11px; }
.accel-dn   { color: #f85149; font-size: 11px; }
#rowcnt { font-size: 12px; color: #8b949e; margin-top: 6px; }
.ma-filter-btn { background: #161b22; border: 1.5px solid #58a6ff; color: #58a6ff;
    border-radius: 6px; padding: 5px 14px; cursor: pointer;
    font-size: 12px; font-weight: 700; transition: all .15s; white-space: nowrap; letter-spacing: .3px; }
.ma-filter-btn:hover { background: #58a6ff22; }
.ma-filter-btn.active { background: #58a6ff; color: #0d1117; }
.legend { font-size: 11px; color: #8b949e; margin-bottom: 8px; }
.legend span { margin-right: 12px; }
"""


def _ma200_cell(row):
    ma = row.get("MA200"); vs = row.get("Vs_MA200")
    if ma is None or vs is None: return '<td class="tc" data-sort="">-</td>'
    ms = "${:.2f}".format(ma); vs_s = "{:+.1f}%".format(vs)
    sort_attr = ' data-sort="{}"'.format(vs)   # raw float for correct numeric sort
    if vs < -5:
        return ('<td class="tc ma-below"' + sort_attr + '><span class="ma-val">' + ms
                + '</span><br><small class="ma-pct-below">' + vs_s + '</small></td>')
    elif vs < 0:
        return ('<td class="tc ma-near"' + sort_attr + '><span class="ma-val">' + ms
                + '</span><br><small class="ma-pct-near">' + vs_s + '</small></td>')
    return ('<td class="tc ma-above"' + sort_attr + '><span class="ma-val">' + ms
            + '</span><br><small class="ma-pct-above">' + vs_s + '</small></td>')


def _analyst_cell(row):
    au = row.get("Analyst_Upside")
    tgt = row.get("Analyst_Target")
    ac  = row.get("Analyst_Count")
    if tgt is None:
        return '<td class="tc">-</td>', '<td class="tc">-</td>'
    tgt_str = "${:.2f}".format(tgt)
    ac_str  = " <small>({} analysts)</small>".format(int(ac)) if (ac and not _is_nan(ac)) else ""
    if au is None:
        return '<td class="tc">' + tgt_str + ac_str + '</td>', '<td class="tc">-</td>'
    cls = "analyst-up" if au >= 0 else "analyst-dn"
    up_str = '{:+.1f}%'.format(au)
    return ('<td class="tc">' + tgt_str + ac_str + '</td>',
            '<td class="tc"><span class="' + cls + '">' + up_str + '</span></td>')


def _is_nan(v):
    if v is None: return True
    try:
        return (isinstance(v, float) and np.isnan(v)) or pd.isna(v)
    except Exception:
        return False

def _rev_growth_cell(row):
    rg      = row.get("Rev_Growth")
    rg_prev = row.get("Rev_Growth_Prev")
    if rg is None or _is_nan(rg):
        return '<td class="tc">-</td>'
    s = "{:.1f}%".format(rg)
    if rg_prev is not None and not _is_nan(rg_prev):
        accel = rg - rg_prev
        arrow = "▲" if accel > 2 else ("▼" if accel < -2 else "→")
        cls   = "accel-up" if accel > 2 else ("accel-dn" if accel < -2 else "")
        s += '<br><small class="{}">{}  {:.1f}pp</small>'.format(cls, arrow, accel)
    return '<td class="tc">' + s + '</td>'


def _build_rows(df):
    rows_html = ""
    for _, row in df.iterrows():
        rec, badge_cls, color_group = get_recommendation(row["Score"])
        row_cls    = {"GREEN": "row-green", "ORANGE": "row-orange", "RED": "row-red"}[color_group]
        s          = row["Score"]
        sector_val = str(row.get("Sector") or "")
        name_val   = str(row.get("Name")   or row["Ticker"])
        ticker_val = str(row["Ticker"])
        flag_val   = str(row.get("Composite_Flag") or "—")

        if   s >= 78: sc = '<span class="badge score-strong">' + str(s) + "</span>"
        elif s >= 62: sc = '<span class="badge score-buy">'    + str(s) + "</span>"
        elif s >= 44: sc = '<span class="badge score-hold">'   + str(s) + "</span>"
        else:         sc = '<span class="badge score-sell">'   + str(s) + "</span>"
        ac_badge = '<span class="badge ' + badge_cls + '">' + rec + "</span>"

        div_val  = row.get("Div_Yield")
        div_str  = fmt(div_val, suffix="%", decimals=2)
        div_cell = ('<td class="tc div-cell">' if (div_val and div_val > 0)
                    else '<td class="tc">') + div_str + "</td>"

        eps_s = row.get("EPS_Surprise")
        eps_s_str = ('{:+.1f}%'.format(eps_s) if (eps_s is not None and not _is_nan(eps_s)) else '-')
        eps_s_cls = ("analyst-up" if (eps_s and not _is_nan(eps_s) and eps_s > 0) else
                     "analyst-dn" if (eps_s and not _is_nan(eps_s) and eps_s < 0) else "")
        eps_s_cell = '<td class="tc"><span class="{}">{}</span></td>'.format(eps_s_cls, eps_s_str)

        analyst_tgt_cell, analyst_up_cell = _analyst_cell(row)

        cells = (
            "<td><strong>" + ticker_val + "</strong><br><small>" + name_val + "</small></td>"
            + '<td class="tc">'  + sc                                                     + "</td>"
            + '<td class="tc">'  + ac_badge                                               + "</td>"
            + '<td class="tc flag-cell">'  + flag_val                                     + "</td>"
            + '<td class="tr">'  + fmt(row["Price"],         prefix="$")                  + "</td>"
            + _ma200_cell(row)
            + analyst_tgt_cell
            + analyst_up_cell
            + '<td class="tc">'  + str(row.get("Mkt_Cap") or "-")                         + "</td>"
            + '<td class="tc">'  + fmt(row["PEG"])                                        + "</td>"
            + '<td class="tc">'  + fmt(row["PE_Fwd"],        decimals=1)                  + "</td>"
            + '<td class="tc">'  + fmt(row["PS"],            decimals=1)                  + "</td>"
            + '<td class="tc">'  + fmt(row["EV_EBITDA"],     decimals=1)                  + "</td>"
            + '<td class="tc">'  + fmt(row["ROE"],           suffix="%",decimals=1)       + "</td>"
            + _rev_growth_cell(row)
            + '<td class="tc">'  + fmt(row["Gross_Margin"],  suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["FCF_Yield"],     suffix="%",decimals=1)       + "</td>"
            + eps_s_cell
            + '<td class="tc">'  + fmt(row["From_Low_Pct"],  suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["From_High_Pct"], suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["Debt_Equity"])                                + "</td>"
            + '<td class="tc">'  + fmt(row["Beta"])                                       + "</td>"
            + '<td class="tc">'  + fmt(row["Short_Float"],   suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row.get("Insider_Buy_Pct"), suffix="%",decimals=0) + "</td>"
            + div_cell
            + '<td class="tc">'  + fmt(row["Payout_Ratio"],  suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["Op_Margin"],     suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["ROA"],           suffix="%",decimals=1)       + "</td>"
            + '<td class="tc">'  + fmt(row["Current_Ratio"], decimals=2)                  + "</td>"
            + '<td class="tc"><small>' + sector_val + "</small></td>"
        )
        vs200 = row.get("Vs_MA200")
        if vs200 is not None and not _is_nan(vs200):
            ma_status = "below" if vs200 < 0 else "above"
        else:
            ma_status = "none"
        rows_html += (
            '<tr class="' + row_cls + '"'
            + ' data-action="' + rec + '"'
            + ' data-sector="' + sector_val + '"'
            + ' data-ma="' + ma_status + '">'
            + cells + "</tr>\n"
        )
    return rows_html


def generate_html_report(df):
    ts          = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total       = len(df)
    strong_buys = int((df["Action"] == "STRONG BUY").sum())
    buys        = int((df["Action"] == "BUY").sum())
    holds       = int((df["Action"] == "HOLD").sum())
    sells       = int((df["Action"] == "SELL").sum())
    avg_score   = round(float(df["Score"].mean()), 1)
    rows_html   = _build_rows(df)
    js_ready    = _JS.replace("%%TOTAL%%", str(total))

    sectors = sorted(df["Sector"].dropna().unique())
    sec_buttons = ""
    for s in sectors:
        sec_buttons += (
            '<button class="sec-btn" data-sector="' + s + '"'
            + ' onclick="toggleSector(this)">' + s + "</button>\n"
        )

    TH = lambda label, i, div=False, ma=False, new=False: (
        '<th onclick="sortCol(' + str(i) + ')"'
        + (' class="new-col"' if new else (' class="ma-col"' if ma else (' class="div-col"' if div else '')))
        + '>' + label + '</th>\n'
    )

    headers = (
        TH("Ticker / Name",   0)  + TH("Score",        1)  + TH("Action",       2)
        + TH("Signal Flags",  3,  new=True)
        + TH("Price",         4)  + TH("200 DMA",       5,  ma=True)
        + TH("Analyst Target",6,  new=True) + TH("Upside %",     7,  new=True)
        + TH("Mkt Cap",       8)  + TH("PEG",           9)  + TH("P/E Fwd",     10)
        + TH("P/S",          11)  + TH("EV/EBITDA",    12,  new=True)
        + TH("ROE %",        13)  + TH("Rev Gr %",     14)  + TH("Gross Mgn %", 15)
        + TH("FCF Yld %",    16)  + TH("EPS Surp %",   17,  new=True)
        + TH("From Low %",   18)  + TH("From High %",  19)
        + TH("D/E",          20)  + TH("Beta",         21)  + TH("Short %",     22)
        + TH("Insider Buy%", 23,  new=True)
        + TH("Div Yield %",  24,  div=True) + TH("Payout %",    25,  div=True)
        + TH("Op Margin %",  26,  div=True) + TH("ROA %",       27,  div=True)
        + TH("Curr Ratio",   28,  div=True)
        + TH("Sector",       29)
    )

    return (
        "<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n"
        "  <meta charset=\"UTF-8\">\n"
        "  <meta name=\"viewport\" content=\"width=device-width,initial-scale=1\">\n"
        "  <title>Portfolio Analyzer v2 — " + ts + "</title>\n"
        "  <style>" + _CSS + "</style>\n"
        "</head>\n<body>\n"
        "<div style=\"padding:12px 16px\">\n"
        "  <h1>📊 Portfolio Analyzer <span class=\"version-badge\">v2</span>"
        "  <small style=\"font-size:.8rem;color:#8b949e\"> &nbsp;Generated: " + ts + "</small></h1>\n"
        "  <div class=\"legend\">"
        "    <span>🟣 Purple columns = new in v2</span>"
        "    <span>📈 Analyst targets are Wall Street consensus (not fabricated)</span>"
        "    <span>⚡ Rev Gr % shows acceleration arrow + pp change</span>"
        "  </div>\n"
        "  <div class=\"mb-2\">\n"
        "    <div class=\"stat-card\"><div class=\"num\">" + str(total)       + "</div><div class=\"lbl\">Stocks</div></div>\n"
        "    <div class=\"stat-card\" style=\"border-color:#1f6feb\"><div class=\"num\" style=\"color:#1f6feb\">" + str(strong_buys) + "</div><div class=\"lbl\">Strong Buy</div></div>\n"
        "    <div class=\"stat-card\" style=\"border-color:#238636\"><div class=\"num\" style=\"color:#238636\">" + str(buys)        + "</div><div class=\"lbl\">Buy</div></div>\n"
        "    <div class=\"stat-card\" style=\"border-color:#9e6a03\"><div class=\"num\" style=\"color:#9e6a03\">" + str(holds)       + "</div><div class=\"lbl\">Hold</div></div>\n"
        "    <div class=\"stat-card\" style=\"border-color:#b62324\"><div class=\"num\" style=\"color:#b62324\">" + str(sells)       + "</div><div class=\"lbl\">Sell</div></div>\n"
        "    <div class=\"stat-card\"><div class=\"num\">" + str(avg_score)   + "</div><div class=\"lbl\">Avg Score</div></div>\n"
        "  </div>\n"
        "  <div class=\"toolbar\">\n"
        "    <div class=\"filter-group\">\n"
        "      <div class=\"filter-label\">🔍 Search</div>\n"
        "      <input id=\"srch\" type=\"text\" placeholder=\"Ticker or name…\" oninput=\"applyFilters()\">\n"
        "    </div>\n"
        "    <div class=\"filter-group\">\n"
        "      <div class=\"filter-label\">Action</div>\n"
        "      <div class=\"btn-row\">\n"
        "        <button id=\"btnAll\" class=\"act-btn active\" onclick=\"toggleAll()\">ALL</button>\n"
        "        <button class=\"act-btn\" data-action=\"STRONG BUY\" onclick=\"toggleAction(this)\">🟢 STRONG BUY</button>\n"
        "        <button class=\"act-btn\" data-action=\"BUY\"        onclick=\"toggleAction(this)\">🔵 BUY</button>\n"
        "        <button class=\"act-btn\" data-action=\"HOLD\"       onclick=\"toggleAction(this)\">🟡 HOLD</button>\n"
        "        <button class=\"act-btn\" data-action=\"SELL\"       onclick=\"toggleAction(this)\">🔴 SELL</button>\n"
        "      </div>\n"
        "    </div>\n"
        "    <div class=\"filter-group\">\n"
        "      <div class=\"filter-label\">Sector</div>\n"
        "      <div class=\"btn-row\">\n"
        "        <button id=\"btnSecAll\" class=\"sec-btn active\" onclick=\"toggleSecAll()\">ALL</button>\n"
        + sec_buttons
        + "      </div>\n"
        "    </div>\n"
        "    <div class=\"filter-group\">\n"
        "      <div class=\"filter-label\">200 DMA</div>\n"
        "      <button id=\"btnMABelow\" class=\"ma-filter-btn\" onclick=\"toggleMA(this)\">Below 200 MA</button>\n"
        "    </div>\n"
        "    <button class=\"btn-csv\" onclick=\"exportCSV()\">⬇ Export CSV</button>\n"
        "  </div>\n"
        "  <div class=\"wrap\">\n"
        "  <table id=\"tbl\">\n"
        "    <thead><tr>\n" + headers + "    </tr></thead>\n"
        "    <tbody id=\"tbody\">\n" + rows_html + "    </tbody>\n"
        "  </table>\n"
        "  </div>\n"
        "  <div id=\"rowcnt\"></div>\n"
        "</div>\n"
        "<script>\n" + js_ready + "\n</script>\n"
        "</body>\n</html>"
    )



# =============================================================================
# TOP 10 RECOMMENDATIONS ENGINE
# =============================================================================
# Exclusion list — tickers you want to skip (edit freely)
# Reasons: already held, China risk, AI disruption risk, overextended, personal preference
EXCLUDE_TICKERS = set()   # e.g. {"TSM", "PDD", "NVDA"} — empty by default, populate as needed

# Risk flags that disqualify a stock from the top 10
DISQUALIFY_FLAGS = {"⚠️ High Short", "⚠️ High Leverage"}

def _val(v):
    """Return float or None, safe for NaN."""
    if v is None: return None
    try:
        f = float(v)
        return None if (f != f) else f   # NaN check
    except Exception:
        return None

def generate_top10_recommendations(df, n=10):
    """
    Multi-factor stock picker.  Builds a conviction score on top of the base
    scoring model by rewarding:
      - High analyst upside (consensus target vs price)
      - Strong ROE + FCF yield combination  (quality compounder signal)
      - Revenue growth acceleration         (momentum signal)
      - Low debt + low beta                 (risk-adjusted quality)
      - Sector diversification              (max 2 per sector in final 10)
    Penalises:
      - Stocks with disqualifying risk flags
      - Stocks in the EXCLUDE_TICKERS list
      - Stocks with very low analyst coverage (< 3 analysts — low conviction)
    Returns a list of dicts ready for printing.
    """
    rows = []
    for _, r in df.iterrows():
        ticker  = str(r.get("Ticker", ""))
        action  = str(r.get("Action", ""))
        flags   = str(r.get("Composite_Flag") or "")
        score   = _val(r.get("Score"))
        sector  = str(r.get("Sector") or "Unknown")

        # ── Hard filters ─────────────────────────────────────────────────
        if ticker in EXCLUDE_TICKERS:               continue
        if action not in ("STRONG BUY", "BUY"):     continue
        if score is None or score < 62:             continue
        if any(f in flags for f in DISQUALIFY_FLAGS): continue

        analyst_count  = _val(r.get("Analyst_Count"))
        if analyst_count is not None and analyst_count < 3: continue

        # ── Pull key metrics ─────────────────────────────────────────────
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
        vs_ma200 = _val(r.get("Vs_MA200"))
        from_low = _val(r.get("From_Low_Pct"))
        eps_s    = _val(r.get("EPS_Surprise"))
        insider  = _val(r.get("Insider_Buy_Pct"))
        target   = _val(r.get("Analyst_Target"))
        name     = str(r.get("Name", ticker))
        mkt_cap  = str(r.get("Mkt_Cap") or "-")

        # ── Conviction score (0–100, layered on top of base score) ───────
        conv = score   # start with base score

        # Analyst upside — high weight, direct signal from Wall St consensus
        if upside is not None:
            if   upside > 50:  conv += 12
            elif upside > 30:  conv += 8
            elif upside > 15:  conv += 4
            elif upside < 0:   conv -= 10

        # Quality: ROE + FCF together = compounder signal
        if roe is not None and fcf is not None:
            if roe > 25 and fcf > 5:   conv += 8
            elif roe > 15 and fcf > 3: conv += 4

        # Growth acceleration (current vs prior quarter YoY)
        if rev_g is not None and rev_prev is not None:
            accel = rev_g - rev_prev
            if   accel > 10:  conv += 6
            elif accel > 3:   conv += 3
            elif accel < -10: conv -= 5

        # EPS beat
        if eps_s is not None:
            if   eps_s > 15: conv += 4
            elif eps_s > 5:  conv += 2
            elif eps_s < -5: conv -= 4

        # Low debt + low beta = risk-adjusted quality bonus
        if de is not None and de < 0.3 and beta is not None and beta < 1.3:
            conv += 5

        # PEG < 1 = growth at reasonable price
        if peg is not None and 0 < peg < 1.0:
            conv += 4

        # Insider buying signal
        if insider is not None and insider > 60:
            conv += 3

        # Penalty: very extended above 52-week low (momentum fading)
        if from_low is not None and from_low > 150:
            conv -= 4

        # Penalty: low analyst count = less conviction behind the target
        if analyst_count is not None and analyst_count < 6:
            conv -= 3

        rows.append({
            "ticker":         ticker,
            "name":           name,
            "sector":         sector,
            "action":         action,
            "base_score":     round(score, 1),
            "conv_score":     round(conv, 1),
            "price":          price,
            "target":         target,
            "upside":         upside,
            "roe":            roe,
            "fcf":            fcf,
            "rev_g":          rev_g,
            "rev_prev":       rev_prev,
            "de":             de,
            "beta":           beta,
            "peg":            peg,
            "op_m":           op_m,
            "gross_m":        gross_m,
            "vs_ma200":       vs_ma200,
            "eps_s":          eps_s,
            "analyst_count":  analyst_count,
            "mkt_cap":        mkt_cap,
            "flags":          flags,
        })

    # ── Sort by conviction score ──────────────────────────────────────────
    rows.sort(key=lambda x: x["conv_score"], reverse=True)

    # ── Sector cap: max 2 per sector ──────────────────────────────────────
    sector_counts = {}
    top10 = []
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

def print_top10_report(top10, output_file="top10_recommendations.txt"):
    """Prints a formatted top-10 narrative to console and saves to file."""
    lines = []
    def p(s=""):
        lines.append(s)
        print(s)

    ts  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sep = "═" * 72

    p()
    p(sep)
    p("  🏆  TOP 10 STOCK RECOMMENDATIONS")
    p("  Generated: {}".format(ts))
    p("  Methodology: Base score + analyst upside + quality + growth momentum")
    p("               Max 2 stocks per sector for diversification")
    p(sep)
    p()
    p("  ⚠️  DISCLAIMER: Quantitative screen only — not financial advice.")
    p("      Always do your own due diligence before investing.")
    p()

    for i, r in enumerate(top10, 1):
        accel = _accel_label(r["rev_g"], r["rev_prev"])
        de_s  = "{:.2f}".format(r["de"])  if r["de"]   is not None else "N/A"
        beta_s= "{:.2f}".format(r["beta"])if r["beta"] is not None else "N/A"
        peg_s = "{:.2f}".format(r["peg"]) if r["peg"]  is not None else "N/A"
        roe_s = _fmt_pct(r["roe"], plus=False)
        fcf_s = _fmt_pct(r["fcf"], plus=False)
        rev_s = _fmt_pct(r["rev_g"], plus=False)
        op_s  = _fmt_pct(r["op_m"], plus=False)
        ac_s  = str(int(r["analyst_count"])) if r["analyst_count"] is not None else "N/A"
        eps_s = _fmt_pct(r["eps_s"]) if r["eps_s"] is not None else "N/A"

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
        p("  └" + "─" * 68)
        p()

    # Summary table
    p()
    p("  SUMMARY TABLE")
    p("  " + "─" * 68)
    p("  {:<3} {:<7} {:<22} {:>5} {:>7} {:>6} {:>5} {:<22}".format(
        "#", "Ticker", "Name", "Score", "Upside", "ROE%", "D/E", "Sector"))
    p("  " + "─" * 68)
    for i, r in enumerate(top10, 1):
        p("  {:<3} {:<7} {:<22} {:>5} {:>6}% {:>5}% {:>5} {:<22}".format(
            i,
            r["ticker"],
            r["name"][:21],
            r["base_score"],
            "{:.1f}".format(r["upside"])  if r["upside"] is not None else " N/A",
            "{:.0f}".format(r["roe"])     if r["roe"]    is not None else " N/A",
            "{:.2f}".format(r["de"])      if r["de"]     is not None else " N/A",
            r["sector"][:21]
        ))
    p("  " + "─" * 68)
    p()

    # Save to file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print("  ✅  Top 10 report → {}\n".format(output_file))


# =============================================================================
# MAIN
# =============================================================================
def main():
    n = len(PORTFOLIO_TICKERS)
    print("\n" + "=" * 62)
    print("  PORTFOLIO ANALYZER v2 — {} STOCKS".format(n))
    print("  Weighted · Sector-Relative · Analyst Consensus Targets")
    print("=" * 62)
    print("  Started : {}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("  Workers : 8 parallel threads (rate-limit safe)\n")

    print("  Fetching market data (pass 1)...")
    records = fetch_all_parallel(PORTFOLIO_TICKERS, max_workers=8)

    # Second pass: retry any tickers that came back with no price
    empty_tickers = [
        r["Ticker"] for r in records
        if r.get("Price") is None
    ]
    if empty_tickers:
        print("  Retrying {} tickers with no data (pass 2, slower)...".format(len(empty_tickers)))
        time.sleep(3)  # brief pause before retry burst
        retry_records = fetch_all_parallel(empty_tickers, max_workers=4)
        # Replace empty records with retry results
        retry_map = {r["Ticker"]: r for r in retry_records if r.get("Price") is not None}
        records = [retry_map.get(r["Ticker"], r) for r in records]
        recovered = len(retry_map)
        still_empty = len(empty_tickers) - recovered
        print("  Recovered: {}  |  Still empty (likely ETF/delisted): {}".format(recovered, still_empty))

    print("\n  Computing sector medians...")
    df = pd.DataFrame(records)
    sector_medians = compute_sector_medians(df)

    print("  Scoring & sorting...")
    df["Score"] = df.apply(lambda r: calculate_weighted_score(r, sector_medians), axis=1)

    # Drop stocks with insufficient data (score returned None)
    no_data = df["Score"].isna().sum()
    df = df[df["Score"].notna()].copy()
    if no_data > 0:
        print("  ⚠️  Dropped {} tickers with insufficient data (shown as - in all columns)".format(int(no_data)))

    df["Action"] = df["Score"].apply(lambda s: get_recommendation(s)[0])

    # Analyst target is now direct from yfinance — no fabrication
    df["Target"] = df["Analyst_Target"]
    df["Upside"]  = df["Analyst_Upside"]

    # Composite signal flags
    df["Composite_Flag"] = df.apply(assign_composite_flag, axis=1)

    df.sort_values("Score", ascending=False, inplace=True)
    df.reset_index(drop=True, inplace=True)

    csv_cols = [
        "Ticker", "Name", "Sector", "Score", "Action", "Composite_Flag",
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
    df[csv_cols].to_csv("portfolio_analysis_v2.csv", index=False)
    print("  ✅  CSV  → portfolio_analysis_v2.csv")

    with open("portfolio_analysis_v2.html", "w", encoding="utf-8") as f:
        f.write(generate_html_report(df))
    print("  ✅  HTML → portfolio_analysis_v2.html\n")

    sep = "─" * 80
    print(sep)
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

    sb = int((df["Action"] == "STRONG BUY").sum())
    b  = int((df["Action"] == "BUY").sum())
    h  = int((df["Action"] == "HOLD").sum())
    s  = int((df["Action"] == "SELL").sum())
    print("\n  📊  {} stocks  |  Avg Score: {:.1f}".format(n, df["Score"].mean()))
    print("  ⭐  STRONG BUY: {}  |  BUY: {}  |  HOLD: {}  |  SELL: {}\n".format(sb, b, h, s))
    print("  NOTE: Scores are a screening tool, not financial advice.")
    print("        Always do qualitative due diligence before acting.\n")

    # ── Top 10 Recommendations ────────────────────────────────────────────
    print("\n  Generating Top 10 recommendations...")
    print("  (Edit EXCLUDE_TICKERS at the top of this section to skip specific stocks)\n")
    top10 = generate_top10_recommendations(df, n=10)
    print_top10_report(top10, output_file="top10_recommendations.txt")


if __name__ == "__main__":
    main()

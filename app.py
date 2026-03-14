"""
Portfolio Analyzer v2 — Streamlit App
======================================
Loads pre-computed results from data/portfolio_analysis.csv (saved daily by cron).
Embeds the full interactive HTML report (with filters, sorting, search) inside Streamlit.
Also provides a CSV download button.
"""

import streamlit as st
import streamlit.components.v1 as components
import pandas as pd
import numpy as np
import json
import os
from datetime import datetime

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="📊 Portfolio Analyzer — Master",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# ── Minimal outer CSS ─────────────────────────────────────────────────────────
st.markdown("""
<style>
    .stApp { background-color: #0d1117; }
    .block-container { padding-top: 1rem; padding-bottom: 0rem; }
    .stDownloadButton > button {
        background: #238636 !important;
        color: white !important;
        border: none !important;
        border-radius: 6px !important;
        font-weight: 600 !important;
    }
    .stDownloadButton > button:hover { background: #2ea043 !important; }
    div[data-testid="metric-container"] {
        background: #161b22;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 8px 12px;
    }
</style>
""", unsafe_allow_html=True)


# ── Data paths ────────────────────────────────────────────────────────────────
DATA_DIR      = "data"
CSV_PATH      = os.path.join(DATA_DIR, "portfolio_analysis.csv")
RUN_INFO_PATH = os.path.join(DATA_DIR, "run_info.json")
TOP10_PATH    = os.path.join(DATA_DIR, "top10.json")


# ── Load data (cached, re-reads from disk every hour) ────────────────────────
@st.cache_data(ttl=3600)
def load_data():
    if not os.path.exists(CSV_PATH):
        return None, {}, []
    df = pd.read_csv(CSV_PATH)
    run_info = {}
    if os.path.exists(RUN_INFO_PATH):
        with open(RUN_INFO_PATH) as f:
            run_info = json.load(f)
    top10 = []
    if os.path.exists(TOP10_PATH):
        with open(TOP10_PATH) as f:
            top10 = json.load(f)
    return df, run_info, top10


# ── Build the embedded interactive HTML report ────────────────────────────────
def build_html_report(df: pd.DataFrame, run_ts: str) -> str:

    # ── Cell helpers ──────────────────────────────────────────────────────
    def _nan(v):
        if v is None: return True
        try:
            return (isinstance(v, float) and np.isnan(v)) or pd.isna(v)
        except Exception:
            return False

    def fmt(v, prefix="", suffix="", decimals=2, na="-"):
        try:
            if _nan(v): return na
            return prefix + "{:.{}f}".format(float(v), decimals) + suffix
        except Exception:
            return na

    def _score_cls(s):
        s = float(s)
        if s >= 78: return "score-strong"
        if s >= 62: return "score-buy"
        if s >= 44: return "score-hold"
        return "score-sell"

    def _action(s):
        s = float(s)
        if s >= 78: return "STRONG BUY"
        if s >= 62: return "BUY"
        if s >= 44: return "HOLD"
        return "SELL"

    def _row_cls(s):
        s = float(s)
        if s >= 62: return "row-green"
        if s >= 44: return "row-orange"
        return "row-red"

    def _ma_cell(row):
        ma = row.get("MA200"); vs = row.get("Vs_MA200")
        if _nan(ma) or _nan(vs): return '<td class="tc" data-sort="">-</td>'
        ms = "${:.2f}".format(float(ma)); vss = "{:+.1f}%".format(float(vs))
        sa = ' data-sort="{}"'.format(vs)
        if   float(vs) < -5: c = "ma-below"
        elif float(vs) < 0:  c = "ma-near"
        else:                c = "ma-above"
        return '<td class="tc {}"{}><span class="ma-val">{}</span><br><small>{}</small></td>'.format(c, sa, ms, vss)

    def _analyst_cells(row):
        tgt = row.get("Analyst_Target"); au = row.get("Analyst_Upside"); ac = row.get("Analyst_Count")
        if _nan(tgt): return '<td class="tc">-</td>', '<td class="tc">-</td>'
        ts = "${:.2f}".format(float(tgt))
        acs = " <small>({} analysts)</small>".format(int(float(ac))) if not _nan(ac) else ""
        if _nan(au): return '<td class="tc">{}{}</td>'.format(ts, acs), '<td class="tc">-</td>'
        cls = "analyst-up" if float(au) >= 0 else "analyst-dn"
        return ('<td class="tc">{}{}</td>'.format(ts, acs),
                '<td class="tc"><span class="{}">{:+.1f}%</span></td>'.format(cls, float(au)))

    def _rev_cell(row):
        rg = row.get("Rev_Growth"); rp = row.get("Rev_Growth_Prev")
        if _nan(rg): return '<td class="tc">-</td>'
        s = "{:.1f}%".format(float(rg))
        if not _nan(rp):
            a = float(rg) - float(rp)
            arrow = "▲" if a > 2 else ("▼" if a < -2 else "→")
            cls   = "accel-up" if a > 2 else ("accel-dn" if a < -2 else "")
            s += '<br><small class="{}">{} {:.1f}pp</small>'.format(cls, arrow, a)
        return '<td class="tc">' + s + '</td>'

    # ── Build all row HTML strings ────────────────────────────────────────
    rows_html_parts = []
    for _, row in df.iterrows():
        score = row.get("Score", 0)
        sc    = _score_cls(score); rec = _action(score); rc = _row_cls(score)
        sv    = str(row.get("Sector") or ""); nv = str(row.get("Name") or row.get("Ticker",""))
        tv    = str(row.get("Ticker") or ""); fv = str(row.get("Composite_Flag") or "—")
        flags_data = str(row.get("Composite_Flag") or "")

        sb = '<span class="badge {}">{}</span>'.format(sc, score)
        ab = '<span class="badge {}">{}</span>'.format(sc, rec)

        dv = row.get("Div_Yield")
        dc = ('<td class="tc div-cell">' if (not _nan(dv) and float(dv) > 0) else '<td class="tc">') + fmt(dv, suffix="%") + "</td>"

        es = row.get("EPS_Surprise")
        es_s = ('{:+.1f}%'.format(float(es)) if not _nan(es) else '-')
        es_c = "analyst-up" if (not _nan(es) and float(es) > 0) else ("analyst-dn" if (not _nan(es) and float(es) < 0) else "")
        esc = '<td class="tc"><span class="{}">{}</span></td>'.format(es_c, es_s)

        atc, auc = _analyst_cells(row)
        vs200 = row.get("Vs_MA200")
        mas = "none" if _nan(vs200) else ("below" if float(vs200) < 0 else "above")
        ib = row.get("Insider_Buy_Pct")

        # Moat cell
        moat_score = row.get("Moat_Score")
        moat_label = str(row.get("Moat_Label") or "—")
        if moat_label == "Wide":
            moat_cell = '<td class="tc"><span class="badge moat-wide">🏰 Wide</span><br><small>{}</small></td>'.format(
                fmt(moat_score, decimals=0) if not _nan(moat_score) else "")
        elif moat_label == "Narrow":
            moat_cell = '<td class="tc"><span class="badge moat-narrow">〰 Narrow</span><br><small>{}</small></td>'.format(
                fmt(moat_score, decimals=0) if not _nan(moat_score) else "")
        elif moat_label == "Weak":
            moat_cell = '<td class="tc"><span class="badge moat-weak">Weak</span><br><small>{}</small></td>'.format(
                fmt(moat_score, decimals=0) if not _nan(moat_score) else "")
        else:
            moat_cell = '<td class="tc" data-sort="0"><small style="color:#8b949e">—</small></td>'

        cells = (
            "<td><strong>{}</strong><br><small>{}</small></td>".format(tv, nv)
            + '<td class="tc">' + sb + "</td>"
            + '<td class="tc">' + ab + "</td>"
            + '<td class="tc flag-cell">' + fv + "</td>"
            + moat_cell
            + '<td class="tr">'  + fmt(row.get("Price"),        prefix="$")             + "</td>"
            + _ma_cell(row)
            + atc + auc
            + '<td class="tc">'  + str(row.get("Mkt_Cap") or "-")                       + "</td>"
            + '<td class="tc">'  + fmt(row.get("PEG"))                                  + "</td>"
            + '<td class="tc">'  + fmt(row.get("PE_Fwd"),       decimals=1)             + "</td>"
            + '<td class="tc">'  + fmt(row.get("PS"),           decimals=1)             + "</td>"
            + '<td class="tc">'  + fmt(row.get("EV_EBITDA"),    decimals=1)             + "</td>"
            + '<td class="tc">'  + fmt(row.get("ROE"),          suffix="%",decimals=1)  + "</td>"
            + _rev_cell(row)
            + '<td class="tc">'  + fmt(row.get("Gross_Margin"), suffix="%",decimals=1)  + "</td>"
            + '<td class="tc">'  + fmt(row.get("FCF_Yield"),    suffix="%",decimals=1)  + "</td>"
            + esc
            + '<td class="tc">'  + fmt(row.get("From_Low_Pct"),  suffix="%",decimals=1) + "</td>"
            + '<td class="tc">'  + fmt(row.get("From_High_Pct"), suffix="%",decimals=1) + "</td>"
            + '<td class="tc">'  + fmt(row.get("Debt_Equity"))                          + "</td>"
            + '<td class="tc">'  + fmt(row.get("Beta"))                                 + "</td>"
            + '<td class="tc">'  + fmt(row.get("Short_Float"),  suffix="%",decimals=1)  + "</td>"
            + '<td class="tc">'  + fmt(ib,                      suffix="%",decimals=0)  + "</td>"
            + dc
            + '<td class="tc">'  + fmt(row.get("Payout_Ratio"), suffix="%",decimals=1)  + "</td>"
            + '<td class="tc">'  + fmt(row.get("Op_Margin"),    suffix="%",decimals=1)  + "</td>"
            + '<td class="tc">'  + fmt(row.get("ROA"),          suffix="%",decimals=1)  + "</td>"
            + '<td class="tc">'  + fmt(row.get("Current_Ratio"),decimals=2)             + "</td>"
            + '<td class="tc"><small>' + sv + "</small></td>"
        )
        rows_html_parts.append(
            '<tr class="{}" data-action="{}" data-sector="{}" data-ma="{}" data-score="{}" data-flags="{}" data-moat="{}">'.format(
                rc, rec, sv, mas, score, flags_data, moat_label) + cells + "</tr>"
        )

    rows_html = "\n".join(rows_html_parts)

    # ── Stats ─────────────────────────────────────────────────────────────
    total = len(df)
    sb_c  = int((df["Action"] == "STRONG BUY").sum()) if "Action" in df.columns else 0
    b_c   = int((df["Action"] == "BUY").sum())        if "Action" in df.columns else 0
    h_c   = int((df["Action"] == "HOLD").sum())       if "Action" in df.columns else 0
    s_c   = int((df["Action"] == "SELL").sum())       if "Action" in df.columns else 0
    avg   = round(float(df["Score"].mean()), 1)       if "Score"  in df.columns else 0

    sectors = sorted(df["Sector"].dropna().unique()) if "Sector" in df.columns else []
    sec_btns = "".join(
        '<button class="sec-btn" data-sector="{}" onclick="toggleSector(this)">{}</button>\n'.format(s, s)
        for s in sectors
    )

    def TH(label, i, cls=""):
        return '<th onclick="sortCol({})"{}>{}</th>\n'.format(i, ' class="{}"'.format(cls) if cls else "", label)

    headers = (
        TH("Ticker / Name", 0) + TH("Score", 1) + TH("Action", 2)
        + TH("Signal Flags", 3, "new-col")
        + TH("Moat", 4, "moat-col")
        + TH("Price", 5) + TH("200 DMA", 6, "ma-col")
        + TH("Analyst Target", 7, "new-col") + TH("Upside %", 8, "new-col")
        + TH("Mkt Cap", 9) + TH("PEG", 10) + TH("P/E Fwd", 11)
        + TH("P/S", 12) + TH("EV/EBITDA", 13, "new-col")
        + TH("ROE %", 14) + TH("Rev Gr %", 15) + TH("Gross Mgn %", 16)
        + TH("FCF Yld %", 17) + TH("EPS Surp %", 18, "new-col")
        + TH("From Low %", 19) + TH("From High %", 20)
        + TH("D/E", 21) + TH("Beta", 22) + TH("Short %", 23)
        + TH("Insider Buy%", 24, "new-col")
        + TH("Div Yield %", 25, "div-col") + TH("Payout %", 26, "div-col")
        + TH("Op Margin %", 27, "div-col") + TH("ROA %", 28, "div-col")
        + TH("Curr Ratio", 29, "div-col")
        + TH("Sector", 30)
    )

    CSS = """
/* ── Reset & base ───────────────────────────────────────── */
*{box-sizing:border-box;margin:0;padding:0;-webkit-tap-highlight-color:transparent}
html{-webkit-text-size-adjust:100%}
body{background:#0a0d13;color:#cdd9e5;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif;font-size:13px}

/* ── Stats row ──────────────────────────────────────────── */
.cards{display:flex;flex-wrap:wrap;gap:6px;margin-bottom:10px}
.stat-card{background:#161b22;border:1px solid #21262d;border-radius:10px;padding:8px 12px;flex:1 1 60px;text-align:center;min-width:55px}
.stat-card .num{font-size:1.15rem;font-weight:700;line-height:1;display:block}
.stat-card .lbl{font-size:9px;color:#6e7681;text-transform:uppercase;letter-spacing:.6px;margin-top:2px;display:block}

/* ── Top bar ─────────────────────────────────────────────── */
.topbar{display:flex;gap:8px;align-items:center;margin-bottom:8px;flex-wrap:wrap}
input#srch{background:#161b22;border:1px solid #21262d;color:#cdd9e5;border-radius:8px;padding:9px 12px;flex:1;min-width:0;outline:none;font-size:16px;-webkit-appearance:none}
input#srch:focus{border-color:#58a6ff;background:#1c2230}
input#minScore{background:#161b22;border:1px solid #21262d;color:#cdd9e5;border-radius:8px;padding:9px 8px;width:64px;outline:none;font-size:16px;text-align:center;-webkit-appearance:none}
.btn-toggle-filters{background:#161b22;border:1.5px solid #21262d;color:#cdd9e5;border-radius:8px;padding:9px 14px;cursor:pointer;font-size:13px;font-weight:600;white-space:nowrap;-webkit-appearance:none}
.btn-toggle-filters.has-active{border-color:#58a6ff;color:#58a6ff}
.btn-csv{background:#238636;color:#fff;border:none;border-radius:8px;padding:9px 13px;cursor:pointer;font-size:13px;font-weight:600;white-space:nowrap;-webkit-appearance:none}

/* ── Collapsible filter panel ────────────────────────────── */
.filter-panel{display:none;background:#0d1117;border:1px solid #21262d;border-radius:10px;padding:12px;margin-bottom:10px}
.filter-panel.open{display:block}
.fg{margin-bottom:12px}
.fl{font-size:10px;color:#6e7681;text-transform:uppercase;letter-spacing:.6px;font-weight:600;margin-bottom:6px}
.btn-row{display:flex;flex-wrap:wrap;gap:6px}

/* ── Filter buttons ──────────────────────────────────────── */
.act-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:8px;padding:7px 12px;cursor:pointer;font-size:12px;font-weight:600;-webkit-appearance:none}
#btnAll.active{background:#21262d;color:#cdd9e5;border-color:#30363d}
.act-btn[data-action="STRONG BUY"].active{background:#1f4f98;color:#79c0ff;border-color:#1f6feb}
.act-btn[data-action="BUY"].active{background:#1a3f27;color:#56d364;border-color:#238636}
.act-btn[data-action="HOLD"].active{background:#3d2e00;color:#e3b341;border-color:#9e6a03}
.act-btn[data-action="SELL"].active{background:#3d0f0f;color:#f85149;border-color:#b62324}
.sec-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:6px;padding:5px 10px;cursor:pointer;font-size:11px;-webkit-appearance:none}
#btnSecAll.active,.sec-btn[data-sector].active{background:#1c2a3d;color:#58a6ff;border-color:#1f6feb}
.moat-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:6px;padding:5px 10px;cursor:pointer;font-size:11px;-webkit-appearance:none}
.moat-btn[data-moat="Wide"].active{background:#2d1f5e;color:#a78bfa;border-color:#7c3aed}
.moat-btn[data-moat="Narrow"].active{background:#1e1b4b;color:#818cf8;border-color:#4f46e5}
.moat-btn[data-moat="Weak"].active{background:#1f2937;color:#9ca3af;border-color:#6b7280}
.flag-filter-btn{background:#161b22;border:1px solid #21262d;color:#6e7681;border-radius:6px;padding:5px 10px;cursor:pointer;font-size:11px;-webkit-appearance:none}
.flag-filter-btn.active{background:#2a1f3d;color:#bc8cff;border-color:#bc8cff}
.ma-filter-btn{background:#161b22;border:1.5px solid #1f6feb;color:#58a6ff;border-radius:8px;padding:7px 13px;cursor:pointer;font-size:12px;font-weight:700;-webkit-appearance:none}
.ma-filter-btn.active{background:#58a6ff;color:#0a0d13}

/* ── Table ───────────────────────────────────────────────── */
.wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;max-height:78vh;border:1px solid #21262d;border-radius:10px}
table{border-collapse:collapse;width:100%;white-space:nowrap}
thead th{background:#161b22;color:#6e7681;position:sticky;top:0;z-index:9;padding:9px 8px;cursor:pointer;user-select:none;font-weight:700;font-size:11px;text-transform:uppercase;letter-spacing:.4px;border-bottom:1px solid #21262d}
thead th:hover{color:#58a6ff}
thead th.asc::after{content:" ▲";font-size:8px;opacity:.7}
thead th.desc::after{content:" ▼";font-size:8px;opacity:.7}
thead th.new-col{color:#bc8cff}
thead th.ma-col{color:#58a6ff}
thead th.div-col{color:#e6b450}
thead th.moat-col{color:#a78bfa}
td{padding:7px 8px;border-bottom:1px solid #1a1f27;vertical-align:middle}
td small{color:#6e7681;font-size:11px}
.tc{text-align:center}.tr{text-align:right}
.row-green{background:#0b1d10}.row-green:hover{background:#0e2615}
.row-orange{background:#1a1200}.row-orange:hover{background:#221800}
.row-red{background:#160808}.row-red:hover{background:#1e0c0c}

/* ── Badges & status colors ──────────────────────────────── */
.badge{font-size:11px;padding:3px 8px;border-radius:5px;font-weight:700}
.score-strong{background:#1f4f98;color:#79c0ff}
.score-buy{background:#1a3f27;color:#56d364}
.score-hold{background:#3d2e00;color:#e3b341}
.score-sell{background:#3d0f0f;color:#f85149}
.div-cell{color:#e6b450;font-weight:600}
.flag-cell{font-size:11px;max-width:200px;white-space:normal;line-height:1.5;color:#8b949e}
.analyst-up{color:#3fb950;font-weight:600}
.analyst-dn{color:#f85149;font-weight:600}
.accel-up{color:#3fb950;font-size:11px}
.accel-dn{color:#f85149;font-size:11px}
.ma-above .ma-val{color:#3fb950;font-weight:600}
.ma-above small{color:#3fb950}
.ma-near .ma-val{color:#d29922;font-weight:600}
.ma-near small{color:#d29922}
.ma-below .ma-val{color:#f85149;font-weight:600}
.ma-below small{color:#f85149}
.moat-wide{background:#2d1f5e;color:#a78bfa}
.moat-narrow{background:#1e1b4b;color:#818cf8}
.moat-weak{background:#1f2937;color:#9ca3af}
#rowcnt{font-size:11px;color:#6e7681;margin:4px 0 6px}

/* ── Mobile ──────────────────────────────────────────────── */
@media(max-width:600px){
  input#srch,input#minScore{font-size:16px}
  .stat-card .num{font-size:1rem}
  thead th{padding:7px 6px;font-size:10px}
  td{padding:6px 6px}
}
"""

    JS = """
var sortDir = {};
function toggleFilters(){
    var p=document.getElementById('filterPanel'), b=document.getElementById('btnFilters');
    var open=p.classList.toggle('open');
    b.textContent = open ? '▲ Hide Filters' : '▼ Filters';
}
function updateFilterBtn(){
    var active=document.querySelectorAll('.act-btn[data-action].active, .sec-btn[data-sector].active, .flag-filter-btn.active, .ma-filter-btn.active, .moat-btn.active').length;
    var b=document.getElementById('btnFilters');
    var panel=document.getElementById('filterPanel');
    if(active>0){ b.classList.add('has-active'); b.textContent=(panel.classList.contains('open')?'▲ Hide Filters':'▼ Filters ('+active+' active)'); }
    else{ b.classList.remove('has-active'); b.textContent=(panel.classList.contains('open')?'▲ Hide Filters':'▼ Filters'); }
}
function toggleAction(b){ b.classList.toggle('active'); var a=document.querySelectorAll('.act-btn[data-action].active').length>0; document.getElementById('btnAll').classList.toggle('active',!a); applyFilters(); updateFilterBtn(); }
function toggleAll(){ document.querySelectorAll('.act-btn[data-action]').forEach(function(b){b.classList.remove('active');}); document.getElementById('btnAll').classList.add('active'); applyFilters(); updateFilterBtn(); }
function toggleSector(b){ b.classList.toggle('active'); var a=document.querySelectorAll('.sec-btn[data-sector].active').length>0; document.getElementById('btnSecAll').classList.toggle('active',!a); applyFilters(); updateFilterBtn(); }
function toggleSecAll(){ document.querySelectorAll('.sec-btn[data-sector]').forEach(function(b){b.classList.remove('active');}); document.getElementById('btnSecAll').classList.add('active'); applyFilters(); updateFilterBtn(); }
function toggleMA(b){ b.classList.toggle('active'); applyFilters(); updateFilterBtn(); }
function toggleFlag(b){ b.classList.toggle('active'); applyFilters(); updateFilterBtn(); }
function toggleMoat(b){ b.classList.toggle('active'); applyFilters(); updateFilterBtn(); }
function applyFilters(){
    var q=document.getElementById('srch').value.toLowerCase().trim();
    var ms=parseFloat(document.getElementById('minScore').value)||0;
    var aa=Array.from(document.querySelectorAll('.act-btn[data-action].active')).map(function(b){return b.getAttribute('data-action');});
    var allA=document.getElementById('btnAll').classList.contains('active')||aa.length===0;
    var sa=Array.from(document.querySelectorAll('.sec-btn[data-sector].active')).map(function(b){return b.getAttribute('data-sector');});
    var allS=document.getElementById('btnSecAll').classList.contains('active')||sa.length===0;
    var maB=document.getElementById('btnMABelow').classList.contains('active');
    var fa=Array.from(document.querySelectorAll('.flag-filter-btn.active')).map(function(b){return b.getAttribute('data-flag');});
    var ma=Array.from(document.querySelectorAll('.moat-btn.active')).map(function(b){return b.getAttribute('data-moat');});
    document.querySelectorAll('#tbody tr').forEach(function(r){
        var ok=((!q||r.innerText.toLowerCase().indexOf(q)!==-1))
            &&(allA||aa.indexOf(r.getAttribute('data-action')||'')!==-1)
            &&(allS||sa.indexOf(r.getAttribute('data-sector')||'')!==-1)
            &&(!maB||(r.getAttribute('data-ma')||'')==='below')
            &&(parseFloat(r.getAttribute('data-score')||0)>=ms)
            &&(fa.length===0||fa.some(function(f){return (r.getAttribute('data-flags')||'').indexOf(f)!==-1;}))
            &&(ma.length===0||ma.indexOf(r.getAttribute('data-moat')||'')!==-1);
        r.style.display=ok?'':'none';
    });
    updateCount();
}
function sortCol(c){
    var tb=document.getElementById('tbody'),rows=Array.from(tb.querySelectorAll('tr'));
    sortDir[c]=-(sortDir[c]||1); var d=sortDir[c];
    rows.sort(function(a,b){
        var ac=a.cells[c],bc=b.cells[c];
        var av=(ac&&ac.hasAttribute('data-sort'))?ac.getAttribute('data-sort'):(ac?ac.innerText.trim():'');
        var bv=(bc&&bc.hasAttribute('data-sort'))?bc.getAttribute('data-sort'):(bc?bc.innerText.trim():'');
        var an=parseFloat(av.replace(/[^\\d.\\-]/g,'')),bn=parseFloat(bv.replace(/[^\\d.\\-]/g,''));
        return (!isNaN(an)&&!isNaN(bn))?d*(an-bn):d*av.localeCompare(bv);
    });
    document.querySelectorAll('thead th').forEach(function(h,i){h.classList.remove('asc','desc');if(i===c)h.classList.add(d===1?'desc':'asc');});
    rows.forEach(function(r){tb.appendChild(r);});
    updateCount();
}
function updateCount(){
    var v=0; document.querySelectorAll('#tbody tr').forEach(function(r){if(r.style.display!=='none')v++;});
    document.getElementById('rowcnt').textContent='Showing '+v+' of TOTAL stocks';
}
function exportCSV(){
    var hdrs=Array.from(document.querySelectorAll('thead th')).map(function(h){return h.innerText.replace(/[▲▼]/g,'').trim();});
    var rows=Array.from(document.querySelectorAll('#tbody tr')).filter(function(r){return r.style.display!=='none';});
    var csv=hdrs.join(',')+String.fromCharCode(10);
    rows.forEach(function(r){csv+=Array.from(r.cells).map(function(c){return '"'+c.innerText.replace(/[\\r\\n]+/g,' ').trim()+'"';}).join(',')+String.fromCharCode(10);});
    var a=document.createElement('a');
    a.href=URL.createObjectURL(new Blob([csv],{type:'text/csv'}));
    a.download='portfolio_analysis_v2.csv';
    document.body.appendChild(a);a.click();document.body.removeChild(a);
}
window.onload=function(){updateCount();};
""".replace("TOTAL", str(total))

    return """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">
<meta name="theme-color" content="#0a0d13">
<style>{css}</style>
</head>
<body>
<div style="padding:10px 12px">

<!-- Stats row -->
<div class="cards">
  <div class="stat-card"><span class="num">{total}</span><span class="lbl">Stocks</span></div>
  <div class="stat-card"><span class="num" style="color:#79c0ff">{sb}</span><span class="lbl">Str. Buy</span></div>
  <div class="stat-card"><span class="num" style="color:#56d364">{b}</span><span class="lbl">Buy</span></div>
  <div class="stat-card"><span class="num" style="color:#e3b341">{h}</span><span class="lbl">Hold</span></div>
  <div class="stat-card"><span class="num" style="color:#f85149">{s}</span><span class="lbl">Sell</span></div>
  <div class="stat-card"><span class="num">{avg}</span><span class="lbl">Avg Score</span></div>
  <div class="stat-card"><span class="num" style="font-size:.7rem;color:#6e7681">{run_ts}</span><span class="lbl">Last Run</span></div>
</div>

<!-- Always-visible top bar -->
<div class="topbar">
  <input id="srch" type="search" autocomplete="off" autocorrect="off" spellcheck="false"
         placeholder="🔍 Ticker or name…" oninput="applyFilters()">
  <input id="minScore" type="number" value="0" min="0" max="100"
         placeholder="≥Score" oninput="applyFilters()" title="Min score">
  <button id="btnFilters" class="btn-toggle-filters" onclick="toggleFilters()">▼ Filters</button>
  <button class="btn-csv" onclick="exportCSV()">⬇ CSV</button>
</div>

<!-- Collapsible filter panel -->
<div id="filterPanel" class="filter-panel">
  <div class="fg"><div class="fl">🏰 Moat</div><div class="btn-row">
    <button class="moat-btn" data-moat="Wide"   onclick="toggleMoat(this)">🏰 Wide Moat</button>
    <button class="moat-btn" data-moat="Narrow" onclick="toggleMoat(this)">〰 Narrow Moat</button>
    <button class="moat-btn" data-moat="Weak"   onclick="toggleMoat(this)">Weak / None</button>
  </div></div>
  <div class="fg"><div class="fl">Action</div><div class="btn-row">
    <button id="btnAll" class="act-btn active" onclick="toggleAll()">ALL</button>
    <button class="act-btn" data-action="STRONG BUY" onclick="toggleAction(this)">⭐ Strong Buy</button>
    <button class="act-btn" data-action="BUY"        onclick="toggleAction(this)">✅ Buy</button>
    <button class="act-btn" data-action="HOLD"       onclick="toggleAction(this)">⏸ Hold</button>
    <button class="act-btn" data-action="SELL"       onclick="toggleAction(this)">🔴 Sell</button>
  </div></div>
  <div class="fg"><div class="fl">200 DMA</div>
    <button id="btnMABelow" class="ma-filter-btn" onclick="toggleMA(this)">Below 200 MA</button>
  </div>
  <div class="fg"><div class="fl">Signal Flags</div><div class="btn-row">
    <button class="flag-filter-btn" data-flag="Compounder"         onclick="toggleFlag(this)">⭐ Compounder</button>
    <button class="flag-filter-btn" data-flag="Accel Growth"       onclick="toggleFlag(this)">🚀 Accel Growth</button>
    <button class="flag-filter-btn" data-flag="Deep Value"         onclick="toggleFlag(this)">💎 Deep Value</button>
    <button class="flag-filter-btn" data-flag="Analyst Conviction" onclick="toggleFlag(this)">📈 Analyst Conv.</button>
    <button class="flag-filter-btn" data-flag="Income"             onclick="toggleFlag(this)">💰 Income</button>
    <button class="flag-filter-btn" data-flag="Rule of 40"         onclick="toggleFlag(this)">📐 Rule of 40</button>
  </div></div>
  <div class="fg"><div class="fl">Sector</div><div class="btn-row">
    <button id="btnSecAll" class="sec-btn active" onclick="toggleSecAll()">ALL</button>
    {sec_btns}
  </div></div>
</div>

<div id="rowcnt"></div>
<div class="wrap">
  <table id="tbl">
    <thead><tr>{headers}</tr></thead>
    <tbody id="tbody">{rows_html}</tbody>
  </table>
</div>
<p style="font-size:10px;color:#6e7681;margin-top:8px;padding-bottom:4px">
  ⚠️ Quantitative screen only — not financial advice.
</p>
</div>
<script>{js}</script>
</body></html>""".format(
        css=CSS, js=JS,
        run_ts=run_ts.replace("UTC","").strip(),
        total=total, sb=sb_c, b=b_c, h=h_c, s=s_c, avg=avg,
        sec_btns=sec_btns, headers=headers, rows_html=rows_html,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
df, run_info, top10 = load_data()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    '<h1 style="color:#58a6ff;margin-bottom:2px">📊 Stock Portfolio Analyzer — Master</h1>'
    '<p style="color:#8b949e;font-size:12px;margin-top:0">344 stocks · Sector-relative scoring · Moat analysis (Brand · Switching · Network) · Updated daily at 10am</p>',
    unsafe_allow_html=True,
)

if df is None:
    st.warning("⏳ No results yet — `data/portfolio_analysis.csv` not found.")
    st.info("Run `python run_analysis.py` manually once to generate the first results.")
    st.stop()

run_ts  = run_info.get("run_timestamp_utc", "Unknown")
elapsed = run_info.get("elapsed_minutes", "?")
wide_moat   = run_info.get("wide_moat_count", "?")
narrow_moat = run_info.get("narrow_moat_count", "?")
hg_rocket   = run_info.get("hg_rocket_count", "?")
hg_high     = run_info.get("hg_high_count", "?")
st.caption(f"🕐 Last analysis: **{run_ts}** · {elapsed} min · 🏰 Wide Moat: {wide_moat} · 〰 Narrow: {narrow_moat} · 🚀 Rocket: {hg_rocket} · 🔥 HG High: {hg_high}")

# ── Download buttons (Streamlit-native, top of page) ─────────────────────────
dl1, dl2, _ = st.columns([1.2, 1.2, 7])
with dl1:
    st.download_button(
        "⬇️ Download Full CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=f"portfolio_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )
with dl2:
    if top10:
        top10_bytes = pd.DataFrame(top10).to_csv(index=False).encode("utf-8")
        st.download_button(
            "⬇️ Top 10 CSV",
            data=top10_bytes,
            file_name=f"top10_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5 = st.tabs(["📋 Full Screener", "⭐ Top 10 Picks", "🏰 Moat Leaderboard", "🚀 Hypergrowth Hunter", "🧙 Magic Formula"])

with tab1:
    # Build and embed the full interactive HTML report
    html_out = build_html_report(df, run_ts)
    components.html(html_out, height=920, scrolling=False)

with tab2:
    if not top10:
        st.info("No top picks available.")
    else:
        st.subheader(f"🏆 Top {len(top10)} Recommendations")
        st.caption("Multi-factor conviction scoring · Sector-diversified · Risk-filtered · Moat-boosted")

        def fp(v, plus=True):
            if v is None or (isinstance(v, float) and np.isnan(float(v) if v else 0)): return "N/A"
            try:
                fv = float(v)
                sign = "+" if plus and fv > 0 else ""
                return f"{sign}{fv:.1f}%"
            except Exception:
                return "N/A"

        def fpr(v):
            try:
                return f"${float(v):.2f}" if v else "N/A"
            except Exception:
                return "N/A"

        for i, r in enumerate(top10, 1):
            upside = r.get("upside"); roe = r.get("roe"); fcf = r.get("fcf")
            de = r.get("de"); beta = r.get("beta"); rev_g = r.get("rev_g")
            op_m = r.get("op_m"); gross_m = r.get("gross_m"); eps_s = r.get("eps_s")
            rev_prev = r.get("rev_prev")
            try:
                accel = float(rev_g) - float(rev_prev) if (rev_g and rev_prev) else None
            except Exception:
                accel = None

            with st.expander(
                f"#{i}  **{r['ticker']}**  —  {str(r.get('name',''))[:50]}  |  Score: {r['base_score']}  |  {r.get('action','')}",
                expanded=(i <= 3),
            ):
                c1, c2, c3 = st.columns(3)
                with c1:
                    st.markdown("**💰 Price & Target**")
                    st.write(f"Price:  **{fpr(r.get('price'))}**")
                    st.write(f"Target: **{fpr(r.get('target'))}**  ({r.get('analyst_count') or 'N/A'} analysts)")
                    color = "green" if upside and float(upside) > 0 else "red"
                    st.markdown(f"Upside: **:{color}[{fp(upside)}]**")
                    st.write(f"Sector: {r.get('sector')}  ·  Cap: {r.get('mkt_cap','N/A')}")
                    # Moat badge
                    moat_label = r.get("moat_label", "None")
                    moat_score_val = r.get("moat_score")
                    moat_icons  = {"Wide": "🏰", "Narrow": "〰️", "Weak": "◽", "None": "—"}
                    moat_colors = {"Wide": "violet", "Narrow": "blue", "Weak": "gray", "None": "gray"}
                    mi  = moat_icons.get(moat_label, "—")
                    mc_ = moat_colors.get(moat_label, "gray")
                    ms_str = f"  ({moat_score_val:.0f}/100)" if moat_score_val is not None else ""
                    st.markdown(f"Moat: **:{mc_}[{mi} {moat_label}{ms_str}]**")
                with c2:
                    st.markdown("**📊 Quality**")
                    st.write(f"ROE:       {fp(roe, plus=False)}")
                    st.write(f"FCF Yield: {fp(fcf, plus=False)}")
                    st.write(f"Op Margin: {fp(op_m, plus=False)}")
                    st.write(f"Gross Mgn: {fp(gross_m, plus=False)}")
                with c3:
                    st.markdown("**🚀 Growth & Risk**")
                    accel_str = f"  ({'▲' if accel and accel>0 else '▼'} {abs(accel):.1f}pp)" if accel else ""
                    st.write(f"Rev Growth:   {fp(rev_g)}{accel_str}")
                    st.write(f"EPS Surprise: {fp(eps_s) if eps_s else 'N/A'}")
                    st.write(f"Debt/Equity:  {f'{float(de):.2f}' if de else 'N/A'}")
                    st.write(f"Beta:         {f'{float(beta):.2f}' if beta else 'N/A'}")
                flags = r.get("flags","")
                if flags and flags != "—":
                    st.markdown(f"**🏷️ Flags:** {flags}")
                st.caption(f"Base Score: {r['base_score']}  ·  Conviction: {r['conv_score']}")

with tab3:
    st.subheader("🏰 Moat Leaderboard")
    st.caption("Stocks ranked by economic moat score · Three pillars: Brand/Pricing Power · Switching Costs · Network Effects")

    if "Moat_Label" not in df.columns:
        st.info("Moat scores not available — re-run `python run_analysis.py` to generate them.")
    else:
        mc1, mc2, _ = st.columns([1.5, 1.5, 4])
        with mc1:
            moat_filter = st.selectbox("Moat Tier", ["All", "Wide", "Narrow", "Weak", "None"])
        with mc2:
            action_filter_m = st.selectbox("Action", ["All", "STRONG BUY", "BUY", "HOLD", "SELL"])

        moat_df = df[["Ticker", "Name", "Sector", "Score", "Action",
                       "Moat_Score", "Moat_Label", "Moat_Brand", "Moat_Switching", "Moat_Network",
                       "Gross_Margin", "ROE", "Rev_Growth", "FCF_Yield", "Inst_Own",
                       "Analyst_Upside", "Composite_Flag"]].copy()

        if moat_filter != "All":
            moat_df = moat_df[moat_df["Moat_Label"] == moat_filter]
        if action_filter_m != "All":
            moat_df = moat_df[moat_df["Action"] == action_filter_m]
        moat_df = moat_df.sort_values("Moat_Score", ascending=False).head(50)

        wide_n   = int((df["Moat_Label"] == "Wide").sum())
        narrow_n = int((df["Moat_Label"] == "Narrow").sum())
        weak_n   = int((df["Moat_Label"] == "Weak").sum())
        none_n   = int((df["Moat_Label"] == "None").sum())
        ma, mb, mc_col, md = st.columns(4)
        ma.metric("🏰 Wide Moat",   wide_n)
        mb.metric("〰️ Narrow Moat", narrow_n)
        mc_col.metric("◽ Weak",     weak_n)
        md.metric("— None",         none_n)
        st.markdown("---")

        for _, row in moat_df.iterrows():
            label   = str(row.get("Moat_Label") or "None")
            mscore  = row.get("Moat_Score")
            brand_s = row.get("Moat_Brand")
            sw_s    = row.get("Moat_Switching")
            net_s   = row.get("Moat_Network")
            badge   = {"Wide": "🟣", "Narrow": "🔵", "Weak": "⚫", "None": "⚫"}.get(label, "⚫")
            ms_str  = f"{mscore:.0f}/100" if mscore is not None else "N/A"

            with st.expander(
                f"{badge} **{row['Ticker']}**  —  {str(row.get('Name',''))[:40]}"
                f"  |  Moat: **{label}** ({ms_str})  |  Score: {row['Score']}  |  {row['Action']}",
                expanded=False,
            ):
                p1, p2, p3, p4 = st.columns(4)
                with p1:
                    st.markdown("**🏰 Overall Moat**")
                    st.metric("Moat Score", ms_str)
                    st.write(f"Tier: **{label}**")
                    st.write(f"Sector: {row.get('Sector','')}")
                with p2:
                    st.markdown("**💪 Brand / Pricing Power**")
                    st.metric("Pillar", f"{brand_s:.1f}" if brand_s is not None else "N/A")
                    gm  = row.get("Gross_Margin")
                    fcf = row.get("FCF_Yield")
                    st.write(f"Gross Margin: {f'{gm:.1f}%' if gm else 'N/A'}")
                    st.write(f"FCF Yield:    {f'{fcf:.1f}%' if fcf else 'N/A'}")
                with p3:
                    st.markdown("**🔒 Switching Costs**")
                    st.metric("Pillar", f"{sw_s:.1f}" if sw_s is not None else "N/A")
                    roe = row.get("ROE"); rg = row.get("Rev_Growth")
                    st.write(f"ROE:        {f'{roe:.1f}%' if roe else 'N/A'}")
                    st.write(f"Rev Growth: {f'{rg:.1f}%' if rg else 'N/A'}")
                with p4:
                    st.markdown("**🌐 Network Effects**")
                    st.metric("Pillar", f"{net_s:.1f}" if net_s is not None else "N/A")
                    io = row.get("Inst_Own"); au = row.get("Analyst_Upside")
                    rg = row.get("Rev_Growth")
                    st.write(f"Rev Growth: {f'{rg:.1f}%' if rg else 'N/A'}")
                    st.write(f"Inst. Own:  {f'{io:.0f}%' if io else 'N/A'}")
                    st.write(f"Analyst Up: {f'+{au:.1f}%' if au and au > 0 else (f'{au:.1f}%' if au else 'N/A')}")
                flags = str(row.get("Composite_Flag") or "")
                if flags and flags != "—":
                    st.markdown(f"**🏷️ Flags:** {flags}")

        st.markdown("---")
        moat_dl = df[["Ticker", "Name", "Sector", "Score", "Action",
                       "Moat_Score", "Moat_Label", "Moat_Brand", "Moat_Switching", "Moat_Network",
                       "Gross_Margin", "Op_Margin", "FCF_Yield", "ROE", "Rev_Growth",
                       "Inst_Own", "Analyst_Upside", "Composite_Flag"]].copy()
        moat_dl = moat_dl.sort_values("Moat_Score", ascending=False)
        st.download_button(
            "⬇️ Download Moat Data CSV",
            data=moat_dl.to_csv(index=False).encode("utf-8"),
            file_name=f"moat_analysis_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )


with tab4:
    st.subheader("🚀 Hypergrowth Hunter")
    st.caption(
        "Finds the next potential 10x stocks · Ignores valuation multiples · "
        "Scores: Growth Trajectory · Operating Leverage · PMF & Stickiness · Discovery Phase"
    )
    st.info(
        "⚠️ These stocks look **expensive** by traditional metrics — that's the point. "
        "Early NVDA, TSLA, SHOP all scored poorly on P/E but high on these signals. "
        "Always do your own research. This is a discovery tool, not a buy recommendation.",
        icon="💡"
    )

    if "HG_Label" not in df.columns:
        st.warning("Hypergrowth scores not available — re-run `python run_analysis.py` to generate them.")
    else:
        # ── Controls ──────────────────────────────────────────────────────────
        hc1, hc2, hc3, hc4 = st.columns([1.5, 1.5, 1.5, 2])
        with hc1:
            hg_tier = st.selectbox("HG Tier", ["All", "🚀 Rocket", "🔥 High", "📈 Emerging"])
        with hc2:
            hg_sector = st.selectbox("Sector", ["All"] + sorted(df["Sector"].dropna().unique().tolist()))
        with hc3:
            min_rev_growth = st.number_input("Min Rev Growth %", value=15, min_value=0, max_value=200, step=5)
        with hc4:
            show_only_accel = st.checkbox("Only accelerating (streak ≥ 2)", value=False)

        # ── Filter ────────────────────────────────────────────────────────────
        hg_df = df.copy()
        if hg_tier != "All":
            hg_df = hg_df[hg_df["HG_Label"] == hg_tier]
        if hg_sector != "All":
            hg_df = hg_df[hg_df["Sector"] == hg_sector]
        if min_rev_growth > 0:
            hg_df = hg_df[hg_df["Rev_Growth"].fillna(0) >= min_rev_growth]
        if show_only_accel:
            hg_df = hg_df[hg_df["Rev_Accel_Streak"].fillna(0) >= 2]

        hg_df = hg_df.sort_values("HG_Score", ascending=False).head(40)

        # ── Summary stats ─────────────────────────────────────────────────────
        rocket_n   = int((df["HG_Label"] == "🚀 Rocket").sum())
        high_n     = int((df["HG_Label"] == "🔥 High").sum())
        emerging_n = int((df["HG_Label"] == "📈 Emerging").sum())
        avg_hg     = round(df["HG_Score"].dropna().mean(), 1)

        sm1, sm2, sm3, sm4 = st.columns(4)
        sm1.metric("🚀 Rocket",    rocket_n)
        sm2.metric("🔥 High",      high_n)
        sm3.metric("📈 Emerging",  emerging_n)
        sm4.metric("Avg HG Score", avg_hg)
        st.markdown("---")

        # ── Leaderboard ───────────────────────────────────────────────────────
        if len(hg_df) == 0:
            st.info("No stocks match the current filters.")
        else:
            st.caption(f"Showing {len(hg_df)} stocks sorted by Hypergrowth Score")

            for _, row in hg_df.iterrows():
                hg_score   = row.get("HG_Score")
                hg_label   = str(row.get("HG_Label") or "—")
                hg_growth  = row.get("HG_Growth")
                hg_lev     = row.get("HG_Leverage")
                hg_pmf     = row.get("HG_PMF")
                hg_disc    = row.get("HG_Discovery")

                streak   = row.get("Rev_Accel_Streak")
                gm_exp   = row.get("GM_Expansion_4Q")
                ol_ratio = row.get("Op_Leverage_Ratio")
                r40      = row.get("Rule_Of_40")
                rd_pct   = row.get("RD_Pct_Rev")
                evsg     = row.get("EV_Sales_Div_Growth")
                drg      = row.get("Deferred_Rev_Growth")
                runway   = row.get("Cash_Runway_Qtrs")
                rg       = row.get("Rev_Growth")
                gm       = row.get("Gross_Margin")
                moat_lbl = str(row.get("Moat_Label") or "None")
                score    = row.get("Score")
                flags    = str(row.get("Composite_Flag") or "—")

                def _fs(v, suffix="%", dec=1):
                    try:
                        return f"{float(v):.{dec}f}{suffix}" if v is not None and not (isinstance(v, float) and np.isnan(v)) else "N/A"
                    except Exception:
                        return "N/A"

                def _fp(v, dec=1):
                    try:
                        fv = float(v)
                        return f"+{fv:.{dec}f}%" if fv > 0 else f"{fv:.{dec}f}%"
                    except Exception:
                        return "N/A"

                streak_str = f"🔥 {int(streak)}Q streak" if streak and streak >= 2 else (f"1Q" if streak == 1 else "—")
                hg_str     = f"{hg_score:.0f}/100" if hg_score is not None else "N/A"
                rg_str     = _fp(rg)
                gm_exp_str = (_fp(gm_exp) if gm_exp is not None else "N/A")

                with st.expander(
                    f"{hg_label}  **{row['Ticker']}**  —  {str(row.get('Name',''))[:45]}"
                    f"  |  HG Score: {hg_str}  |  Rev Growth: {rg_str}  |  Accel: {streak_str}",
                    expanded=False,
                ):
                    # Top metrics bar
                    tb1, tb2, tb3, tb4, tb5 = st.columns(5)
                    tb1.metric("HG Score",    hg_str)
                    tb2.metric("Rev Growth",  rg_str)
                    tb3.metric("Rule of 40",  _fs(r40, suffix="", dec=0))
                    tb4.metric("Op Leverage", _fs(ol_ratio, suffix="x", dec=2))
                    tb5.metric("Stock Score", f"{score:.0f}" if score else "N/A")

                    st.markdown("---")
                    p1, p2, p3, p4 = st.columns(4)

                    with p1:
                        st.markdown("**📈 Growth Trajectory**")
                        st.metric("Pillar Score", f"{hg_growth:.1f}" if hg_growth is not None else "N/A")
                        st.write(f"Rev Growth:     {rg_str}")
                        st.write(f"Accel Streak:   {streak_str}")
                        st.write(f"GM Expansion:   {gm_exp_str}")
                        st.write(f"Gross Margin:   {_fs(gm)}")

                    with p2:
                        st.markdown("**⚙️ Operating Leverage**")
                        st.metric("Pillar Score", f"{hg_lev:.1f}" if hg_lev is not None else "N/A")
                        st.write(f"Op Leverage:    {_fs(ol_ratio, suffix='x', dec=2)}")
                        st.write(f"Rule of 40:     {_fs(r40, suffix='', dec=0)}")
                        st.write(f"R&D % Rev:      {_fs(rd_pct)}")
                        st.write(f"EV/Sales÷Grwth: {_fs(evsg, suffix='', dec=2)}")

                    with p3:
                        st.markdown("**🎯 PMF & Stickiness**")
                        st.metric("Pillar Score", f"{hg_pmf:.1f}" if hg_pmf is not None else "N/A")
                        st.write(f"Deferred Rev Gr:{_fs(drg)}")
                        st.write(f"EPS Surprise:   {_fp(row.get('EPS_Surprise'))}")
                        st.write(f"Analyst Upside: {_fp(row.get('Analyst_Upside'))}")
                        st.write(f"Cash Runway:    {_fs(runway, suffix='Q', dec=1)}")

                    with p4:
                        st.markdown("**🔍 Discovery Phase**")
                        st.metric("Pillar Score", f"{hg_disc:.1f}" if hg_disc is not None else "N/A")
                        st.write(f"Inst. Own:      {_fs(row.get('Inst_Own'))}")
                        st.write(f"Short Float:    {_fs(row.get('Short_Float'))}")
                        st.write(f"Analyst Count:  {int(row['Analyst_Count']) if row.get('Analyst_Count') and not (isinstance(row.get('Analyst_Count'), float) and np.isnan(row['Analyst_Count'])) else 'N/A'}")
                        moat_icon = {"Wide": "🏰", "Narrow": "〰️", "Weak": "◽", "None": "—"}.get(moat_lbl, "—")
                        st.write(f"Moat:           {moat_icon} {moat_lbl}")

                    if flags and flags != "—":
                        st.markdown(f"**🏷️ Signal Flags:** {flags}")

                    st.caption(
                        f"Sector: {row.get('Sector','')}  ·  "
                        f"Price: ${row['Price']:.2f}  ·  "
                        f"Mkt Cap: {row.get('Mkt_Cap','N/A')}  ·  "
                        f"Traditional Score: {score:.0f}  ←  may look 'expensive' by design"
                    )

        # ── Download ──────────────────────────────────────────────────────────
        st.markdown("---")
        hg_dl_cols = [
            "Ticker", "Name", "Sector", "HG_Score", "HG_Label",
            "HG_Growth", "HG_Leverage", "HG_PMF", "HG_Discovery",
            "Score", "Action", "Rev_Growth", "Rev_Accel_Streak",
            "GM_Expansion_4Q", "Op_Leverage_Ratio", "Rule_Of_40",
            "RD_Pct_Rev", "EV_Sales_Div_Growth", "Deferred_Rev_Growth",
            "Cash_Runway_Qtrs", "Gross_Margin", "EPS_Surprise",
            "Analyst_Upside", "Inst_Own", "Short_Float",
            "Moat_Label", "Composite_Flag", "Price", "Mkt_Cap",
        ]
        existing_cols = [c for c in hg_dl_cols if c in df.columns]
        hg_dl = df[existing_cols].sort_values("HG_Score", ascending=False)
        st.download_button(
            "⬇️ Download Hypergrowth Data CSV",
            data=hg_dl.to_csv(index=False).encode("utf-8"),
            file_name=f"hypergrowth_{datetime.now().strftime('%Y%m%d')}.csv",
            mime="text/csv",
        )

with tab5:
    st.subheader("🧙 Magic Formula — Joel Greenblatt")
    st.caption(
        "From *The Little Book That Still Beats the Market*. "
        "Ranks stocks by **Earnings Yield** (cheap) + **Return on Capital** (quality). "
        "Lowest combined rank = best pick. Financials, Utilities & Real Estate excluded per Greenblatt."
    )

    # ── Compute Magic Formula ranks from the loaded CSV ────────────────────────
    _MF_EXCLUDE = {"Financial Services", "Utilities", "Real Estate"}

    mf = df.copy()

    # Earnings Yield proxy: 1/EV_EBITDA, fallback 1/PE_Fwd
    def _ey(row):
        ev = row.get("EV_EBITDA")
        if ev is not None and not (isinstance(ev, float) and np.isnan(ev)) and float(ev) > 0:
            return round((1.0 / float(ev)) * 100, 2)
        pe = row.get("PE_Fwd")
        if pe is not None and not (isinstance(pe, float) and np.isnan(pe)) and float(pe) > 0:
            return round((1.0 / float(pe)) * 100, 2)
        return None

    # Return on Capital proxy: ROA, fallback ROE
    def _roc(row):
        roa = row.get("ROA")
        if roa is not None and not (isinstance(roa, float) and np.isnan(roa)):
            return float(roa)
        roe = row.get("ROE")
        if roe is not None and not (isinstance(roe, float) and np.isnan(roe)):
            return float(roe)
        return None

    mf["MF_EY"]  = mf.apply(_ey,  axis=1)
    mf["MF_ROC"] = mf.apply(_roc, axis=1)
    mf["MF_Excluded"] = mf["Sector"].apply(lambda s: str(s) in _MF_EXCLUDE)

    excl_count = int(mf["MF_Excluded"].sum())
    eligible   = mf[~mf["MF_Excluded"]].dropna(subset=["MF_EY", "MF_ROC"])
    eligible   = eligible[(eligible["MF_EY"] > 0) & (eligible["MF_ROC"] > 0)].copy()

    if len(eligible) == 0:
        st.warning("Not enough data to compute Magic Formula ranks. Re-run the analysis.")
        st.stop()

    eligible["MF_EY_Rank"]  = eligible["MF_EY"].rank(ascending=False, method="min").astype(int)
    eligible["MF_ROC_Rank"] = eligible["MF_ROC"].rank(ascending=False, method="min").astype(int)
    eligible["MF_Rank"]     = eligible["MF_EY_Rank"] + eligible["MF_ROC_Rank"]
    eligible.sort_values("MF_Rank", ascending=True, inplace=True)
    eligible.reset_index(drop=True, inplace=True)

    # ── Filters ───────────────────────────────────────────────────────────────
    mf_c1, mf_c2, mf_c3 = st.columns([1.5, 1.5, 2])
    with mf_c1:
        mf_sector = st.selectbox(
            "Sector", ["All"] + sorted(eligible["Sector"].dropna().unique().tolist()),
            key="mf_sector"
        )
    with mf_c2:
        mf_action = st.selectbox(
            "Action", ["All", "STRONG BUY", "BUY", "HOLD", "SELL"],
            key="mf_action"
        )
    with mf_c3:
        mf_top_n = st.slider("Show top N stocks", min_value=10, max_value=50, value=20, step=5)

    mf_view = eligible.copy()
    if mf_sector != "All":
        mf_view = mf_view[mf_view["Sector"] == mf_sector]
    if mf_action != "All":
        mf_view = mf_view[mf_view["Action"] == mf_action]
    mf_view = mf_view.head(mf_top_n)

    # ── Summary metrics ───────────────────────────────────────────────────────
    sm1, sm2, sm3, sm4 = st.columns(4)
    sm1.metric("Eligible Stocks",  len(eligible))
    sm2.metric("Excluded",         excl_count, help="Financials, Utilities, Real Estate")
    sm3.metric("Avg Earnings Yield", f"{eligible['MF_EY'].mean():.1f}%")
    sm4.metric("Avg Return on Capital", f"{eligible['MF_ROC'].mean():.1f}%")

    st.markdown("---")

    # ── Top N expander cards ──────────────────────────────────────────────────
    if len(mf_view) == 0:
        st.info("No stocks match the current filters.")
    else:
        st.caption(f"Showing top {len(mf_view)} stocks by Magic Formula combined rank · {excl_count} sectors excluded")

        medals = {1: "🥇", 2: "🥈", 3: "🥉"}

        for rank_pos, (_, row) in enumerate(mf_view.iterrows(), 1):
            ticker     = str(row.get("Ticker", ""))
            name       = str(row.get("Name", ticker))
            sector     = str(row.get("Sector", ""))
            action     = str(row.get("Action", ""))
            ey         = row.get("MF_EY")
            roc        = row.get("MF_ROC")
            ey_rank    = row.get("MF_EY_Rank")
            roc_rank   = row.get("MF_ROC_Rank")
            mf_rank    = row.get("MF_Rank")
            price      = row.get("Price")
            mkt_cap    = row.get("Mkt_Cap", "N/A")
            au         = row.get("Analyst_Upside")
            score      = row.get("Score")
            ev_ebitda  = row.get("EV_EBITDA")
            roa        = row.get("ROA")
            roe        = row.get("ROE")
            moat_label = str(row.get("Moat_Label") or "—")
            flags      = str(row.get("Composite_Flag") or "—")
            rev_g      = row.get("Rev_Growth")
            fcf        = row.get("FCF_Yield")
            de         = row.get("Debt_Equity")

            ey_src  = "EV/EBITDA" if (ev_ebitda and not (isinstance(ev_ebitda, float) and np.isnan(ev_ebitda)) and float(ev_ebitda) > 0) else "Fwd P/E"
            roc_src = "ROA" if (roa and not (isinstance(roa, float) and np.isnan(roa))) else "ROE"
            medal   = medals.get(rank_pos, f"#{rank_pos:02d}")

            action_colors = {"STRONG BUY": "blue", "BUY": "green", "HOLD": "orange", "SELL": "red"}
            action_color  = action_colors.get(action, "gray")

            def _fs(v, suffix="%", dec=1):
                try:
                    return f"{float(v):.{dec}f}{suffix}" if v is not None and not (isinstance(v, float) and np.isnan(v)) else "N/A"
                except Exception:
                    return "N/A"

            def _fpu(v):
                try:
                    fv = float(v)
                    return f"+{fv:.1f}%" if fv >= 0 else f"{fv:.1f}%"
                except Exception:
                    return "N/A"

            with st.expander(
                f"{medal}  **{ticker}**  —  {name[:45]}"
                f"  |  Magic Rank: **#{int(mf_rank)}**"
                f"  |  EY: {_fs(ey)}  ROC: {_fs(roc)}"
                f"  |  {action}",
                expanded=(rank_pos <= 3),
            ):
                col1, col2, col3 = st.columns(3)

                with col1:
                    st.markdown("**🧙 Magic Formula**")
                    st.metric("Combined Rank", f"#{int(mf_rank)}", help="Lower = better. Sum of EY rank + ROC rank.")
                    st.write(f"Earnings Yield:  **{_fs(ey)}** (#{int(ey_rank)}) · via {ey_src}")
                    st.write(f"Return on Cap:   **{_fs(roc)}** (#{int(roc_rank)}) · via {roc_src}")
                    st.write(f"Sector:  {sector}  ·  Cap: {mkt_cap}")
                    st.markdown(f"Action: **:{action_color}[{action}]**  ·  Score: **{_fs(score, suffix='', dec=0)}**")

                with col2:
                    st.markdown("**💰 Price & Analyst**")
                    st.write(f"Price:  **${float(price):.2f}**" if price else "Price: N/A")
                    au_str = _fpu(au)
                    au_color = "green" if (au and float(au) >= 0) else "red"
                    if au:
                        st.markdown(f"Analyst Upside: **:{au_color}[{au_str}]**")
                    st.write(f"EV/EBITDA: {_fs(ev_ebitda, suffix='x', dec=1)}")
                    moat_icons = {"Wide": "🏰", "Narrow": "〰️", "Weak": "◽", "None": "—"}
                    st.write(f"Moat: {moat_icons.get(moat_label,'—')} {moat_label}")

                with col3:
                    st.markdown("**📊 Quality Check**")
                    st.write(f"ROE:        {_fs(roe)}")
                    st.write(f"ROA:        {_fs(roa)}")
                    st.write(f"FCF Yield:  {_fs(fcf)}")
                    st.write(f"Rev Growth: {_fpu(rev_g)}")
                    st.write(f"Debt/Equity:{_fs(de, suffix='', dec=2)}")

                if flags and flags != "—":
                    st.markdown(f"**🏷️ Signals:** {flags}")

                st.caption(
                    f"EY Rank #{int(ey_rank)} + ROC Rank #{int(roc_rank)} = Combined #{int(mf_rank)}  ·  "
                    f"Lower combined rank = Greenblatt's preferred stock"
                )

    # ── Methodology note ──────────────────────────────────────────────────────
    st.markdown("---")
    with st.expander("📖 How the Magic Formula Works", expanded=False):
        st.markdown("""
**Joel Greenblatt's Magic Formula** from *The Little Book That Still Beats the Market* (2006):

1. **Screen** for stocks with market cap > $50M *(we use our curated 344-stock universe)*
2. **Exclude** Utilities, Financials, and Real Estate *(excluded {excl} stocks here)*
3. **Rank by Earnings Yield** = EBIT ÷ Enterprise Value *(proxy: 1 ÷ EV/EBITDA or 1 ÷ Fwd P/E)*
   - Higher EY = you're buying more earnings per dollar invested = cheaper
4. **Rank by Return on Capital** = EBIT ÷ (Net Working Capital + Net Fixed Assets) *(proxy: ROA or ROE)*
   - Higher ROC = company deploys capital more efficiently = better business
5. **Add the two ranks** — lowest combined rank = the Magic Formula's top pick
6. **Buy top 20–30 stocks**, hold for 1 year, then rebalance annually

**Greenblatt's backtest (1988–2004):** ~30.8% annual return vs S&P 500's 12.4%.

⚠️ *EY and ROC here are proxies from yfinance data, not exact Greenblatt calculations.
Past performance does not guarantee future results. Not financial advice.*
        """.format(excl=excl_count))

    # ── Download ──────────────────────────────────────────────────────────────
    mf_dl_cols = [
        c for c in [
            "Ticker", "Name", "Sector", "Action", "Score",
            "MF_EY", "MF_ROC", "MF_EY_Rank", "MF_ROC_Rank", "MF_Rank",
            "Price", "Mkt_Cap", "EV_EBITDA", "PE_Fwd", "ROA", "ROE",
            "FCF_Yield", "Rev_Growth", "Debt_Equity",
            "Moat_Label", "Analyst_Upside", "Composite_Flag",
        ] if c in eligible.columns
    ]
    st.download_button(
        "⬇️ Download Magic Formula CSV",
        data=eligible[mf_dl_cols].to_csv(index=False).encode("utf-8"),
        file_name=f"magic_formula_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv",
    )

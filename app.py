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
    page_title="📊 Portfolio Analyzer v2",
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

        cells = (
            "<td><strong>{}</strong><br><small>{}</small></td>".format(tv, nv)
            + '<td class="tc">' + sb + "</td>"
            + '<td class="tc">' + ab + "</td>"
            + '<td class="tc flag-cell">' + fv + "</td>"
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
            '<tr class="{}" data-action="{}" data-sector="{}" data-ma="{}" data-score="{}" data-flags="{}">'.format(
                rc, rec, sv, mas, score, flags_data) + cells + "</tr>"
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
        + TH("Price", 4) + TH("200 DMA", 5, "ma-col")
        + TH("Analyst Target", 6, "new-col") + TH("Upside %", 7, "new-col")
        + TH("Mkt Cap", 8) + TH("PEG", 9) + TH("P/E Fwd", 10)
        + TH("P/S", 11) + TH("EV/EBITDA", 12, "new-col")
        + TH("ROE %", 13) + TH("Rev Gr %", 14) + TH("Gross Mgn %", 15)
        + TH("FCF Yld %", 16) + TH("EPS Surp %", 17, "new-col")
        + TH("From Low %", 18) + TH("From High %", 19)
        + TH("D/E", 20) + TH("Beta", 21) + TH("Short %", 22)
        + TH("Insider Buy%", 23, "new-col")
        + TH("Div Yield %", 24, "div-col") + TH("Payout %", 25, "div-col")
        + TH("Op Margin %", 26, "div-col") + TH("ROA %", 27, "div-col")
        + TH("Curr Ratio", 28, "div-col")
        + TH("Sector", 29)
    )

    CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body { background: #0d1117; color: #c9d1d9; font-family: "Segoe UI", sans-serif; font-size: 13px; }
h2 { font-size: 1.1rem; color: #58a6ff; margin-bottom: 4px; }
.run-ts { font-size: 11px; color: #8b949e; }
.legend { font-size: 11px; color: #8b949e; margin: 4px 0 8px; }
.legend span { margin-right: 14px; }
.cards { display: flex; flex-wrap: wrap; gap: 8px; margin-bottom: 10px; }
.stat-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px; padding: 8px 14px; min-width: 80px; }
.stat-card .num { font-size: 1.4rem; font-weight: 700; }
.stat-card .lbl { font-size: .65rem; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.toolbar { display: flex; flex-wrap: wrap; align-items: flex-end; gap: 10px; margin-bottom: 8px; }
.fg { display: flex; flex-direction: column; gap: 4px; }
.fl { font-size: .68rem; color: #8b949e; text-transform: uppercase; letter-spacing: .5px; }
.btn-row { display: flex; flex-wrap: wrap; gap: 5px; }
.act-btn { background:#161b22; border:1px solid #30363d; color:#8b949e; border-radius:6px; padding:5px 12px; cursor:pointer; font-size:12px; font-weight:600; transition:all .15s; }
.act-btn:hover { border-color:#58a6ff; color:#c9d1d9; }
#btnAll.active { background:#30363d; color:#c9d1d9; border-color:#8b949e; }
.act-btn[data-action="STRONG BUY"].active { background:#1f6feb; color:#fff; border-color:#1f6feb; }
.act-btn[data-action="BUY"].active        { background:#238636; color:#fff; border-color:#238636; }
.act-btn[data-action="HOLD"].active       { background:#9e6a03; color:#fff; border-color:#9e6a03; }
.act-btn[data-action="SELL"].active       { background:#b62324; color:#fff; border-color:#b62324; }
.sec-btn { background:#161b22; border:1px solid #30363d; color:#8b949e; border-radius:6px; padding:4px 9px; cursor:pointer; font-size:11px; transition:all .15s; }
.sec-btn:hover { border-color:#58a6ff; color:#c9d1d9; }
#btnSecAll.active { background:#30363d; color:#c9d1d9; border-color:#8b949e; }
.sec-btn[data-sector].active { background:#388bfd22; color:#58a6ff; border-color:#388bfd; }
.flag-filter-btn { background:#161b22; border:1px solid #30363d; color:#8b949e; border-radius:6px; padding:4px 9px; cursor:pointer; font-size:11px; transition:all .15s; }
.flag-filter-btn:hover { border-color:#bc8cff; color:#bc8cff; }
.flag-filter-btn.active { background:#bc8cff33; color:#bc8cff; border-color:#bc8cff; }
.ma-filter-btn { background:#161b22; border:1.5px solid #58a6ff; color:#58a6ff; border-radius:6px; padding:5px 13px; cursor:pointer; font-size:12px; font-weight:700; transition:all .15s; }
.ma-filter-btn.active { background:#58a6ff; color:#0d1117; }
input#srch { background:#161b22; border:1px solid #30363d; color:#c9d1d9; border-radius:6px; padding:6px 11px; width:200px; outline:none; font-size:13px; }
input#srch:focus { border-color:#58a6ff; }
input#minScore { background:#161b22; border:1px solid #30363d; color:#c9d1d9; border-radius:6px; padding:5px 8px; width:68px; outline:none; font-size:13px; text-align:center; }
.btn-csv { background:#238636; color:#fff; border:none; border-radius:6px; padding:6px 14px; cursor:pointer; font-size:13px; font-weight:600; }
.btn-csv:hover { background:#2ea043; }
.wrap { overflow-x:auto; max-height:72vh; border:1px solid #21262d; border-radius:8px; margin-top:6px; }
table { border-collapse:collapse; width:100%; white-space:nowrap; }
thead th { background:#161b22; color:#8b949e; position:sticky; top:0; z-index:9; padding:8px 7px; cursor:pointer; user-select:none; font-weight:600; border-bottom:1px solid #30363d; }
thead th:hover { color:#58a6ff; }
thead th.asc::after  { content:" ▲"; font-size:9px; }
thead th.desc::after { content:" ▼"; font-size:9px; }
thead th.new-col { color:#bc8cff; }
thead th.ma-col  { color:#58a6ff; }
thead th.div-col { color:#e6b450; }
td { padding:6px 7px; border-bottom:1px solid #21262d; vertical-align:middle; }
td small { color:#8b949e; font-size:11px; }
.tc { text-align:center; } .tr { text-align:right; }
.row-green  { background:#0d1f0f; } .row-green:hover  { background:#0f2a14 !important; }
.row-orange { background:#1e1600; } .row-orange:hover { background:#2a1f00 !important; }
.row-red    { background:#1c0707; } .row-red:hover    { background:#2a0d0d !important; }
.badge { font-size:11px; padding:3px 7px; border-radius:4px; font-weight:600; }
.score-strong { background:#1f6feb; color:#fff; }
.score-buy    { background:#238636; color:#fff; }
.score-hold   { background:#9e6a03; color:#fff; }
.score-sell   { background:#b62324; color:#fff; }
.div-cell   { color:#e6b450; font-weight:600; }
.flag-cell  { font-size:11px; max-width:200px; white-space:normal; line-height:1.4; }
.analyst-up { color:#3fb950; font-weight:600; }
.analyst-dn { color:#f85149; font-weight:600; }
.accel-up   { color:#3fb950; font-size:11px; }
.accel-dn   { color:#f85149; font-size:11px; }
.ma-above .ma-val { color:#3fb950; font-weight:600; }
.ma-above small   { color:#3fb950; }
.ma-near  .ma-val { color:#d29922; font-weight:600; }
.ma-near  small   { color:#d29922; }
.ma-below .ma-val { color:#f85149; font-weight:600; }
.ma-below small   { color:#f85149; }
#rowcnt { font-size:12px; color:#8b949e; margin-top:5px; }
"""

    JS = """
var sortDir = {};
function toggleAction(b){ b.classList.toggle('active'); var a=document.querySelectorAll('.act-btn[data-action].active').length>0; document.getElementById('btnAll').classList.toggle('active',!a); applyFilters(); }
function toggleAll(){ document.querySelectorAll('.act-btn[data-action]').forEach(function(b){b.classList.remove('active');}); document.getElementById('btnAll').classList.add('active'); applyFilters(); }
function toggleSector(b){ b.classList.toggle('active'); var a=document.querySelectorAll('.sec-btn[data-sector].active').length>0; document.getElementById('btnSecAll').classList.toggle('active',!a); applyFilters(); }
function toggleSecAll(){ document.querySelectorAll('.sec-btn[data-sector]').forEach(function(b){b.classList.remove('active');}); document.getElementById('btnSecAll').classList.add('active'); applyFilters(); }
function toggleMA(b){ b.classList.toggle('active'); applyFilters(); }
function toggleFlag(b){ b.classList.toggle('active'); applyFilters(); }
function applyFilters(){
    var q=document.getElementById('srch').value.toLowerCase().trim();
    var ms=parseFloat(document.getElementById('minScore').value)||0;
    var aa=Array.from(document.querySelectorAll('.act-btn[data-action].active')).map(function(b){return b.getAttribute('data-action');});
    var allA=document.getElementById('btnAll').classList.contains('active')||aa.length===0;
    var sa=Array.from(document.querySelectorAll('.sec-btn[data-sector].active')).map(function(b){return b.getAttribute('data-sector');});
    var allS=document.getElementById('btnSecAll').classList.contains('active')||sa.length===0;
    var maB=document.getElementById('btnMABelow').classList.contains('active');
    var fa=Array.from(document.querySelectorAll('.flag-filter-btn.active')).map(function(b){return b.getAttribute('data-flag');});
    document.querySelectorAll('#tbody tr').forEach(function(r){
        var ok=((!q||r.innerText.toLowerCase().indexOf(q)!==-1))
            &&(allA||aa.indexOf(r.getAttribute('data-action')||'')!==-1)
            &&(allS||sa.indexOf(r.getAttribute('data-sector')||'')!==-1)
            &&(!maB||(r.getAttribute('data-ma')||'')==='below')
            &&(parseFloat(r.getAttribute('data-score')||0)>=ms)
            &&(fa.length===0||fa.some(function(f){return (r.getAttribute('data-flags')||'').indexOf(f)!==-1;}));
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
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>{css}</style></head>
<body><div style="padding:10px 14px">

<h2>📊 Portfolio Analyzer v2 &nbsp;<span class="run-ts">Last run: {run_ts}</span></h2>
<div class="legend">
  <span>🟣 Purple = new v2 columns</span>
  <span>📈 Analyst targets = Wall St consensus</span>
  <span>⚡ Rev Gr % shows acceleration arrow</span>
</div>

<div class="cards">
  <div class="stat-card"><div class="num">{total}</div><div class="lbl">Stocks</div></div>
  <div class="stat-card" style="border-color:#1f6feb"><div class="num" style="color:#1f6feb">{sb}</div><div class="lbl">Strong Buy</div></div>
  <div class="stat-card" style="border-color:#238636"><div class="num" style="color:#238636">{b}</div><div class="lbl">Buy</div></div>
  <div class="stat-card" style="border-color:#9e6a03"><div class="num" style="color:#9e6a03">{h}</div><div class="lbl">Hold</div></div>
  <div class="stat-card" style="border-color:#b62324"><div class="num" style="color:#b62324">{s}</div><div class="lbl">Sell</div></div>
  <div class="stat-card"><div class="num">{avg}</div><div class="lbl">Avg Score</div></div>
</div>

<div class="toolbar">
  <div class="fg"><div class="fl">🔍 Search</div>
    <input id="srch" type="text" placeholder="Ticker or name…" oninput="applyFilters()">
  </div>
  <div class="fg"><div class="fl">Min Score</div>
    <input id="minScore" type="number" value="0" min="0" max="100" oninput="applyFilters()">
  </div>
  <div class="fg"><div class="fl">Action</div><div class="btn-row">
    <button id="btnAll" class="act-btn active" onclick="toggleAll()">ALL</button>
    <button class="act-btn" data-action="STRONG BUY" onclick="toggleAction(this)">🟢 STRONG BUY</button>
    <button class="act-btn" data-action="BUY"        onclick="toggleAction(this)">🔵 BUY</button>
    <button class="act-btn" data-action="HOLD"       onclick="toggleAction(this)">🟡 HOLD</button>
    <button class="act-btn" data-action="SELL"       onclick="toggleAction(this)">🔴 SELL</button>
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
  </div></div>
  <div class="fg"><div class="fl">Sector</div><div class="btn-row">
    <button id="btnSecAll" class="sec-btn active" onclick="toggleSecAll()">ALL</button>
    {sec_btns}
  </div></div>
  <button class="btn-csv" onclick="exportCSV()">⬇ Export CSV</button>
</div>

<div id="rowcnt"></div>
<div class="wrap">
  <table id="tbl">
    <thead><tr>{headers}</tr></thead>
    <tbody id="tbody">{rows_html}</tbody>
  </table>
</div>
</div>
<script>{js}</script>
</body></html>""".format(
        css=CSS, js=JS, run_ts=run_ts,
        total=total, sb=sb_c, b=b_c, h=h_c, s=s_c, avg=avg,
        sec_btns=sec_btns, headers=headers, rows_html=rows_html,
    )


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════
df, run_info, top10 = load_data()

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown(
    '<h1 style="color:#58a6ff;margin-bottom:2px">📊 Stock Portfolio Analyzer v2</h1>'
    '<p style="color:#8b949e;font-size:12px;margin-top:0">344 stocks · Sector-relative scoring · Updated daily at 10am by cron job</p>',
    unsafe_allow_html=True,
)

if df is None:
    st.warning("⏳ No results yet — `data/portfolio_analysis.csv` not found.")
    st.info("Run `python run_analysis.py` manually once to generate the first results.")
    st.stop()

run_ts  = run_info.get("run_timestamp_utc", "Unknown")
elapsed = run_info.get("elapsed_minutes", "?")
st.caption(f"🕐 Last analysis: **{run_ts}** · Completed in {elapsed} min")

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
tab1, tab2 = st.tabs(["📋 Full Screener", "⭐ Top 10 Picks"])

with tab1:
    # Build and embed the full interactive HTML report
    html_out = build_html_report(df, run_ts)
    components.html(html_out, height=920, scrolling=False)

with tab2:
    if not top10:
        st.info("No top picks available.")
    else:
        st.subheader(f"🏆 Top {len(top10)} Recommendations")
        st.caption("Multi-factor conviction scoring · Sector-diversified · Risk-filtered")

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


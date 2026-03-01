# 📊 Stock Portfolio Analyzer v2

A web-based stock screener for 344 stocks with weighted, sector-relative scoring and analyst consensus targets.

Built with Python + Streamlit. Runs entirely in the browser — no local installation needed.

---

## 🚀 Live App

> 🔗 **Your Streamlit Cloud URL will appear here after deployment**

---

## ✨ Features

- **344 stocks** screened in one click
- **Weighted scoring engine** — FCF yield, ROE, growth, valuation, technicals
- **Sector-relative scoring** — P/E vs sector median, not absolute
- **Real analyst consensus targets** from Yahoo Finance (not fabricated)
- **Composite signal flags** — Compounder, Accel Growth, Deep Value, Analyst Conviction, etc.
- **Top N picks** with sector diversification and risk filters
- **Export to CSV** directly from the browser
- **Works on mobile** 📱

---

## 🗂️ File Structure

```
stock-analyzer/
├── app.py                    ← Streamlit web app (main entry point)
├── portfolio_analyzer_v2.py  ← Core logic: fetching, scoring, recommendations
├── requirements.txt          ← Python dependencies
├── .gitignore
└── README.md
```

---

## 🔧 Deploy to Streamlit Cloud (Free)

### Step 1 — Push to GitHub

1. Create a new **public** repository on [github.com](https://github.com) (e.g. `stock-analyzer`)
2. Upload both Python files + `requirements.txt` + `README.md`

   **Option A — GitHub web UI (easiest):**
   - Go to your new repo → click **Add file → Upload files**
   - Drag and drop all files → click **Commit changes**

   **Option B — Git command line:**
   ```bash
   git init
   git add .
   git commit -m "Initial commit"
   git remote add origin https://github.com/YOUR_USERNAME/stock-analyzer.git
   git push -u origin main
   ```

### Step 2 — Deploy on Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io) → sign in with GitHub
2. Click **New app**
3. Choose your repo → branch: `main` → main file: `app.py`
4. Click **Deploy** → wait ~2 minutes
5. 🎉 You get a public URL like `https://YOUR_USERNAME-stock-analyzer-app-xxxx.streamlit.app`

> Share this URL with anyone — it works on mobile browsers too.

---

## 🔄 Updating / Re-running

- **Re-run analysis anytime**: Open the URL → click **Run Analysis Now** in the sidebar
- **Update the code**: Edit files on GitHub → Streamlit Cloud auto-redeploys in ~1 minute
- **Add/remove tickers**: Edit `PORTFOLIO_TICKERS` list in `portfolio_analyzer_v2.py`

---

## ⏱️ Performance

| Stocks | Time (8 workers) |
|--------|-----------------|
| 344    | ~4–7 minutes    |

Yahoo Finance rate limits mean it can't go much faster. The app shows a live progress bar while running.

---

## ⚠️ Disclaimer

This tool is for **research and screening purposes only**. It is not financial advice. Always conduct your own due diligence before making any investment decisions.

Data sourced from Yahoo Finance via `yfinance`. Accuracy depends on Yahoo Finance data availability.

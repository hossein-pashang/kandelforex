import yfinance as yf
import pandas as pd
import numpy as np
import schedule
import time
import requests
import os
from datetime import datetime

# =============================
# ENV
# =============================

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SAVE_FILE = "institutional_market_data.csv"

# =============================
# FULL MARKET SET
# =============================

symbols = [
    # Majors
    "EURUSD=X","GBPUSD=X","USDJPY=X","USDCHF=X",
    "AUDUSD=X","USDCAD=X","NZDUSD=X",

    # Minors / Crosses
    "EURGBP=X","EURJPY=X","EURCHF=X","EURAUD=X","EURCAD=X",
    "GBPJPY=X","GBPAUD=X","GBPCAD=X","GBPCHF=X",
    "AUDJPY=X","AUDCAD=X","AUDCHF=X",
    "CADJPY=X","CADCHF=X","CHFJPY=X",
    "NZDJPY=X","NZDCAD=X","NZDCHF=X",

    # Metals / Energy
    "GC=F","SI=F","CL=F",

    # Indices
    "^GSPC","^IXIC","^DJI",

    # Dollar Index
    "DX-Y.NYB",

    # Volatility
    "^VIX",

    # Bond Yield
    "^TNX",

    # Crypto
    "BTC-USD"
]

intervals = {
    "1h": "7d",
    "4h": "30d",
    "1d": "90d"
}

# =============================
# FUNCTIONS
# =============================

def compute_returns(df):
    return df['Close'].pct_change().iloc[-1]

def compute_volatility(df):
    return df['Close'].pct_change().rolling(20).std().iloc[-1]

def compute_atr(df, period=14):
    tr = df['High'] - df['Low']
    return tr.rolling(period).mean().iloc[-1]

def market_structure(df):
    highs = df['High'].tail(5)
    lows = df['Low'].tail(5)
    if highs.is_monotonic_increasing:
        return "Bullish Expansion"
    if lows.is_monotonic_decreasing:
        return "Bearish Expansion"
    return "Compression"

def liquidity_grab(df):
    if len(df) < 20:
        return False
    return df['High'].iloc[-1] > df['High'].iloc[-20:-1].max()

def fetch_data():
    print("Fetching institutional market data...")
    results = []

    # ===== Macro Regime =====
    macro_data = yf.download(["^VIX","DX-Y.NYB","^TNX","^GSPC"],
                             period="30d", interval="1d", progress=False)

    try:
        vix = macro_data['Close']['^VIX'].iloc[-1]
        dxy_series = macro_data['Close']['DX-Y.NYB']
        yield10 = macro_data['Close']['^TNX'].iloc[-1]
        spx_series = macro_data['Close']['^GSPC']

        score = 0
        if vix > 25: score -= 2
        if yield10 > 4: score -= 1
        if dxy_series.iloc[-1] > dxy_series.mean(): score -= 1
        if spx_series.iloc[-1] > spx_series.mean(): score += 2

        if score >= 2:
            regime = "Strong Risk On"
        elif score == 1:
            regime = "Risk On"
        elif score == 0:
            regime = "Neutral"
        elif score == -1:
            regime = "Risk Off"
        else:
            regime = "Crisis Mode"
    except:
        regime = "Unknown"

    # ===== Main Loop =====
    for symbol in symbols:
        for tf, period in intervals.items():

            df = yf.download(symbol, period=period, interval=tf, progress=False)
            if df.empty:
                continue

            df.dropna(inplace=True)

            ret = compute_returns(df)
            vol = compute_volatility(df)
            atr = compute_atr(df)
            structure = market_structure(df)
            sweep = liquidity_grab(df)

            try:
                dxy_tf = yf.download("DX-Y.NYB",
                                     period=period,
                                     interval=tf,
                                     progress=False)
                corr = df['Close'].pct_change().corr(
                    dxy_tf['Close'].pct_change()
                )
            except:
                corr = np.nan

            last = df.iloc[-1]

            results.append({
                "Symbol": symbol,
                "Timeframe": tf,
                "Open": last["Open"],
                "High": last["High"],
                "Low": last["Low"],
                "Close": last["Close"],

                "Return": ret,
                "Volatility": vol,
                "ATR": atr,
                "Market Structure": structure,
                "Liquidity Sweep": sweep,
                "Correlation vs DXY": corr,
                "Macro Regime": regime,
                "Timestamp": datetime.now()
            })

    if results:
        final_df = pd.DataFrame(results)
        final_df.to_csv(SAVE_FILE, index=False)
        send_to_telegram()

def send_to_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(SAVE_FILE, "rb") as f:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
    print("File sent to Telegram")

# ===== First Run =====
fetch_data()

# ===== Every 10 Minutes =====
schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

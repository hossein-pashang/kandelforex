import yfinance as yf
import pandas as pd
import schedule
import time
import requests
import os
from datetime import datetime, timedelta

# ====== ENV VARIABLES ======
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")
SAVE_FILE = "market_data.csv"

# ====== Symbols & Intervals ======
symbols = [
    # Major Forex
    "EURUSD=X", "GBPUSD=X", "USDJPY=X", "USDCHF=X",
    "AUDUSD=X", "NZDUSD=X", "USDCAD=X", "EURGBP=X", "EURJPY=X", "GBPJPY=X",
    # Minor Forex
    "EURAUD=X", "EURNZD=X", "GBPCHF=X", "AUDJPY=X", "AUDNZD=X", "NZDJPY=X",
    # Commodities
    "GC=F", "SI=F", "CL=F",
    # Indexes
    "^DXY", "^GSPC", "^VIX",
    # Crypto
    "BTC-USD"
]

intervals = ["5m", "15m", "60m", "4h", "1d"]

# ====== Market Structure Helper ======
def market_structure(df):
    result = {}
    if df.empty or 'High' not in df.columns or 'Low' not in df.columns:
        result['Market Structure State'] = 'N/A'
        result['Liquidity Sweep Flag'] = 'N/A'
        result['Expansion / Compression'] = 'N/A'
        return result

    highs = df['High']
    lows = df['Low']

    # Market Structure State
    try:
        if highs.is_monotonic_increasing and lows.is_monotonic_increasing:
            result['Market Structure State'] = 'Uptrend'
        elif highs.is_monotonic_decreasing and lows.is_monotonic_decreasing:
            result['Market Structure State'] = 'Downtrend'
        else:
            result['Market Structure State'] = 'Sideways'
    except:
        result['Market Structure State'] = 'N/A'

    # Simple Liquidity Sweep Example
    try:
        result['Liquidity Sweep Flag'] = 'Yes' if highs.iloc[-1] > highs.max() else 'No'
    except:
        result['Liquidity Sweep Flag'] = 'N/A'

    # Expansion / Compression (Volatility)
    try:
        result['Expansion / Compression'] = 'Expansion' if highs.std() > lows.std() else 'Compression'
    except:
        result['Expansion / Compression'] = 'N/A'

    return result

# ====== Fetch & Process Data ======
def fetch_data():
    print(f"Fetching market data... {datetime.now()}")
    all_data = []

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    for symbol in symbols:
        for interval in intervals:
            try:
                df = yf.download(symbol, start=start_date, interval=interval, progress=False)
                if df.empty:
                    continue
                df.reset_index(inplace=True)
                df['Symbol'] = symbol
                df['Interval'] = interval

                # Market structure info
                ms_info = market_structure(df)
                for k, v in ms_info.items():
                    df[k] = v

                all_data.append(df)
            except Exception as e:
                print(f"Error fetching {symbol} {interval}: {e}")
                continue

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        send_to_telegram()
    else:
        print("No data fetched this round.")

# ====== Send CSV to Telegram ======
def send_to_telegram():
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
        with open(SAVE_FILE, "rb") as f:
            requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
        print("File sent to Telegram")
    except Exception as e:
        print(f"Error sending file: {e}")

# ====== Scheduler ======
fetch_data()  # Run once immediately
schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

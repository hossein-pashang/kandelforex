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

symbols = [
    "EURUSD=X","GBPUSD=X","USDJPY=X","USDCHF=X",
    "AUDUSD=X","NZDUSD=X","USDCAD=X",
    "EURGBP=X","EURJPY=X","GBPJPY=X",
    "GC=F","SI=F","CL=F",
    "^DXY","^GSPC","^VIX",
    "BTC-USD"
]

intervals = ["5m","15m","60m","4h","1d"]

def fetch_data():
    print("Fetching market data...")
    all_data = []

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")

    for symbol in symbols:
        for interval in intervals:
            try:
                df = yf.download(symbol, start=start_date, interval=interval, progress=False)
                if df.empty:
                    continue
                df.reset_index(inplace=True)
                df["Symbol"] = symbol
                df["Interval"] = interval
                all_data.append(df)
            except:
                pass

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        send_to_telegram()

def send_to_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(SAVE_FILE, "rb") as f:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
    print("File sent to Telegram")

fetch_data()

schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()

    time.sleep(5)


import requests
import pandas as pd
import numpy as np
import schedule
import time
import os
from datetime import datetime, timezone

# ================= ENV =================
OANDA_API_KEY = os.getenv("OANDA_API_KEY")
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SAVE_FILE = "market_data.csv"

OANDA_BASE = "https://api-fxpractice.oanda.com/v3"
FINNHUB_BASE = "https://finnhub.io/api/v1"

oanda_headers = {
    "Authorization": f"Bearer {OANDA_API_KEY}"
}

# ================= SYMBOLS =================
forex_symbols = [
    "EUR_USD","GBP_USD","USD_JPY","USD_CHF",
    "AUD_USD","NZD_USD","USD_CAD"
]

symbols = forex_symbols

granularity_map = {
    "5m": "M5",
    "15m": "M15",
    "1h": "H1"
}

# ================= OANDA CANDLES =================

def get_candles(instrument, granularity):
    url = f"{OANDA_BASE}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "count": 300,
        "price": "M"
    }

    r = requests.get(url, headers=oanda_headers, params=params)

    if r.status_code != 200:
        print(f"Candle Error {instrument} {granularity}: {r.status_code}")
        return pd.DataFrame()

    data = r.json()
    rows = []

    for c in data.get("candles", []):
        if c.get("complete"):
            rows.append({
                "Time": c["time"],
                "Open": float(c["mid"]["o"]),
                "High": float(c["mid"]["h"]),
                "Low": float(c["mid"]["l"]),
                "Close": float(c["mid"]["c"]),
                "Volume": c["volume"],
                "Symbol": instrument,
                "Timeframe": granularity
            })

    return pd.DataFrame(rows)

# ================= FINNHUB LIVE PRICE =================

def get_live_price(symbol):
    # تبدیل فرمت به OANDA:EUR_USD
    finnhub_symbol = f"OANDA:{symbol}"
    url = f"{FINNHUB_BASE}/quote"
    params = {
        "symbol": finnhub_symbol,
        "token": FINNHUB_API_KEY
    }

    r = requests.get(url, params=params)

    if r.status_code != 200:
        print("Finnhub Error:", r.status_code)
        return None

    data = r.json()

    return {
        "Live_Mid": data.get("c"),   # current price
        "Live_High": data.get("h"),
        "Live_Low": data.get("l"),
        "Live_Open": data.get("o")
    }

# ================= FETCH =================

def fetch_data():
    print("Fetching Data...", datetime.now(timezone.utc))

    all_data = []

    for symbol in symbols:
        for label, gran in granularity_map.items():

            df = get_candles(symbol, gran)

            if df.empty:
                continue

            # گرفتن قیمت زنده
            live = get_live_price(symbol)

            if live:
                last_index = df.index[-1]
                for key, value in live.items():
                    df.loc[last_index, key] = value

            all_data.append(df)
            time.sleep(0.2)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        send_to_telegram()
        print("Sent successfully.")
    else:
        print("No data fetched.")

def send_to_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(SAVE_FILE, "rb") as f:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})

# ================= RUN =================

fetch_data()
schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

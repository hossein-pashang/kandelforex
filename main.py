import requests
import pandas as pd
import schedule
import time
import os
from datetime import datetime

# ===== ENV =====
OANDA_API_KEY = os.getenv("OANDA_API_KEY")
ACCOUNT_ID = os.getenv("OANDA_ACCOUNT_ID")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SAVE_FILE = "market_data.csv"

# ===== OANDA CONFIG =====
BASE_URL = "https://api-fxpractice.oanda.com/v3"

headers = {
    "Authorization": f"Bearer {OANDA_API_KEY}"
}

symbols = [
    "EUR_USD","GBP_USD","USD_JPY","USD_CHF",
    "AUD_USD","NZD_USD","USD_CAD",
    "EUR_GBP","EUR_JPY","GBP_JPY",
    "EURAUD","EURNZD","GBP_CHF","AUD_JPY","AUD_NZD","NZD_JPY"
]

granularity_map = {
    "5m": "M5",
    "15m": "M15",
    "1h": "H1",
    "4h": "H4",
    "1d": "D"
}

def get_candles(instrument, granularity):
    url = f"{BASE_URL}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "count": 200,
        "price": "M"
    }
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        return pd.DataFrame()
    data = r.json()
    candles = data.get("candles", [])
    rows = []
    for c in candles:
        if c["complete"]:
            rows.append({
                "Time": c["time"],
                "Open": float(c["mid"]["o"]),
                "High": float(c["mid"]["h"]),
                "Low": float(c["mid"]["l"]),
                "Close": float(c["mid"]["c"]),
                "Volume": c["volume"]
            })
    return pd.DataFrame(rows)

def market_structure(df):
    if df.empty:
        return {"Market Structure": "N/A"}

    highs = df["High"]
    lows = df["Low"]

    if highs.is_monotonic_increasing and lows.is_monotonic_increasing:
        state = "Uptrend"
    elif highs.is_monotonic_decreasing and lows.is_monotonic_decreasing:
        state = "Downtrend"
    else:
        state = "Sideways"

    return {"Market Structure": state}

def fetch_data():
    print("Fetching OANDA data...", datetime.now())
    all_data = []

    for symbol in symbols:
        for label, gran in granularity_map.items():
            try:
                df = get_candles(symbol, gran)
                if df.empty:
                    continue

                df["Symbol"] = symbol
                df["Timeframe"] = label

                ms = market_structure(df)
                df["Market Structure"] = ms["Market Structure"]

                all_data.append(df)

            except Exception as e:
                print("Error:", e)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        send_to_telegram()
    else:
        print("No data fetched.")

def send_to_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(SAVE_FILE, "rb") as f:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
    print("Sent to Telegram")

fetch_data()
schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

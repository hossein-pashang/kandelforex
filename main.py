import requests
import pandas as pd
import numpy as np
import schedule
import time
import os
from datetime import datetime, timezone

# ================= ENV =================
OANDA_API_KEY = os.getenv("OANDA_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SAVE_FILE = "market_data.csv"
BASE_URL = "https://api-fxpractice.oanda.com/v3"

headers = {
    "Authorization": f"Bearer {OANDA_API_KEY}"
}

# ================= ENV TEST =================
print("===== OANDA ENV TEST =====")

headers_test = {
    "Authorization": f"Bearer {OANDA_API_KEY}"
}

r1 = requests.get("https://api-fxpractice.oanda.com/v3/accounts", headers=headers_test)
print("Practice status:", r1.status_code)

r2 = requests.get("https://api-fxtrade.oanda.com/v3/accounts", headers=headers_test)
print("Live status:", r2.status_code)

print("Using BASE_URL:", BASE_URL)
print("===== END TEST =====")


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

# ================= DATA =================

def get_candles(instrument, granularity):
    url = f"{BASE_URL}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "count": 100,
        "price": "M"
    }

    r = requests.get(url, headers=headers, params=params)

    if r.status_code != 200:
        print(f"Error {instrument} {granularity}: {r.status_code}")
        return pd.DataFrame()

    data = r.json()
    candles = data.get("candles", [])
    rows = []

    for c in candles:
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


def fetch_data():
    print("Fetching data...", datetime.now(timezone.utc))
    all_data = []

    for symbol in symbols:
        for label, gran in granularity_map.items():
            df = get_candles(symbol, gran)
            if not df.empty:
                print(symbol, gran, "Last candle:", df.iloc[-1]["Time"])
                all_data.append(df)
                time.sleep(0.2)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        print("File saved.")


fetch_data()

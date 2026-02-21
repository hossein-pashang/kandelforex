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

# ================= SYMBOLS =================
forex_symbols = [
    "EUR_USD","GBP_USD","USD_JPY","USD_CHF",
    "AUD_USD","NZD_USD","USD_CAD",
    "EUR_GBP","EUR_JPY","GBP_JPY",
    "EUR_AUD","EUR_NZD","GBP_CHF",
    "AUD_JPY","AUD_NZD","NZD_JPY"
]

commodities = [
    "XAU_USD",
    "XAG_USD",
    "BCO_USD"
]

indices = [
    "SPX500_USD",
    "NAS100_USD",
    "US30_USD",
    "DE30_EUR",
    "UK100_GBP"
]

symbols = forex_symbols + commodities + indices

granularity_map = {
    "5m": "M5",
    "15m": "M15",
    "1h": "H1",
    "4h": "H4",
    "1d": "D"
}

# ================= DATA =================

def get_candles(instrument, granularity):
    url = f"{BASE_URL}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "count": 800,   # عدد امن برای جلوگیری از rate limit
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


# ================= INSTITUTIONAL ENGINE =================

def calculate_atr(df, period=14):
    high_low = df["High"] - df["Low"]
    high_close = np.abs(df["High"] - df["Close"].shift())
    low_close = np.abs(df["Low"] - df["Close"].shift())
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    return tr.rolling(period).mean()


def detect_market_structure(df):
    if len(df) < 20:
        return "N/A", "N/A"

    recent_high = df["High"].iloc[-1]
    prev_high = df["High"].iloc[-20:-1].max()
    recent_low = df["Low"].iloc[-1]
    prev_low = df["Low"].iloc[-20:-1].min()

    if recent_high > prev_high:
        return "Bullish BOS", "Uptrend"
    elif recent_low < prev_low:
        return "Bearish BOS", "Downtrend"
    else:
        return "No BOS", "Range"


def detect_liquidity(df):
    highs = df["High"].round(4)
    lows = df["Low"].round(4)

    if highs.duplicated().any():
        return "Equal High Liquidity"
    if lows.duplicated().any():
        return "Equal Low Liquidity"
    return "No Clear Liquidity"


def volatility_regime(df):
    df["ATR"] = calculate_atr(df)
    if df["ATR"].iloc[-1] > df["ATR"].mean():
        return "High Volatility"
    return "Low Volatility"


def session_label():
    now = datetime.now(timezone.utc)
    hour = now.hour

    if 0 <= hour < 7:
        return "Asia"
    elif 7 <= hour < 13:
        return "London"
    elif 13 <= hour < 22:
        return "New York"
    return "After Hours"


# ================= FETCH =================

def fetch_data():
    now = datetime.now(timezone.utc)
    print("Fetching Institutional Data...", now)

    all_data = []

    for symbol in symbols:
        for label, gran in granularity_map.items():
            try:
                df = get_candles(symbol, gran)

                if df.empty:
                    continue

                bos, trend = detect_market_structure(df)

                df["Structure"] = bos
                df["Trend"] = trend
                df["Liquidity"] = detect_liquidity(df)
                df["Volatility_Regime"] = volatility_regime(df)
                df["Session"] = session_label()

                all_data.append(df)

                time.sleep(0.15)  # جلوگیری از rate limit

            except Exception as e:
                print(f"Error {symbol}: {e}")
                continue

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df.to_csv(SAVE_FILE, index=False)
        print("File size (KB):", round(os.path.getsize(SAVE_FILE)/1024,2))
        send_to_telegram()
    else:
        print("No data fetched.")


def send_to_telegram():
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendDocument"
    with open(SAVE_FILE, "rb") as f:
        requests.post(url, data={"chat_id": CHAT_ID}, files={"document": f})
    print("Sent to Telegram")


# ================= RUN =================

fetch_data()
schedule.every(15).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

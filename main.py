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
ACCOUNT_ID = os.getenv("ACCOUNT_ID")

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
    "1d": "D"
}

# ================= DATA =================

def get_candles(instrument, granularity, count=500):
    url = f"{BASE_URL}/instruments/{instrument}/candles"
    params = {
        "granularity": granularity,
        "count": count,
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

def get_live_prices(instruments):
    url = f"{BASE_URL}/accounts/{ACCOUNT_ID}/pricing"
    params = {"instruments": ",".join(instruments)}
    r = requests.get(url, headers=headers, params=params)
    if r.status_code != 200:
        print("Pricing Error:", r.status_code)
        return {}
    data = r.json()
    prices = {}
    for p in data.get("prices", []):
        symbol = p["instrument"]
        bid = float(p["bids"][0]["price"])
        ask = float(p["asks"][0]["price"])
        mid = (bid + ask) / 2
        prices[symbol] = {"Bid": bid, "Ask": ask, "Mid": mid}
    return prices

# ================= ENGINE =================

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
    atr = calculate_atr(df)
    if len(atr) == 0:
        return "N/A"
    if atr.iloc[-1] > atr.mean():
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

def calculate_usd_strength(df_dict):
    usd_pairs = [s for s in df_dict if "USD" in s]
    strength = {}
    for tf in ["M5","M15","H1"]:
        try:
            vals = []
            for sym in usd_pairs:
                df = df_dict[sym]
                last = df[df["Timeframe"]==tf].iloc[-1]
                mid = (last["High"]+last["Low"])/2
                if sym.startswith("USD_"):
                    vals.append(mid)
                elif sym.endswith("_USD"):
                    vals.append(-mid)
            strength[tf] = sum(vals)
        except Exception:
            strength[tf] = np.nan
    return strength

# ================= FETCH =================

def fetch_data():
    now = datetime.now(timezone.utc)
    print("Fetching Data...", now)
    all_data = []
    df_dict = {}
    for symbol in symbols:
        for label, gran in granularity_map.items():
            df = get_candles(symbol, gran)
            if df.empty:
                continue
            bos, trend = detect_market_structure(df)
            # اضافه کردن ستون‌ها به طول DataFrame
            df["Structure"] = [bos]*len(df)
            df["Trend"] = [trend]*len(df)
            df["Liquidity"] = [detect_liquidity(df)]*len(df)
            df["Volatility_Regime"] = [volatility_regime(df)]*len(df)
            df["Session"] = [session_label()]*len(df)
            all_data.append(df)
            df_dict[symbol] = df
            time.sleep(0.15)
    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # قیمت زنده
        live_prices = get_live_prices(symbols)
        for symbol in live_prices:
            mask = final_df["Symbol"]==symbol
            if mask.any():
                last_idx = final_df[mask].index[-1]
                final_df.loc[last_idx, "Live_Bid"] = live_prices[symbol]["Bid"]
                final_df.loc[last_idx, "Live_Ask"] = live_prices[symbol]["Ask"]
                final_df.loc[last_idx, "Live_Mid"] = live_prices[symbol]["Mid"]
        # شاخص دلار مصنوعی
        usd_strength = calculate_usd_strength(df_dict)
        for tf in usd_strength:
            final_df[f"USD_Strength_{tf}"] = usd_strength[tf]
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
schedule.every(10).minutes.do(fetch_data)

while True:
    schedule.run_pending()
    time.sleep(5)

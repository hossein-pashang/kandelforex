import requests
import pandas as pd
import numpy as np
import schedule
import time
import os
from datetime import datetime, timedelta, timezone

# ================= ENV =================
FINNHUB_API_KEY = os.getenv("FINNHUB_API_KEY")
BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID")

SAVE_FILE = "market_data.csv"

# ================= SYMBOLS =================
forex_symbols = [
    "EURUSD","GBPUSD","USDJPY","USDCHF",
    "AUDUSD","NZDUSD","USDCAD",
    "EURGBP","EURJPY","GBPJPY",
    "EURAUD","EURNZD","GBPCHF",
    "AUDJPY","AUDNZD","NZDJPY"
]

commodities = [
    "XAUUSD",  # Gold
    "XAGUSD",  # Silver
    "BCOUSD"   # Brent Oil
]

indices = [
    "SPX",     # S&P500
    "NAS100",  # Nasdaq
    "DJI",     # Dow Jones
    "DAX",     # Germany DAX
    "FTSE"     # UK FTSE
]

dollar_index = ["DXY"]

symbols = forex_symbols + commodities + indices + dollar_index

granularity_map = {
    "5m": 5,
    "15m": 15,
    "1h": 60,
    "4h": 240,
    "1d": 1440
}

# ================= DATA =================

def get_finnhub_candles(symbol, minutes):
    """دیتای کندل Finnhub برای یک هفته اخیر"""
    now = int(datetime.now(timezone.utc).timestamp())
    week_ago = int((datetime.now(timezone.utc) - timedelta(days=7)).timestamp())

    resolution = str(minutes) if minutes < 60 else str(minutes // 60 * 60)
    url = f"https://finnhub.io/api/v1/forex/candle" if symbol in forex_symbols else f"https://finnhub.io/api/v1/forex/candle"
    
    # Finnhub endpoint متفاوت برای شاخص دلار و شاخص ها
    if symbol in indices + dollar_index + commodities:
        url = f"https://finnhub.io/api/v1/forex/candle"

    params = {
        "symbol": symbol,
        "resolution": str(minutes),
        "from": week_ago,
        "to": now,
        "token": FINNHUB_API_KEY
    }

    r = requests.get(url, params=params)
    if r.status_code != 200:
        print(f"Finnhub Error {symbol}:", r.status_code)
        return pd.DataFrame()

    data = r.json()
    if data.get("s") != "ok":
        return pd.DataFrame()

    df = pd.DataFrame({
        "Time": pd.to_datetime(data["t"], unit="s"),
        "Open": data["o"],
        "High": data["h"],
        "Low": data["l"],
        "Close": data["c"],
        "Volume": data.get("v", [0]*len(data["t"])),
        "Symbol": symbol
    })
    return df

def get_live_prices(symbols):
    """قیمت زنده Mid/Bid/Ask"""
    prices = {}
    for symbol in symbols:
        url = f"https://finnhub.io/api/v1/quote?symbol={symbol}&token={FINNHUB_API_KEY}"
        r = requests.get(url)
        if r.status_code != 200:
            print(f"Finnhub Error: {r.status_code} {symbol}")
            continue
        data = r.json()
        prices[symbol] = {
            "Live_Bid": data.get("pc", None),  # previous close as approximation
            "Live_Ask": data.get("c", None),
            "Live_Mid": data.get("c", None)
        }
    return prices

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
    atr = calculate_atr(df)
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

# ================= FETCH =================

def fetch_data():
    now = datetime.now(timezone.utc)
    print("Fetching Data...", now)

    all_data = []

    for symbol in symbols:
        for label, minutes in granularity_map.items():
            df = get_finnhub_candles(symbol, minutes)
            if df.empty:
                continue
            bos, trend = detect_market_structure(df)
            df["Structure"] = bos
            df["Trend"] = trend
            df["Liquidity"] = detect_liquidity(df)
            df["Volatility_Regime"] = volatility_regime(df)
            df["Session"] = session_label()
            df["Timeframe"] = label
            all_data.append(df)
            time.sleep(0.1)

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # ===== GET LIVE PRICES =====
        live_prices = get_live_prices(symbols)
        for symbol in live_prices:
            mask = final_df["Symbol"] == symbol
            if mask.any():
                last_index = final_df[mask].index[-1]
                final_df.loc[last_index, "Live_Bid"] = live_prices[symbol]["Live_Bid"]
                final_df.loc[last_index, "Live_Ask"] = live_prices[symbol]["Live_Ask"]
                final_df.loc[last_index, "Live_Mid"] = live_prices[symbol]["Live_Mid"]

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

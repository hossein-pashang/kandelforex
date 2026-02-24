import requests
import pandas as pd
from datetime import datetime, timedelta, timezone
import time

# ===============================
# CONFIG
# ===============================
OANDA_API_KEY = "156cfd275691cddf1ae9abca4378f544-1372068a45acd548a0ca1d6b9f483293"
ACCOUNT_TYPE = "practice"   # practice یا live
BASE_URL = f"https://api-fx{ACCOUNT_TYPE}.oanda.com/v3"

HEADERS = {
    "Authorization": f"Bearer {OANDA_API_KEY}"
}

# ===============================
# PROFESSIONAL FETCH FUNCTION
# ===============================
def fetch_candles(instrument, granularity="M5", days=7,
                  max_retries=5, max_delay_minutes=15):

    for attempt in range(max_retries):

        try:
            end_time = datetime.now(timezone.utc)
            start_time = end_time - timedelta(days=days)

            url = f"{BASE_URL}/instruments/{instrument}/candles"

            params = {
                "from": start_time.isoformat(),
                "to": end_time.isoformat(),
                "granularity": granularity,
                "price": "M"
            }

            response = requests.get(url, headers=HEADERS, params=params, timeout=10)

            if response.status_code != 200:
                print(f"[{instrument}] Retry {attempt+1} - Status {response.status_code}")
                time.sleep(2)
                continue

            data = response.json()

            if "candles" not in data or len(data["candles"]) == 0:
                print(f"[{instrument}] No candles returned")
                time.sleep(2)
                continue

            # فقط کندل کامل
            candles = [c for c in data["candles"] if c["complete"]]

            if len(candles) == 0:
                print(f"[{instrument}] No complete candles")
                time.sleep(2)
                continue

            df = pd.DataFrame([{
                "time": c["time"],
                "open": float(c["mid"]["o"]),
                "high": float(c["mid"]["h"]),
                "low": float(c["mid"]["l"]),
                "close": float(c["mid"]["c"]),
                "volume": c["volume"]
            } for c in candles])

            df["time"] = pd.to_datetime(df["time"])
            df.set_index("time", inplace=True)
            df.sort_index(inplace=True)

            # ===============================
            # CHECK DATA FRESHNESS
            # ===============================
            last_candle_time = df.index[-1]
            now = datetime.now(timezone.utc)
            delay = (now - last_candle_time).total_seconds() / 60

            print(f"[{instrument}] Last candle: {last_candle_time}")
            print(f"[{instrument}] Delay: {delay:.2f} minutes")

            if delay > max_delay_minutes:
                print(f"[{instrument}] ⚠ Data delayed. Retrying...")
                time.sleep(2)
                continue

            # ===============================
            # CHECK DATA LENGTH
            # ===============================
            expected_rows = int((days * 24 * 60) / get_minutes(granularity))

            if len(df) < expected_rows * 0.7:  # حداقل 70٪ دیتای مورد انتظار
                print(f"[{instrument}] ⚠ Not enough candles")
                time.sleep(2)
                continue

            print(f"[{instrument}] ✅ Data OK ({len(df)} candles)")
            return df

        except Exception as e:
            print(f"[{instrument}] ERROR: {e}")
            time.sleep(2)

    print(f"[{instrument}] ❌ Failed after retries")
    return None


# ===============================
# HELPER
# ===============================
def get_minutes(granularity):
    mapping = {
        "M1": 1,
        "M5": 5,
        "M15": 15,
        "M30": 30,
        "H1": 60,
        "H4": 240,
        "D": 1440
    }
    return mapping.get(granularity, 5)


# ===============================
# RUN
# ===============================
if __name__ == "__main__":

    instruments = [
        "EUR_USD", "GBP_USD", "USD_JPY", "USD_CHF",
        "AUD_USD", "NZD_USD", "USD_CAD", "EUR_GBP",
        "XAU_USD", "XAG_USD",
        "BCO_USD",
        "NAS100_USD"
    ]

    all_data = {}

    for instrument in instruments:
        print("\n============================")
        df = fetch_candles(instrument, granularity="M5", days=7)

        if df is not None:
            all_data[instrument] = df
        else:
            print(f"Skipping {instrument} due to data issue")

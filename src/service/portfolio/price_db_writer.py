import csv
import io
import os
import sys
import time

import requests
import yfinance as yf

from src.data.config import (
    LEDGER_IND_COMMODITY_LIST,
    LEDGER_PRICE_DB_DIR,
    LEDGER_US_COMMODITY_LIST,
)

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
CRYPTO_LIST = ["XRP", "BTC", "CORECHAIN", "NEAR"]


def read_commodity_file(file_path):
    """
    Reads Ledger commodity files and returns a SET of commodity symbols.

    Rules:
    - line must start with 'commodity'
    - skip comment lines starting with ';'
    - skip options: lines containing '; Option', '; GSec', '; Mutual Fund'
    - extract quoted name
    - normalize to UPPERCASE here
    """

    commodities = set()

    if not file_path.exists():
        return commodities

    with open(file_path) as f:
        for line in f:
            line = line.strip()

            if not line or line.startswith(";"):
                continue

            if not line.startswith("commodity"):
                continue

            if "; Option" in line or "; GSec" in line or "; Mutual Fund" in line:
                continue

            if '"' in line:
                name = line.split('"')[1].strip().upper()
                commodities.add(name)

    return commodities


def load_existing_price_commodities(price_file):
    """
    Load all commodities already present in a Ledger price file.
    """
    existing = set()

    if not price_file.exists():
        return existing

    with open(price_file) as f:
        for line in f:
            if line.startswith("P "):
                parts = line.split()
                if len(parts) >= 3:
                    existing.add(parts[2].replace('"', ""))

    return existing


_INSTRUMENT_CACHE = {}


def load_upstox_instruments():
    """
    Download and cache all Upstox NSE EQ instruments once.
    Handles JSON lines format returned by Upstox.
    """
    if _INSTRUMENT_CACHE:
        return _INSTRUMENT_CACHE

    print("Loading Upstox instrument master...")

    url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.csv.gz"
    headers = {"User-Agent": "Mozilla/5.0"}

    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()

    # The content is gzip-compressed JSON lines
    import gzip

    f = gzip.GzipFile(fileobj=io.BytesIO(resp.content))
    text_io = io.TextIOWrapper(f, encoding="utf-8")

    reader = csv.DictReader(text_io)

    count = 0
    for row in reader:
        symbol = row.get("tradingsymbol")
        instrument_key = row.get("instrument_key")

        if symbol and instrument_key:
            _INSTRUMENT_CACHE[symbol] = instrument_key
            _INSTRUMENT_CACHE[instrument_key] = instrument_key
            count += 1

    print(f"Loaded {count} NSE EQ instruments")
    return _INSTRUMENT_CACHE


def get_instrument_key(symbol):
    """Get instrument key from cache"""
    load_upstox_instruments()

    if symbol not in _INSTRUMENT_CACHE:
        raise ValueError(f"Symbol {symbol} not found in NSE instruments")

    return _INSTRUMENT_CACHE[symbol]


def fetch_ind_price_history(symbol, year):
    """
    Fetch daily closing prices from Upstox (EOD candles).
    Prices are always in INR.
    """
    if not UPSTOX_ACCESS_TOKEN:
        print("ERROR: UPSTOX_ACCESS_TOKEN not set")
        sys.exit(1)

    headers = {
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
        "Accept": "application/json",
    }

    start = f"{year}-01-01"
    end = f"{year}-12-31"

    # Get instrument key (uses cache)
    instrument_key = get_instrument_key(symbol)

    url = (
        f"https://api.upstox.com/v2/historical-candle/"
        f"{instrument_key}/day/{end}/{start}"
    )

    resp = requests.get(url, headers=headers)

    if resp.status_code != 200:
        print(f"ERROR: {symbol} - Status {resp.status_code}: {resp.text}")
        return {}

    data = resp.json()

    if "data" not in data or "candles" not in data["data"]:
        print(f"WARNING: No candle data for {symbol}")
        return {}

    prices = {}
    for candle in data["data"]["candles"]:
        date_str = candle[0][:10]
        close_price = float(candle[4])
        prices[date_str] = close_price

    return prices


def fetch_us_price_history(symbol, year):
    """
    Fetch daily closing prices from yfinance.
    """
    if symbol.upper() in CRYPTO_LIST:
        symbol = f"{symbol.upper()}-USD"

    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date, end=end_date)

    return {
        idx.strftime("%Y-%m-%d"): float(row["Close"]) for idx, row in data.iterrows()
    }


def write_prices_for_year(year, us_commodities, ind_commodities):
    """
    Write Ledger price entries for one year.
    """
    output_file = LEDGER_PRICE_DB_DIR / f"{year}.db"
    new_lines = []

    # Load existing commodities ONCE (performance fix)
    existing_commodities = load_existing_price_commodities(output_file)

    # ---------- US COMMODITIES ----------
    for commodity in us_commodities:
        if commodity in existing_commodities:
            # print(f"Skipping {commodity} (already present)")
            continue

        print(f"Fetching US commodity {commodity} for {year}")
        prices = fetch_us_price_history(commodity, year)

        for d, rate in sorted(prices.items()):
            new_lines.append(f'P {d} "{commodity}" {rate:.4f} USD')

    # ---------- INDIAN COMMODITIES ----------
    for commodity in ind_commodities:
        if commodity in existing_commodities:
            # print(f"Skipping {commodity} (already present)")
            continue

        print(f"Fetching Indian commodity {commodity} for {year}")
        prices = fetch_ind_price_history(commodity, year)

        time.sleep(3)

        for d, rate in sorted(prices.items()):
            new_lines.append(f'P {d} "{commodity}" {rate:.4f} INR')

    if not new_lines:
        print("No new prices to write.")
        return

    write_header = not output_file.exists() or output_file.stat().st_size == 0

    with open(output_file, "a") as f:
        if write_header:
            f.write(";\n")
            f.write(f"; Auto-generated prices for {year}\n")
            f.write(";\n")

        f.write("\n".join(new_lines))
        f.write("\n")

    print(f"Updated {output_file}")


if __name__ == "__main__":
    # Commodities are read ONCE and stored as SETS
    us_commodities = read_commodity_file(LEDGER_US_COMMODITY_LIST)
    ind_commodities = read_commodity_file(LEDGER_IND_COMMODITY_LIST)

    for year in range(2026, 2027):
        write_prices_for_year(year, us_commodities, ind_commodities)

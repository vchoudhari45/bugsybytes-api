import csv
import io
import os
import sys
import time
from datetime import datetime
from typing import Dict

import pandas as pd
import requests
import yaml
import yfinance as yf

from src.data.config import (
    DASHBOARD_CONFIG_PATH,
    LEDGER_IND_COMMODITY_LIST,
    LEDGER_IND_MF_COMMODITY_LIST,
    LEDGER_PRICE_DB_DIR,
    LEDGER_US_COMMODITY_LIST,
    NSE_GSEC_LIVE_DATA_DIR,
)
from src.service.util.csv_util import read_all_dated_csv_files_from_folder

UPSTOX_ACCESS_TOKEN = os.getenv("UPSTOX_ACCESS_TOKEN")
CRYPTO_LIST = ["XRP", "BTC", "CORECHAIN", "NEAR", "FLR"]
nse_gsec_files = read_all_dated_csv_files_from_folder(NSE_GSEC_LIVE_DATA_DIR)


with open(DASHBOARD_CONFIG_PATH, "r") as f:
    dashboard_config = yaml.safe_load(f)


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
_MF_SCHEME_CODE_CACHE = {}


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
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {UPSTOX_ACCESS_TOKEN}",
    }

    start = f"{year}-01-01"
    end = f"{year}-12-31"

    # Get instrument key (uses cache)
    instrument_key = get_instrument_key(symbol)
    url = (
        f"https://api.upstox.com/v3/historical-candle/"
        f"{instrument_key}/days/1/{end}/{start}"
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


_MF_NAV_CACHE: Dict[str, Dict] = None


def fetch_ind_mf_price_history(isin: str, year: int) -> Dict[str, float]:
    global _MF_NAV_CACHE

    if _MF_NAV_CACHE is None:
        _MF_NAV_CACHE = {}
        url = dashboard_config["dashboard"]["base_urls"]["mf_india_nav_all"]

        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            text = response.text.strip()

            for line in text.splitlines():
                line = line.strip()
                if not line or ";" not in line or line.startswith("Scheme Code"):
                    continue

                parts = [p.strip() for p in line.split(";")]
                if len(parts) < 6:
                    continue

                isin1 = parts[1] if len(parts) > 1 and parts[1] != "-" else ""
                isin2 = parts[2] if len(parts) > 2 and parts[2] != "-" else ""
                scheme_name = parts[3] if len(parts) > 3 else ""
                nav_str = parts[4] if len(parts) > 4 else "0"
                nav_date_str = parts[5] if len(parts) > 5 else ""

                try:
                    nav = float(nav_str)
                    dt = datetime.strptime(nav_date_str, "%d-%b-%Y")

                    # Changed to proper YYYY-MM-DD format
                    date_key = dt.strftime("%Y-%m-%d")

                    data = {
                        "nav": nav,
                        "date": date_key,
                        "date_obj": dt,
                        "scheme_name": scheme_name,
                    }

                    if isin1:
                        _MF_NAV_CACHE[isin1.upper()] = data
                    if isin2 and isin2 != isin1:
                        _MF_NAV_CACHE[isin2.upper()] = data

                except (ValueError, TypeError):
                    continue

            print(f"NAV cache built successfully with {len(_MF_NAV_CACHE)} schemes")

        except Exception as e:
            print(f"ERROR: Failed to build NAV cache - {e}")
            _MF_NAV_CACHE = {}

    isin_upper = isin.upper()
    if isin_upper not in _MF_NAV_CACHE:
        print(f"WARNING: ISIN {isin} not found in latest NAV")
        return {}

    cached = _MF_NAV_CACHE[isin_upper]

    if cached["date_obj"].year != year:
        print(
            f"WARNING: Latest NAV for {isin} is on {cached['date']} (not in year {year})"
        )
        return {}

    return {cached["date"]: cached["nav"]}


def write_prices_for_year(year, us_commodities, ind_commodities, ind_mf_commodities):
    """
    Write Ledger price entries for one year.
    """
    output_file = LEDGER_PRICE_DB_DIR / f"{year}.db"
    new_lines = []

    # Load existing commodities ONCE (performance fix)
    existing_commodities = load_existing_price_commodities(output_file)

    # Track (date + commodity) to avoid duplicates
    existing_prefixes = set()

    def add_price_line(d, commodity, rate, currency):
        prefix = f'P {d} "{commodity}"'
        if prefix not in existing_prefixes:
            new_lines.append(f"{prefix} {rate:.4f} {currency}")
            existing_prefixes.add(prefix)

    # ---------- US COMMODITIES ----------
    for commodity in us_commodities:
        if commodity in existing_commodities:
            # print(f"Skipping {commodity} (already present)")
            continue

        print(f"Fetching US commodity {commodity} for {year}")
        prices = fetch_us_price_history(commodity, year)

        for d, rate in sorted(prices.items()):
            add_price_line(d, commodity, float(rate), "USD")

    # ---------- INDIAN COMMODITIES ----------
    for commodity in ind_commodities:
        if commodity in existing_commodities:
            # print(f"Skipping {commodity} (already present)")
            continue

        print(f"Fetching Indian commodity {commodity} for {year}")
        prices = fetch_ind_price_history(commodity, year)

        time.sleep(3)

        for d, rate in sorted(prices.items()):
            add_price_line(d, commodity, float(rate), "INR")

    # ---------- INDIAN MUTUAL FUNDS ----------
    for commodity in ind_mf_commodities:
        if commodity in existing_commodities:
            # print(f"Skipping {commodity} (already present)")
            continue

        print(f"Fetching Indian MF {commodity} for {year}")
        prices = fetch_ind_mf_price_history(commodity, year)

        for d, rate in sorted(prices.items()):
            add_price_line(d, commodity, float(rate), "INR")

    # ---------- Update file from nse live data for GSec  ----------
    if not nse_gsec_files.empty:
        df = nse_gsec_files.copy()

        # Filter out Treasury Bills and GS series
        df = df[(df["SERIES"] != "TB") & (~df["SYMBOL"].str.startswith("GS", na=False))]
        df.rename(columns={"PREV.CLOSE": "PREV_CLOSE"}, inplace=True)
        df["DATE"] = pd.to_datetime(df["DATE"], errors="raise")

        for row in df.itertuples(index=False):
            if pd.isna(row.DATE):
                continue
            d = row.DATE.strftime("%Y-%m-%d")
            ltp = row.LTP

            # If LTP is '-' use PREV.CLOSE
            if not ltp or ltp == "-" or pd.isna(ltp):
                rate = float(str(row.PREV_CLOSE).replace(",", ""))
            else:
                rate = float(str(ltp).replace(",", ""))

            add_price_line(d, row.SYMBOL, rate, "INR")

    if not new_lines:
        print("No new prices to write.")
        return

    new_lines.sort(key=lambda line: (line.split('"')[1], line.split()[1]))
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
    ind_mf_commodities = read_commodity_file(LEDGER_IND_MF_COMMODITY_LIST)

    for year in range(2026, 2027):
        write_prices_for_year(year, us_commodities, ind_commodities, ind_mf_commodities)

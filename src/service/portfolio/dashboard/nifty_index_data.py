import time

import requests
import yaml

from src.data.config import DASHBOARD_CONFIG_PATH


def get_with_retry(session, url, max_retries=5, backoff_factor=2, timeout=60):
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=timeout)
            response.raise_for_status()
            return response
        except requests.exceptions.RequestException as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            if attempt < max_retries - 1:
                sleep_time = backoff_factor * (2**attempt)
                print(f"Retrying after {sleep_time}s...")
                time.sleep(sleep_time)
            else:
                raise e


def fetch_nse_stocks():
    with open(DASHBOARD_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)["dashboard"]["nifty_index"]

    base_url = config["base_url"]
    api_endpoint = config["api_endpoint"]
    indices = config["indices"]

    session = requests.Session()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Referer": base_url,
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
    }
    session.headers.update(headers)

    # Load main page to set cookies
    try:
        session.get(base_url, timeout=60)
    except Exception as e:
        print(f"Warning: Could not load main page: {e}")

    all_stocks = {}
    index_ffmc_totals = {}

    for index_name, index_param in indices.items():
        url = f"{base_url}{api_endpoint}?index={index_param}"
        try:
            response = get_with_retry(session, url)
            data = response.json()

            for stock in data.get("data", []):
                symbol = stock.get("symbol")

                if not symbol:
                    continue

                if symbol.upper().strip() == index_name.upper().strip():
                    continue

                ffmc = round(stock.get("ffmc", 0) or 0, 2)

                if symbol and symbol not in all_stocks:  # Priority logic
                    all_stocks[symbol] = {
                        "NIFTY INDEX": index_name,
                        "COMPANY NAME": stock.get("meta", {}).get("companyName", ""),
                        "30D %": round(stock.get("perChange30d", 0), 2),
                        "365D %": round(stock.get("perChange365d", 0), 2),
                        "NEAR 52W HIGH %": round(stock.get("nearWKH", 0), 2),
                        "NEAR 52W LOW %": round(stock.get("nearWKL", 0), 2),
                        "FREE FLOATING MARKET CAP": ffmc,
                        "PREV CLOSE": round(stock.get("previousClose", 0), 2),
                        "YEAR LOW": round(stock.get("yearLow", 0), 2),
                        "YEAR HIGH": round(stock.get("yearHigh", 0), 2),
                    }
                    index_ffmc_totals[index_name] = (
                        index_ffmc_totals.get(index_name, 0) + ffmc
                    )
        except Exception as e:
            print(f"Failed to fetch {index_name}: {e}")

    for symbol, stock_data in all_stocks.items():
        index_name = stock_data["NIFTY INDEX"]
        ffmc = stock_data["FREE FLOATING MARKET CAP"]
        total_ffmc = index_ffmc_totals.get(index_name, 0)
        if total_ffmc > 0:
            stock_data["TARGET INDEX WEIGHTAGE"] = round(ffmc / total_ffmc, 6)
        else:
            stock_data["TARGET INDEX WEIGHTAGE"] = 0.0

    weight_sums = {}
    for stock_data in all_stocks.values():
        index_name = stock_data["NIFTY INDEX"]
        weight = stock_data["TARGET INDEX WEIGHTAGE"]
        weight_sums[index_name] = weight_sums.get(index_name, 0) + weight

    print("\nWeight validation per index:")
    for index_name, total_weight in weight_sums.items():
        # tolerance because of rounding
        if abs(total_weight - 1.0) < 0.001:
            print(f"✅ {index_name} weight sum = {total_weight:.6f}")
        else:
            print(f"⚠️  {index_name} weight sum = {total_weight:.6f} (unexpected)")

    return all_stocks


# import csv
# import sys
# def main():
#     try:
#         stocks = fetch_nse_stocks()
#         print(f"Fetched {len(stocks)} stocks\n")
#         if not stocks:
#             return
#         # CSV header
#         fieldnames = ["SYMBOL"] + list(next(iter(stocks.values())).keys())
#         writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
#         writer.writeheader()
#         # Write rows
#         for symbol, data in stocks.items():
#             row = {"SYMBOL": symbol}
#             row.update(data)
#             writer.writerow(row)
#     except Exception as e:
#         print(f"Error running script: {e}")

# if __name__ == "__main__":
#     main()

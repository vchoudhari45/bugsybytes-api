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

    for index_name, index_param in indices.items():
        url = f"{base_url}{api_endpoint}?index={index_param}"
        try:
            response = get_with_retry(session, url)
            data = response.json()
            for stock in data.get("data", []):
                symbol = stock.get("symbol")
                if symbol and symbol not in all_stocks:  # ← Priority logic
                    all_stocks[symbol] = {
                        "NIFTY INDEX": index_name,
                        "COMPANY NAME": round(stock.get("companyName", 0), 2),
                        "30D %": round(stock.get("perChange30d", 0), 2),
                        "365D %": round(stock.get("perChange365d", 0), 2),
                        "NEAR 52W HIGH %": round(stock.get("nearWKH", 0), 2),
                        "NEAR 52W LOW %": round(stock.get("nearWKL", 0), 2),
                        "FREE FLOATING MARKET CAP": round(stock.get("ffmc", 0), 2),
                    }
        except Exception as e:
            print(f"Failed to fetch {index_name}: {e}")

    return all_stocks

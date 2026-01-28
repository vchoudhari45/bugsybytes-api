import sys

import yfinance as yf

from src.data.config import LEDGER_PRICE_DB_DIR, RED_BOLD, RESET


def fetch_price_history(symbol, year):
    """
    Fetch daily closing prices for a full year from yfinance.
    """
    start_date = f"{year}-01-01"
    end_date = f"{year + 1}-01-01"

    ticker = yf.Ticker(symbol)
    data = ticker.history(start=start_date, end=end_date)

    prices = {}
    for index, row in data.iterrows():
        prices[index.strftime("%Y-%m-%d")] = float(row["Close"])

    return prices


def commodity_exists_in_file(file_path, commodity):
    """
    Check if a commodity already exists in a Ledger price file.
    """
    if not file_path.exists():
        return False

    with open(file_path) as f:
        for line in f:
            if line.startswith("P ") and f" {commodity} " in line:
                return True
    return False


def validate_instruments_strict(instruments):
    """
    Strict validation:
    - yfinance symbol must be UPPERCASE
    - commodity must be UPPERCASE
    - base currency must be UPPERCASE

    Exits immediately on error.
    """

    for yf_symbol, commodity, base in instruments:

        if yf_symbol != yf_symbol.upper():
            print(
                f"{RED_BOLD}ERROR: yfinance symbol '{yf_symbol}' "
                f"must be UPPERCASE (use '{yf_symbol.upper()}') {RESET}"
            )
            sys.exit(1)

        if commodity != commodity.upper():
            print(
                f"{RED_BOLD}ERROR: Commodity '{commodity}' "
                f"must be UPPERCASE (use '{commodity.upper()}') {RESET}"
            )
            sys.exit(1)

        if base != base.upper():
            print(
                f"{RED_BOLD}ERROR: Base currency '{base}' "
                f"must be UPPERCASE (use '{base.upper()}') {RESET}"
            )
            sys.exit(1)


def write_prices_for_year(instruments, year):
    """
    Fetch prices for multiple instruments and write to a yearly Ledger prices file.
    """

    # validate for all uppercase
    validate_instruments_strict(instruments)

    output_file = LEDGER_PRICE_DB_DIR / f"{year}.db"
    new_lines = []

    for yf_symbol, commodity, base in instruments:
        if commodity_exists_in_file(output_file, commodity):
            print(f"Skipping {commodity} for {year} (already present)")
            continue

        print(f"Fetching {commodity} for {year}...")
        prices = fetch_price_history(yf_symbol, year)

        for d, rate in sorted(prices.items()):
            new_lines.append(f"P {d} {commodity} {rate:.4f} {base}")

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
    instruments = [
        ("USDINR=X", "USD", "INR"),
        ("GOOG", "GOOG", "USD"),
    ]
    write_prices_for_year(instruments, year=2020)
    write_prices_for_year(instruments, year=2021)
    write_prices_for_year(instruments, year=2022)
    write_prices_for_year(instruments, year=2023)
    write_prices_for_year(instruments, year=2024)
    write_prices_for_year(instruments, year=2025)
    write_prices_for_year(instruments, year=2026)

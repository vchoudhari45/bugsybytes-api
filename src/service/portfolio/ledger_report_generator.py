import csv
import subprocess
from pathlib import Path
from typing import List

from src.data.config import (
    LEDGER_ACCOUNT_LIST,
    LEDGER_ME_MAIN,
    LEDGER_MOM_MAIN,
    LEDGER_PAPA_MAIN,
    PORTFOLIO_REPORT,
)


def run_ledger_for_year_currency(
    year: int,
    currency: str,
    ledger_files: list,
) -> dict:
    """
    Run one ledger balance command for ALL accounts for a given year & currency.
    Parses the default ledger output (no --format needed).
    """
    end_date = f"{year + 1}-01-01"

    cmd = [
        "ledger",
        "balance",
        "-X",
        currency,
        "--strict",
        "-e",
        end_date,
    ]

    for lf in ledger_files:
        cmd.extend(["-f", str(lf)])

    # Debug print
    print(
        f"Running ledger balance for year={year}, currency={currency}\n"
        f"Command: {' '.join(cmd)}\n"
    )

    # Run ledger
    result = subprocess.run(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        shell=False,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Ledger error: {result.stderr.strip()}")

    balances = {}
    stack: List[str] = []
    for line in result.stdout.strip().splitlines():
        line = line.strip()
        if not line.strip():
            continue
        if line.strip().startswith("-") or line.strip() == "0":
            continue

        arr = line.split(" ")

        # filter out empty strings for clarity
        arr_filtered = [x for x in arr if x != ""]

        if len(arr_filtered) < 2:
            continue

        currency = arr_filtered[0]
        amount = arr_filtered[1]
        account_name = arr_filtered[-1]

        # number of empty strings
        level = len(arr) - 2 - (len(arr_filtered) - 1)
        # there are two spaces per level in ledger-cli output
        level = level // 2
        # print(f"{level}: {arr}")

        if level < len(stack):
            stack = stack[:level]  # truncate stack if needed

        if level == len(stack):
            # append new level
            stack.append(account_name)
        else:
            # replace existing level
            stack[level] = account_name

        full_account = ":".join(stack)
        # print(f"{full_account}: {amount}")
        balances[full_account] = amount

    return balances


def generate_merged_csv(
    account_list_file: Path,
    output_csv: Path,
    years: List[int],
    currencies: List[str],
    ledger_files: List[Path],
):
    """
    Generate CSV with accounts as rows, years/currencies as columns.
    Runs only one ledger CLI call per year per currency.
    """
    with open(account_list_file, "r", encoding="utf-8") as f:
        accounts = [
            line.replace("account", "").strip()
            for line in f
            if line.strip() and not line.startswith("#") and not line.startswith(";")
        ]
    accounts = sorted(accounts)

    # all_balances[account][year-currency] = amount
    all_balances = {acct: {} for acct in accounts}

    for currency in currencies:
        for year in years:
            balances = run_ledger_for_year_currency(year, currency, ledger_files)
            for account in accounts:
                balance = balances.get(f"{account}", "0")
                all_balances[account][f"{year}-{currency}"] = balance

    # Prepare CSV headers
    headers = ["Account"]
    for currency in currencies:
        for year in years:
            headers.append(f"{year}-{currency}")

    # Write CSV
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for account in accounts:
            row = [account]
            for currency in currencies:
                for year in years:
                    account = account
                    balance = all_balances[account].get(f"{year}-{currency}", "0")
                    row.append(balance)
            writer.writerow(row)


if __name__ == "__main__":
    # -------------------------------
    # Config using project constants
    # -------------------------------
    ledger_files = [
        LEDGER_ME_MAIN,
        LEDGER_MOM_MAIN,
        LEDGER_PAPA_MAIN,
    ]

    years = list(range(2020, 2027))
    currencies = ["INR", "USD"]

    generate_merged_csv(
        account_list_file=LEDGER_ACCOUNT_LIST,
        output_csv=PORTFOLIO_REPORT,
        years=years,
        currencies=currencies,
        ledger_files=ledger_files,
    )

    print(f"CSV generated: {PORTFOLIO_REPORT}")

import csv
import subprocess
from pathlib import Path
from typing import Dict, List

from openpyxl import Workbook
from openpyxl.styles import PatternFill

from src.data.config import (
    LEDGER_ACCOUNT_LIST,
    LEDGER_ME_MAIN,
    LEDGER_MOM_MAIN,
    LEDGER_PAPA_MAIN,
    PORTFOLIO_EXPECTED_BALANCES,
    PORTFOLIO_RECON_REPORT,
)


EXPECTED_BALANCES: Dict[str, Dict[str, float]] = {}
with open(PORTFOLIO_EXPECTED_BALANCES, "r", encoding="utf-8") as f:
    filtered_lines = (
        line for line in f
        if line.strip() and not line.lstrip().startswith("#")
    )
    reader = csv.DictReader(filtered_lines)
    for row in reader:
        account = row["Account"].strip()
        period = row["Period"].strip()
        expected = float(row["Expected"])
        EXPECTED_BALANCES.setdefault(account, {})[period] = expected


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
        f"Running ledger balance for year={year}, currency={currency}"
        # f"\nCommand: {' '.join(cmd)}\n"
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
        amount = float(arr_filtered[1].replace(",", ""))
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


def generate_reconciled_xlsx(
    account_list_file: Path,
    output_xlsx: Path,
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
    all_balances: Dict[str, Dict[str, float]] = {acct: {} for acct in accounts}

    for currency in currencies:
        for year in years:
            balances = run_ledger_for_year_currency(year, currency, ledger_files)
            for account in accounts:
                balance = balances.get(f"{account}", "0")
                all_balances[account][f"{year}-{currency}"] = balance

    # Prepare CSV headers
    wb = Workbook()
    ws = wb.active
    ws.title = "Portfolio"

    GREEN = PatternFill(
        fill_type="solid",
        start_color="C6EFCE",
        end_color="C6EFCE",
    )

    RED = PatternFill(
        fill_type="solid",
        start_color="FFC7CE",
        end_color="FFC7CE",
    )

    WHITE = PatternFill(
        fill_type="solid",
        start_color="FFFFFF",
        end_color="FFFFFF",
    )

    # Write CSV
    headers = ["Account"]
    for currency in currencies:
        for year in years:
            headers.append(f"{year}-{currency}")

    ws.append(headers)

    for account in accounts:
        row = [account]
        for key in headers[1:]:
            row.append(all_balances[account].get(key, 0.0))
        ws.append(row)

        row_idx = ws.max_row
        for col_idx, key in enumerate(headers[1:], start=2):
            cell = ws.cell(row=row_idx, column=col_idx)
            actual = float(cell.value or 0)

            # lookup expected value
            expected = EXPECTED_BALANCES.get(account, {}).get(key)

            if expected is None:
                cell.fill = WHITE
            elif abs(actual - expected) < 0.01:
                cell.fill = GREEN
            else:
                cell.fill = RED

    wb.save(output_xlsx)


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

    generate_reconciled_xlsx(
        account_list_file=LEDGER_ACCOUNT_LIST,
        output_xlsx=PORTFOLIO_RECON_REPORT,
        years=years,
        currencies=currencies,
        ledger_files=ledger_files,
    )

    print("✅ Reconciled XLSX generated successfully")

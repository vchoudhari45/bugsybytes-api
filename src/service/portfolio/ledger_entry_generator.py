import os
import sys
import traceback
from collections import defaultdict, deque
from decimal import ROUND_HALF_UP, Decimal

from src.data.config import (
    LEDGER_ME_DIR,
    LEDGER_MOM_DIR,
    LEDGER_PAPA_DIR,
    TRANSACTION_ME_DIR,
    TRANSACTION_MOM_DIR,
    TRANSACTION_PAPA_DIR,
)

# Round to exactly 6 decimal places
SIX_DP = Decimal("0.000001")


def transaction_sort_key(row_parts):
    """
    Sorting key for CSV rows.

    Primary sort:
        - date (YYYY-MM-DD)

    Secondary sort (same date):
        - BUY equity first
        - SELL equity next
        - Non-equity last
    """

    (
        date,
        description,
        from_account,
        from_value,
        to_account,
        to_value,
        adjustment_account,
        adjustment_value,
        lot_selection_method,
    ) = row_parts

    # BUY: Equity goes TO an equity account
    if is_equity_account(to_account):
        priority = 0

    # SELL: Equity comes FROM an equity account
    elif is_equity_account(from_account):
        priority = 1

    # Non-equity
    else:
        priority = 2

    return (date, priority)


def fmt(value: Decimal) -> Decimal:
    """
    Force 6 decimal places everywhere.
    This prevents lot mismatches later.
    """
    return value.quantize(SIX_DP, rounding=ROUND_HALF_UP)


def is_equity_account(account: str) -> bool:
    """
    Any account containing investment instruments.

    Recognized asset classes:
      - :Equity:
      - :MutualFunds:
      - :Options:
      - :GSec:
    """
    INVESTMENT_MARKERS = (
        "Assets:Investments:Equity:",
        "Assets:Investments:MutualFunds:",
        "Assets:Investments:Options:",
        "Assets:Investments:GSec:",
    )
    EXCLUDE_ACCOUNTS = {
        "Assets:Investments:MutualFunds:ICICI",
    }

    if account in EXCLUDE_ACCOUNTS:
        return False

    return any(marker in account for marker in INVESTMENT_MARKERS)


def parse_amount(value: str):
    """
    Parse amount and unit.

    Rules:
    - First token is always the numeric amount
    - Everything after the first space is the unit
    - If unit is wrapped in double quotes, treat it as one unit

    Examples:
        "-4900.4 USD"
            -> (-4900.4, "USD")

        "40 ACLS"
            -> (40, "ACLS")

        '1 "AAPL 2023-10-23 Call 195.00"'
            -> (1, "AAPL 2023-10-23 Call 195.00")
    """
    value = value.strip()
    # Split only ONCE: amount | rest
    amount_str, unit_part = value.split(None, 1)

    amount = Decimal(amount_str)

    # Remove surrounding double quotes if present
    unit_part = unit_part.strip()
    if unit_part.startswith('"') and unit_part.endswith('"'):
        unit_part = unit_part[1:-1]

    return amount, unit_part


def select_lot(lots_deque, method: str):
    """
    Select lot based on lot selection method.
    """
    method = (method or "FIFO").upper()

    if method == "FIFO":
        return lots_deque[0]
    if method == "LIFO":
        return lots_deque[-1]
    if method == "HIFO":
        return max(lots_deque, key=lambda l: l["cost"])

    raise ValueError(f"Unsupported lot selection method: {method}")


def csv_to_ledger_year_range(
    transaction_dir: str,
    ledger_dir: str,
    start_year: int,
    end_year: int,
):
    """
    Converts CSV files into Ledger files for a range of years.
    One ledger file per year.
    FIFO lot accounting per (equity_account, symbol).
    Rows are sorted by date and BUY before SELL on the same date.

    Args:
        transaction_dir: directory containing CSVs (one per year)
        ledger_dir: directory to write Ledger files
        start_year: first year to process
        end_year: last year to process
    """

    # FIFO lot storage
    # Key: (equity_account, symbol)
    # Value: deque of lots, each lot is a dict: {"qty": Decimal, "cost": Decimal}
    lots = defaultdict(deque)

    for year in range(start_year, end_year + 1):
        csv_path = f"{transaction_dir}/{year}.csv"
        ledger_path = f"{ledger_dir}/{year}.ledger"

        if not os.path.exists(csv_path):
            print(
                f"Transaction file not found for year: {year} in transaction_dir: {transaction_dir}"
            )
            sys.exit(1)

        rows = []

        # -------------------------
        # Read CSV and store rows
        # -------------------------
        with open(csv_path, "r", encoding="utf-8") as f:
            next(f, None)  # skip CSV header

            for row in f:
                row = row.strip()
                if not row or row.startswith("#"):
                    continue

                parts = row.split(",")
                if len(parts) != 9: 
                    raise RuntimeError(f"Malformed row: {row}")

                rows.append(parts)

        # -------------------------
        # Sort rows by:
        #   1. date ascending
        #   2. BUY first, SELL second, Non-equity last
        # -------------------------
        rows.sort(key=transaction_sort_key)

        lines = []

        # -------------------------
        # Process each row in sorted order
        # -------------------------
        for (
            date,
            description,
            from_account,
            from_value,
            to_account,
            to_value,
            adjustment_account,
            adjustment_value,
            lot_selection_method
        ) in rows:

            # Ledger transaction header
            lines.append(f"{date} {description}")

            # Parse amounts and units
            from_amt, from_unit = parse_amount(from_value)
            to_amt, to_unit = parse_amount(to_value)

            # -------------------------
            # BUY Equity Transaction
            # Cash -> Equity
            # -------------------------
            if is_equity_account(to_account):
                qty = to_amt  # Shares bought
                symbol = to_unit  # e.g., ACLS, INFY
                total_cost = abs(from_amt)  # Cash paid
                cost_per_unit = fmt(total_cost / qty)

                # Push lot into FIFO queue
                lots[(to_account, symbol)].append({"qty": qty, "cost": cost_per_unit})
                currency = from_value.split()[-1]

                lines.append(f"    {from_account:<50}{fmt(from_amt)} {currency}")
                lines.append(
                    f'    {to_account:<50}{qty} "{symbol}" @ {cost_per_unit} {currency}'
                )
            # -------------------------
            # SELL Equity Transaction
            # Equity -> Cash
            # Consume FIFO lots
            # -------------------------
            elif is_equity_account(from_account):
                sell_qty = abs(from_amt)
                symbol = from_unit
                proceeds = to_amt
                currency = to_value.split()[-1]
                sell_price = fmt(abs(proceeds) / sell_qty)
                lot_key = (from_account, symbol)

                # sell quantity from deque until it is greater than 0
                remaining = sell_qty
                while remaining > 0:
                    if not lots[lot_key]:
                        print(
                            f" Not enough shares to sell "
                            f"{sell_qty} {symbol} from {from_account} on date: {date}"
                        )
                        sys.exit(1)

                    lot = select_lot(lots[lot_key], lot_selection_method)
                    take = min(lot["qty"], remaining)

                    lines.append(
                        f"    {from_account:<50}-"
                        f'{take} "{symbol}" '
                        f"{{{lot['cost']} {currency}}} "
                        f"@ {sell_price} {currency}"
                    )

                    # remove used quantity from deque
                    lot["qty"] -= take
                    remaining -= take
                    if lot["qty"] == 0:
                        lots[lot_key].remove(lot)

                # Cash received
                lines.append(f"    {to_account:<50}{fmt(proceeds)} {currency}")
            # Non-Equity Transactions
            else:
                lines.append(f"    {from_account:<50}{from_value}")
                lines.append(f"    {to_account:<50}{to_value}")

            # append adjustment posting
            if adjustment_account:
                if adjustment_value:
                    # explicit posting: account + amount
                    lines.append(f"    {adjustment_account:<50}{adjustment_value}")
                else:
                    # implicit balancing posting: account only
                    lines.append(f"    {adjustment_account}")

            # Blank line between transactions
            lines.append("")

        # Remove last empty line
        if lines and lines[-1] == "":
            lines.pop()

        # Write ledger file
        with open(ledger_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))


# Entry Point
if __name__ == "__main__":
    try:
        csv_to_ledger_year_range(
            TRANSACTION_ME_DIR,
            LEDGER_ME_DIR,
            start_year=2020,
            end_year=2026,
        )

        csv_to_ledger_year_range(
            TRANSACTION_MOM_DIR,
            LEDGER_MOM_DIR,
            start_year=2020,
            end_year=2026,
        )

        csv_to_ledger_year_range(
            TRANSACTION_PAPA_DIR,
            LEDGER_PAPA_DIR,
            start_year=2020,
            end_year=2026,
        )

    except Exception:
        print("Error occurred:", file=sys.stderr)
        traceback.print_exc()
        sys.exit(2)

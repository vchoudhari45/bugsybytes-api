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

# Round to exactly 4 decimal places
FOUR_DP = Decimal("0.0001")


def d(value: str) -> Decimal:
    """
    Convert string to Decimal.
    """
    return Decimal(value)


def fmt(value: Decimal) -> Decimal:
    """
    Force 4 decimal places everywhere.
    This prevents lot mismatches later.
    """
    return value.quantize(FOUR_DP, rounding=ROUND_HALF_UP)


def is_equity_account(account: str) -> bool:
    """
    Any account containing investment instruments.

    Recognized asset classes:
      - :Equity:
      - :MutualFunds:
      - :Options:
      - :GSec:

    Examples:
      Assets:Investments:Equity:Etrade:XXXX        -> True
      Assets:Investments:MutualFunds:Etrade:XXXX   -> True
      Assets:Investments:Options:Etrade:XXXX       -> True
      Assets:Investments:GSec:Zerodha:XXXX         -> True
      Assets:Investments:Cash:Etrade:XXXX          -> False
    """
    INVESTMENT_MARKERS = (
        ":Equity:",
        ":MutualFunds:",
        ":Options:",
        ":GSec:",
    )

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
    """

    # FIFO LOT STORAGE
    #
    # Key   = (equity_account, symbol)
    # Value = deque of lots
    #
    # Each lot:
    #   {
    #       "qty": Decimal,
    #       "cost": Decimal (cost per unit)
    #   }
    #
    lots = defaultdict(deque)

    for year in range(start_year, end_year + 1):
        csv_path = f"{transaction_dir}/{year}.csv"
        ledger_path = f"{ledger_dir}/{year}.ledger"

        if not os.path.exists(csv_path):
            print(
                f"Transaction file not found for year: {year} in transaction_dir: {transaction_dir}"
            )
            sys.exit(1)

        lines = []

        with open(csv_path, "r", encoding="utf-8") as f:
            next(f, None)  # skip CSV header

            for row in f:
                row = row.strip()
                if not row or row.startswith("#"):
                    continue

                parts = row.split(",")
                if len(parts) != 8:
                    raise RuntimeError(f"Malformed row: {row}")

                (
                    date,
                    description,
                    from_account,
                    from_value,
                    to_account,
                    to_value,
                    adjustment_account,
                    adjustment_value,
                ) = parts

                # Ledger transaction header
                lines.append(f"{date} {description}")

                from_amt, from_unit = parse_amount(from_value)
                to_amt, to_unit = parse_amount(to_value)

                # Buy Equity Transaction
                if is_equity_account(to_account):
                    qty = to_amt  # Shares bought
                    symbol = to_unit  # ACLS / INFY / etc.
                    total_cost = abs(from_amt)  # Money paid
                    cost_per_unit = fmt(total_cost / qty)

                    # Push lot into FIFO queue
                    lots[(to_account, symbol)].append(
                        {"qty": qty, "cost": cost_per_unit}
                    )

                    currency = from_value.split()[-1]

                    lines.append(f"    {from_account:<50}{fmt(from_amt)} {currency}")
                    lines.append(
                        f"    {to_account:<50}{qty} {symbol} @ {cost_per_unit} {currency}"
                    )
                # Sell Equity Transaction
                # FIFO lots are popped until sell quantity is satisfied.
                elif is_equity_account(from_account):
                    sell_qty = abs(from_amt)
                    symbol = from_unit
                    proceeds = to_amt

                    currency = to_value.split()[-1]
                    sell_price = fmt(abs(proceeds) / sell_qty)

                    lot_key = (from_account, symbol)

                    if lot_key not in lots:
                        print(f"Selling without inventory: {from_account} {symbol}")
                        sys.exit(1)

                    # Starts equal to requested sell quantity
                    remaining = sell_qty
                    while remaining > 0:
                        if not lots[lot_key]:
                            print(
                                f" Not enough shares to sell "
                                f"{sell_qty} {symbol} from {from_account}"
                            )
                            sys.exit(1)

                        lot = lots[lot_key][0]
                        take = min(lot["qty"], remaining)

                        # Ledger posting for this lot
                        lines.append(
                            f"    {from_account:<50}-"
                            f"{take} {symbol} "
                            f"{{{lot['cost']} {currency}}} "
                            f"@ {sell_price} {currency}"
                        )

                        lot["qty"] -= take
                        remaining -= take

                        if lot["qty"] == 0:
                            lots[lot_key].popleft()

                    lines.append(f"    {to_account:<50}{fmt(proceeds)} {currency}")

                    # Capital gains (implicit balancing)
                    if adjustment_account:
                        lines.append(f"    {adjustment_account}")

                # Non Equity Transactions
                else:
                    lines.append(f"    {from_account:<50}{from_value}")
                    lines.append(f"    {to_account:<50}{to_value}")
                    if adjustment_account:
                        lines.append(f"    {adjustment_account}")

                lines.append("")

        if lines and lines[-1] == "":
            lines.pop()

        with open(ledger_path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines))


# ENTRY POINT
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

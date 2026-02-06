import csv
import sys
from datetime import date, datetime

from src.data.config import SCRIP_CODE_TO_TICKER_LOOKUP, STATEMENTS_ME_DIR
from src.service.portfolio.transaction.statement_rule_engine import create_transaction
from src.service.util.csv_util import non_comment_lines, normalized_dict_reader
from src.service.util.date_util import parse_date


def ingest_zb_tb_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_date(row.get("trade_date", "").strip())

                # Symbol
                symbol = row.get("symbol", "").strip()

                # Series
                series = row.get("series", "").strip().upper()
                if series in ("GS", "SG"):
                    name = "GSec"
                else:
                    name = row.get("symbol", "").strip()

                # Quantity and price
                quantity = float(row.get("quantity").replace(",", ""))
                price = float(row.get("price").replace(",", ""))

                # Net amount
                net_amount = quantity * price

                # Remark
                trade_type = row.get("trade_type", "").strip().lower()
                if trade_type == "buy":
                    remark = f"Bought {name} {symbol}"
                else:
                    remark = f"Sold {name} {symbol}"

                # FROM_VALUE / TO_VALUE logic
                if trade_type == "buy":
                    from_value = f"{-net_amount} INR"
                    to_value = f'{quantity} "{symbol}"'
                else:  # sell
                    from_value = f'-{quantity} "{symbol}"'
                    to_value = f"{net_amount} INR"

                # Create transaction
                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )

                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_zb_lg_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_date(row.get("posting_date", "").strip())

                # Remark
                remark = row.get("particulars", "").strip()

                # Amounts
                debit = float(row.get("debit").replace(",", ""))
                credit = float(row.get("credit").replace(",", ""))

                net_amount = credit - debit
                abs_amount = abs(net_amount)

                # FROM_VALUE / TO_VALUE logic
                if net_amount < 0:
                    from_value = f"{-abs_amount} INR"
                    to_value = f"{abs_amount} INR"
                else:
                    from_value = f"{-abs_amount} INR"
                    to_value = f"{abs_amount} INR"

                # Create transaction
                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )

                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def load_scrip_lookup():
    lookup = {}
    with open(SCRIP_CODE_TO_TICKER_LOOKUP, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            code = row["ScripCode"].strip()
            ticker = row["Ticker"].strip()
            lookup[code] = ticker
    return lookup


SCRIP_LOOKUP = load_scrip_lookup()


def ingest_ub_tb_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_date(row.get("Date", "").strip())

                # ScripCode → Ticker
                scrip_code = row.get("Scrip Code", "").strip()
                ticker = SCRIP_LOOKUP.get(scrip_code, "")

                # Remark: Company + BOUGHT/ SOLD
                side = row.get("Side", "").strip().lower()
                remark = f"BOUGHT {ticker}" if side == "buy" else f"SOLD {ticker}"

                # Quantity
                quantity = float(row.get("Quantity").replace(",", "").strip())

                # Net amount
                net_amount = quantity * float(row.get("Price").replace(",", "").strip())

                # From / To values
                if side == "buy":
                    from_value = f"{-net_amount} INR"
                    to_value = f"{quantity} {ticker}"
                else:
                    from_value = f"{quantity} {ticker}"
                    to_value = f"{net_amount} INR"

                # Create transaction
                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )

                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_ub_lg_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Debit / Credit may contain '–' or be empty
                debit_raw = row.get("Debit", "").replace("–", "").replace("—", "")
                credit_raw = row.get("Credit", "").replace("–", "").replace("—", "")

                debit = 0.0 if debit_raw == "" else float(debit_raw)
                credit = 0.0 if credit_raw == "" else float(credit_raw)

                net_amount = credit - debit
                abs_amount = abs(net_amount)

                from_value = f"{str(-abs_amount)} INR"
                to_value = f"{str(abs_amount)} INR"

                # Dates
                date = parse_date(row.get("Trade Date", "").strip())

                remark = row.get("Narration", "").strip()

                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )

                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_ks_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f))

            for row in reader:
                credit = float(str(row.get("Credit", "")).replace(",", "").strip() or 0)
                debit = float(str(row.get("Debit", "")).replace(",", "").strip() or 0)

                net_amount = credit - debit

                abs_amount = abs(net_amount)
                from_value = f"{str(-abs_amount)} INR"
                to_value = f"{str(abs_amount)} INR"

                raw_date = row.get("Transaction Date", "").strip()
                date_part = raw_date.split()[0]
                date = parse_date(date_part)

                remark = row.get("Description", "").strip()
                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )
                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_hs_statements(statement_type, filename, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = normalized_dict_reader(non_comment_lines(f))

            for row in reader:
                debit = float(row.get("Debit Amount", 0) or 0)
                credit = float(row.get("Credit Amount", 0) or 0)

                net_amount = credit - debit
                abs_amount = abs(net_amount)

                from_value = f"{str(-abs_amount)} INR"
                to_value = f"{str(abs_amount)} INR"

                remark = row.get("Narration", "").strip()
                date = parse_date(row["Date"])

                transaction = create_transaction(
                    statement_type=statement_type,
                    who=who,
                    date=date,
                    remark=remark,
                    from_value=from_value,
                    to_value=to_value,
                    net_amount=net_amount,
                )

                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_is_statements(statement_type, filename, who):
    new_transactions = []
    for file in STATEMENTS_ME_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = normalized_dict_reader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                deposit = float(row.get("Deposit Amount (INR )"))
                withdrawal = float(row.get("Withdrawal Amount (INR )"))

                net_amount = deposit - withdrawal
                abs_amount = abs(net_amount)

                from_value = f"{str(-abs_amount)} INR"
                to_value = f"{str(abs_amount)} INR"

                remark = row.get("Transaction Remarks", "").strip()
                date = parse_date(row["Transaction Date"])

                transaction = create_transaction(
                    statement_type, who, date, remark, from_value, to_value, net_amount
                )
                new_transactions.append(transaction)

    return sorted(new_transactions, key=lambda x: x["DATE(YYYY-MM-DD)"])


def ingest_statements(statement_type, filename, who, print_after_date):
    print()
    print("#" * 130)
    print(
        f"Parsing transactions"
        f" from file: {filename},"
        f" for person: {who},"
        f" of statement_type: {statement_type}"
    )
    print("#" * 130)
    print()
    fieldnames = [
        "DATE(YYYY-MM-DD)",
        "TRANSACTION_REMARK",
        "FROM_ACCOUNT",
        "FROM_VALUE",
        "TO_ACCOUNT",
        "TO_VALUE",
        "ADJUSTMENT_ACCOUNT",
        "ADJUSTMENT_VALUE",
        "LOT_SELECTION_METHOD",
    ]

    if statement_type == "is":
        transactions = ingest_is_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "hs":
        transactions = ingest_hs_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "ks":
        transactions = ingest_ks_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "ub-lg":
        transactions = ingest_ub_lg_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "ub-tb":
        transactions = ingest_ub_tb_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "zb-lg":
        transactions = ingest_zb_lg_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "zb-tb":
        transactions = ingest_zb_tb_statements(
            statement_type=statement_type, filename=filename, who=who
        )

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for tx in transactions:
        tx_date = datetime.strptime(tx["DATE(YYYY-MM-DD)"], "%Y-%m-%d").date()
        if tx_date > print_after_date:
            writer.writerow(tx)


if __name__ == "__main__":
    # date(YYYY, MM, DD)
    d = date(2025, 12, 31)
    ingest_statements(statement_type="is", filename="is", who="me", print_after_date=d)
    ingest_statements(statement_type="hs", filename="hs", who="me", print_after_date=d)
    ingest_statements(statement_type="ks", filename="ks", who="me", print_after_date=d)
    ingest_statements(
        statement_type="ub-lg", filename="ub-lg", who="me", print_after_date=d
    )
    ingest_statements(
        statement_type="ub-tb", filename="ub-tb", who="me", print_after_date=d
    )
    ingest_statements(
        statement_type="zb-lg", filename="zb-lg", who="me", print_after_date=d
    )
    ingest_statements(
        statement_type="zb-tb", filename="zb-tb", who="me", print_after_date=d
    )

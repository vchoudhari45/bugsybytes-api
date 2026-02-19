import csv
import sys
from datetime import date

import yaml

from src.data.config import (
    ADDITIONAL_STATEMENTS_DIR,
    DASHBOARD_CONFIG_PATH,
    SCRIP_CODE_TO_TICKER_LOOKUP,
)
from src.service.portfolio.transaction.statement_rule_engine import create_transaction
from src.service.util.csv_util import non_comment_lines, normalized_dict_reader
from src.service.util.date_util import parse_indian_date_format

with open(DASHBOARD_CONFIG_PATH, "r") as f:
    dashboard_config = yaml.safe_load(f)
mutual_funds_gr_config = dashboard_config["dashboard"]["mutual_funds_gr"]


def ingest_gr_lg_statements(statement_type, filename, who):
    new_transactions = []

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse Settlement Date (as required in output mapping)
                date = parse_indian_date_format(row.get("Settlement Date", "").strip())

                # Read first 3 letters of Voucher No.
                voucher_no = (
                    row.get("Voucher No.", "").strip().upper()
                )  # normalize to uppercase
                voucher_prefix = voucher_no[:3]  # first 3 letters

                # Prepend appropriate category
                if voucher_prefix == "SET":
                    remark = f"Settlement: {voucher_no}"
                elif voucher_prefix == "WTD":
                    remark = f"Withdrawal: {voucher_no}"
                elif voucher_prefix == "EXP":
                    remark = f"Expenses: {voucher_no}"
                elif voucher_prefix == "DEP":
                    remark = f"Deposit: {voucher_no}"
                else:
                    print(f"voucher_no: {voucher_no} has no valid prefix")
                    sys.exit(1)

                # Parse amounts
                debit_raw = row.get("Debit (Rs.)", "0").replace(",", "").strip()
                credit_raw = row.get("Credit (Rs.)", "0").replace(",", "").strip()

                debit = float(debit_raw) if debit_raw else 0.0
                credit = float(credit_raw) if credit_raw else 0.0

                # Net amount calculation
                net_amount = credit - debit
                abs_amount = abs(net_amount)

                from_value = f"{-abs_amount} INR"
                to_value = f"{abs_amount} INR"

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


def ingest_gr_tb_mf_statements(statement_type, filename, who):
    new_transactions = []

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date (Date column)
                date = parse_indian_date_format(row.get("Date", "").strip())

                # Scheme lookup
                scheme_name = row.get("Scheme Name", "").strip()
                scheme_config = mutual_funds_gr_config.get(scheme_name, {})

                symbol = scheme_config.get("isin", "").strip()
                name = scheme_config.get("name", scheme_name)

                # Quantity and amount
                quantity_raw = row.get("Units", "0").replace(",", "").strip()
                amount_raw = row.get("Amount", "0").replace(",", "").strip()

                quantity = float(quantity_raw) if quantity_raw else 0.0
                net_amount = float(amount_raw) if amount_raw else 0.0

                # Transaction type
                transaction_type = row.get("Transaction Type", "").strip().upper()

                if transaction_type == "PURCHASE":
                    from_value = f"{-net_amount} INR"
                    to_value = f'{quantity} "{symbol}"'
                    remark = f"Bought {name}"
                else:
                    from_value = f'-{quantity} "{symbol}"'
                    to_value = f"{net_amount} INR"
                    remark = f"Sold {name}"

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


def ingest_gr_tb_statements(statement_type, filename, who):
    new_transactions = []

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse execution date
                date = parse_indian_date_format(
                    row.get("Execution date and time", "").strip()
                )

                # Basic fields
                symbol = row.get("Symbol", "").strip()
                trade_type = row.get("Type", "").strip().upper()

                # Quantity and amount
                quantity_raw = row.get("Quantity", "0").replace(",", "").strip()
                amount_raw = row.get("Value", "0").replace(",", "").strip()

                quantity = float(quantity_raw) if quantity_raw else 0.0
                net_amount = float(amount_raw) if amount_raw else 0.0

                # Remark
                if trade_type == "BUY":
                    remark = f"Bought {symbol}"
                else:
                    remark = f"Sold {symbol}"

                # FROM_VALUE / TO_VALUE logic
                if trade_type == "BUY":
                    from_value = f"{-net_amount} INR"
                    to_value = f'{quantity} "{symbol}"'
                else:  # SELL
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


def ingest_zb_tb_statements(statement_type, filename, who):
    new_transactions = []

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_indian_date_format(row.get("trade_date", "").strip())

                # Symbol
                symbol = row.get("symbol", "").strip()

                # Series
                series = row.get("series", "").strip().upper()
                if series in ("GS", "SG", "G"):
                    name = f"GSec {symbol}"
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
                    remark = f"Bought {name}"
                else:
                    remark = f"Sold {name}"

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


def ingest_zb_tb_mf_statements(statement_type, filename, who):
    new_transactions = []

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_indian_date_format(row.get("trade_date", "").strip())

                # Symbol
                symbol = row.get("isin", "").strip()
                name = row.get("symbol", "").strip()

                # Quantity and price
                quantity = float(row.get("quantity").replace(",", ""))
                price = float(row.get("price").replace(",", ""))

                # Net amount
                net_amount = quantity * price

                # Remark
                trade_type = row.get("trade_type", "").strip().lower()
                if trade_type == "buy":
                    remark = f"Bought {name}"
                else:
                    remark = f"Sold {name}"

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

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_indian_date_format(row.get("posting_date", "").strip())

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

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                # Parse date
                date = parse_indian_date_format(row.get("Date", "").strip())

                # ScripCode → Ticker
                scrip_code = row.get("Scrip Code", "").strip()
                ticker = SCRIP_LOOKUP.get(scrip_code, "").strip()

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
                    to_value = f'{quantity} "{ticker}"'
                else:
                    from_value = f'-{quantity} "{ticker}"'
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

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
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
                date = parse_indian_date_format(row.get("Trade Date", "").strip())

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

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

            for row in reader:
                credit = float(str(row.get("Credit", "")).replace(",", "").strip() or 0)
                debit = float(str(row.get("Debit", "")).replace(",", "").strip() or 0)

                net_amount = credit - debit

                abs_amount = abs(net_amount)
                from_value = f"{str(-abs_amount)} INR"
                to_value = f"{str(abs_amount)} INR"

                raw_date = row.get("Transaction Date", "").strip()
                date_part = raw_date.split()[0]
                date = parse_indian_date_format(date_part)

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

    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
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
                date = parse_indian_date_format(row["Date"])

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
    for file in ADDITIONAL_STATEMENTS_DIR.glob(f"{filename}.csv"):
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
                date = parse_indian_date_format(row["Transaction Date"])

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
    elif statement_type == "zb-tb-mf":
        transactions = ingest_zb_tb_mf_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "gr-lg":
        transactions = ingest_gr_lg_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "gr-tb-mf":
        transactions = ingest_gr_tb_mf_statements(
            statement_type=statement_type, filename=filename, who=who
        )
    elif statement_type == "gr-tb":
        transactions = ingest_gr_tb_statements(
            statement_type=statement_type, filename=filename, who=who
        )

    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    for tx in transactions:
        tx_date = tx["DATE(YYYY-MM-DD)"].date()
        if tx_date > print_after_date:
            tx["DATE(YYYY-MM-DD)"] = tx_date.strftime("%Y-%m-%d")
            writer.writerow(tx)


if __name__ == "__main__":
    # date(YYYY, MM, DD)
    d = date(2026, 2, 15)
    # ingest me
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
    ingest_statements(
        statement_type="zb-tb-mf", filename="zb-tb-mf", who="me", print_after_date=d
    )

    # ingest mom
    ingest_statements(
        statement_type="is", filename="is-mom", who="mom", print_after_date=d
    )
    ingest_statements(
        statement_type="zb-lg", filename="zb-lg-mom", who="mom", print_after_date=d
    )
    ingest_statements(
        statement_type="zb-tb", filename="zb-tb-mom", who="mom", print_after_date=d
    )
    ingest_statements(
        statement_type="zb-tb-mf",
        filename="zb-tb-mf-mom",
        who="mom",
        print_after_date=d,
    )

    # ingest papa
    ingest_statements(
        statement_type="is", filename="is-papa", who="papa", print_after_date=d
    )
    ingest_statements(
        statement_type="gr-lg", filename="gr-lg-papa", who="papa", print_after_date=d
    )
    ingest_statements(
        statement_type="gr-tb-mf",
        filename="gr-tb-mf-papa",
        who="papa",
        print_after_date=d,
    )
    ingest_statements(
        statement_type="gr-tb", filename="gr-tb-papa", who="papa", print_after_date=d
    )

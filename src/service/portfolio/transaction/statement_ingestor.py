import csv
import sys

from src.data.config import STATEMENTS_ME_DIR
from src.service.portfolio.transaction.statement_rule_engine import apply_rules
from src.service.util.csv_util import non_comment_lines
from src.service.util.date_util import parse_date


def create_transaction(
    statement_type, who, date, remark, from_value, to_value, net_amount
):
    context = {
        "statement_type": statement_type.lower(),
        "who": who.lower(),
        "remark": remark.lower(),
        "net_amount": net_amount,
    }
    rule_output = apply_rules(context)

    from_account = rule_output.get("from_account", "")
    to_account = rule_output.get("to_account", "")

    return {
        "DATE(YYYY-MM-DD)": date,
        "TRANSACTION_REMARK": remark,
        "FROM_ACCOUNT": from_account,
        "FROM_VALUE": from_value,
        "TO_ACCOUNT": to_account,
        "TO_VALUE": to_value,
        "ADJUSTMENT_ACCOUNT": "",
        "ADJUSTMENT_VALUE": "",
        "LOT_SELECTION_METHOD": "",
    }


def ingest_is_statements(statement_type, who):
    new_transactions = []

    for file in STATEMENTS_ME_DIR.glob(f"{statement_type}.csv"):
        with file.open(encoding="utf-8") as f:
            reader = csv.DictReader(non_comment_lines(f), delimiter="\t")

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


def ingest_statements():
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
    is_transactions = ingest_is_statements(statement_type="is", who="me")
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()

    for tx in is_transactions:
        writer.writerow(tx)


if __name__ == "__main__":
    ingest_statements()

import json
import sys

from src.data.config import (
    LEDGER_ME_DIR,
    LEDGER_MOM_DIR,
    TRANSACTION_ME_DIR,
    TRANSACTION_MOM_DIR,
)


def json_to_ledger(json_file, output_file):
    with open(json_file, "r", encoding="utf-8") as f:
        txns = json.load(f)

    lines = []
    for t in txns:
        lines.append(f"{t['date']} {t['description']}".rstrip())
        for p in t["postings"]:
            if "amount" in p:
                lines.append(
                    f"    {p['account']:<50}{p['amount']:>12} {p['commodity']}"
                )
            else:
                lines.append(f"    {p['account']}")
        lines.append("")

    if lines and lines[-1] == "":
        lines.pop()

    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    try:
        json_to_ledger(
            f"{TRANSACTION_ME_DIR}/2020.json", f"{LEDGER_ME_DIR}/2020.ledger"
        )
        json_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2020.json", f"{LEDGER_MOM_DIR}/2020.ledger"
        )
        # json_to_ledger(
        #     f"{TRANSACTION_PAPA_DIR}/2020.json", f"{LEDGER_PAPA_DIR}/2020.ledger"
        # )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

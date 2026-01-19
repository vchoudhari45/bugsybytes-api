import sys

from src.data.config import (
    LEDGER_ME_DIR,
    LEDGER_MOM_DIR,
    LEDGER_PAPA_DIR,
    TRANSACTION_ME_DIR,
    TRANSACTION_MOM_DIR,
    TRANSACTION_PAPA_DIR,
)


def csv_to_ledger(csv_file, output_file):
    """
    Read pipe-separated CSV in format:
    DATE(YYYY-MM-DD)|TRANSACTION_REMARK|FROM_ACCOUNT|FROM_VALUE|TO_ACCOUNT|TO_VALUE
    and convert to ledger-cli format.
    """
    lines = []

    with open(csv_file, "r", encoding="utf-8") as f:
        next(f, None)  # skip header

        for row in f:
            row = row.strip()
            if not row:
                continue

            parts = row.split(",")
            if len(parts) != 8:
                print(f"Malformed row: {row}", file=sys.stderr)
                sys.exit(1)

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

            # Ledger-cli first line: date + description
            lines.append(f"{date} {description}".rstrip())

            # Indented postings
            lines.append(f"    {from_account:<50}{from_value}")
            lines.append(f"    {to_account:<50}{to_value}")

            # if adjustment_account exists add third entry
            if adjustment_account and adjustment_account.strip():
                if adjustment_value and adjustment_value.strip():
                    lines.append(f"    {adjustment_account:<50}{adjustment_value}")
                else:
                    lines.append(f"    {adjustment_account:<50}")

            # Empty line between transactions
            lines.append("")

    # Remove last empty line if present
    if lines and lines[-1] == "":
        lines.pop()

    # Write ledger file
    with open(output_file, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))


if __name__ == "__main__":
    try:
        # 2020
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2020.csv", f"{LEDGER_ME_DIR}/2020.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2020.csv", f"{LEDGER_MOM_DIR}/2020.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2020.csv", f"{LEDGER_PAPA_DIR}/2020.ledger"
        )

        # 2021
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2021.csv", f"{LEDGER_ME_DIR}/2021.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2021.csv", f"{LEDGER_MOM_DIR}/2021.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2021.csv", f"{LEDGER_PAPA_DIR}/2021.ledger"
        )

        # 2022
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2022.csv", f"{LEDGER_ME_DIR}/2022.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2022.csv", f"{LEDGER_MOM_DIR}/2022.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2022.csv", f"{LEDGER_PAPA_DIR}/2022.ledger"
        )

        # 2023
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2023.csv", f"{LEDGER_ME_DIR}/2023.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2023.csv", f"{LEDGER_MOM_DIR}/2023.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2023.csv", f"{LEDGER_PAPA_DIR}/2023.ledger"
        )

        # 2024
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2024.csv", f"{LEDGER_ME_DIR}/2024.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2024.csv", f"{LEDGER_MOM_DIR}/2024.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2024.csv", f"{LEDGER_PAPA_DIR}/2024.ledger"
        )

        # 2025
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2025.csv", f"{LEDGER_ME_DIR}/2025.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2025.csv", f"{LEDGER_MOM_DIR}/2025.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2025.csv", f"{LEDGER_PAPA_DIR}/2025.ledger"
        )

        # 2026
        csv_to_ledger(f"{TRANSACTION_ME_DIR}/2026.csv", f"{LEDGER_ME_DIR}/2026.ledger")
        csv_to_ledger(
            f"{TRANSACTION_MOM_DIR}/2026.csv", f"{LEDGER_MOM_DIR}/2026.ledger"
        )
        csv_to_ledger(
            f"{TRANSACTION_PAPA_DIR}/2026.csv", f"{LEDGER_PAPA_DIR}/2026.ledger"
        )
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(2)

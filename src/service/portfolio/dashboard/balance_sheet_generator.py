import xlsxwriter
import yaml

from src.data.config import (
    DASHBOARD_CONFIG_PATH,
    LEDGER_ME_MAIN,
    LEDGER_MOM_MAIN,
    LEDGER_PAPA_MAIN,
    PORTFOLIO_DASHBOARD_FILEPATH,
)
from src.service.portfolio.dashboard.ledger_cli_output_parser import (
    get_ledger_cli_output,
    parse_ledger_cli_output,
)

if __name__ == "__main__":
    # ---------------- Load config ----------------
    with open(DASHBOARD_CONFIG_PATH, "r") as f:
        config = yaml.safe_load(f)

    ledger_files = {
        LEDGER_ME_MAIN,
        LEDGER_MOM_MAIN,
        LEDGER_PAPA_MAIN,
    }

    cmd = config["dashboard"]["balance_sheet"]["command"].split()

    end_date = config["dashboard"]["balance_sheet"].get("end_date")
    if end_date:
        cmd.extend(["--end", end_date])

    for path in ledger_files:
        cmd.extend(["-f", str(path)])

    # ---------------- Ledger output ----------------
    output = get_ledger_cli_output(cmd, ledger_files)
    all_accounts = parse_ledger_cli_output(output)

    accounts = [p["account"] for p in all_accounts]
    account_set = set(accounts)

    filter_account = config["dashboard"]["balance_sheet"].get("filter", [])

    leaf_accounts = []
    for p in all_accounts:
        prefix = p["account"] + ":"
        if (
            not any(a.startswith(prefix) for a in account_set)
            and p["account"] not in filter_account
        ):
            leaf_accounts.append(p)

    # ---------------- Workbook ----------------
    workbook = xlsxwriter.Workbook(PORTFOLIO_DASHBOARD_FILEPATH)

    title_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 18,
            "align": "center",
            "valign": "vcenter",
        }
    )
    header_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 13,
            "bottom": 1,
        }
    )
    account_fmt = workbook.add_format({"indent": 1})
    amount_fmt = workbook.add_format(
        {
            "num_format": "#,##0.00",
            "align": "right",
        }
    )
    total_label_fmt = workbook.add_format(
        {
            "bold": True,
            "font_size": 13,
            "top": 1,
        }
    )
    total_fmt = workbook.add_format(
        {
            "bold": True,
            "top": 1,
            "num_format": "#,##0.00",
            "align": "right",
        }
    )

    worksheet = workbook.add_worksheet("Balance Sheet")

    worksheet.hide_gridlines(2)
    worksheet.set_column("A:A", 55)
    worksheet.set_column("B:B", 20)
    worksheet.set_column("D:D", 55)
    worksheet.set_column("E:E", 20)

    worksheet.merge_range("A1:E1", "Balance Sheet", title_fmt)

    worksheet.write("A3", "Assets", header_fmt)
    worksheet.write("B3", "Amount", header_fmt)
    worksheet.write("D3", "Liabilities", header_fmt)
    worksheet.write("E3", "Amount", header_fmt)

    START_ROW = 4
    GAP = 3

    asset_row = START_ROW
    liability_row = START_ROW

    total_assets = 0.0
    total_liabilities = 0.0
    total_equity = 0.0
    total_income = 0.0
    total_expenses = 0.0

    # ---------------- Assets & Liabilities ----------------
    for entry in leaf_accounts:
        account = entry["account"]
        amount = entry["amount"]

        if account.startswith("Assets"):
            worksheet.write(asset_row, 0, account, account_fmt)
            worksheet.write(asset_row, 1, amount, amount_fmt)
            total_assets += amount
            asset_row += 1

        elif account.startswith("Liabilities"):
            worksheet.write(liability_row, 3, account, account_fmt)
            worksheet.write(liability_row, 4, amount, amount_fmt)
            total_liabilities += amount
            liability_row += 1

        elif account.startswith("Equity"):
            total_equity += amount

        elif account.startswith("Income"):
            total_income += amount

        elif account.startswith("Expenses"):
            total_expenses += amount

    # ---------------- Total Liabilities ----------------
    liability_row += GAP
    worksheet.write(liability_row, 3, "Total Liabilities", total_label_fmt)
    worksheet.write(liability_row, 4, total_liabilities, total_fmt)

    # ---------------- Earnings ----------------
    earnings = abs(total_income) - total_expenses

    # ---------------- Equity ----------------
    equity_header_row = liability_row + GAP + 1
    equity_row = equity_header_row + 1

    worksheet.write(equity_header_row, 3, "Equity", header_fmt)
    worksheet.write(equity_header_row, 4, "Amount", header_fmt)

    for entry in leaf_accounts:
        account = entry["account"]
        amount = entry["amount"]

        if account.startswith("Equity"):
            worksheet.write(equity_row, 3, account, account_fmt)
            worksheet.write(equity_row, 4, amount, amount_fmt)
            equity_row += 1

    # --- Current Year Earnings ---
    worksheet.write(equity_row, 3, f"Equity:Earnings:YearEnd:{end_date}", account_fmt)
    worksheet.write(equity_row, 4, earnings, amount_fmt)
    equity_row += 1

    adjusted_equity = total_equity + earnings

    # ---------------- Total Equity ----------------
    equity_row += GAP
    worksheet.write(equity_row, 3, "Total Equity", total_label_fmt)
    worksheet.write(equity_row, 4, adjusted_equity, total_fmt)

    # ---------------- Final Totals (same row) ----------------
    final_row = max(asset_row, equity_row) + GAP

    worksheet.write(final_row, 0, "Total Assets", total_label_fmt)
    worksheet.write(final_row, 1, total_assets, total_fmt)

    worksheet.write(final_row, 3, "Total Liabilities + Equity", total_label_fmt)
    worksheet.write(
        final_row,
        4,
        total_liabilities + adjusted_equity,
        total_fmt,
    )

    workbook.close()

    print(f"✅ Balance sheet written to {PORTFOLIO_DASHBOARD_FILEPATH}")

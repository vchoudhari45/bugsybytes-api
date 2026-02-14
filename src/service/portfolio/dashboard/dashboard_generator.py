import sys

import xlsxwriter
import yaml

from src.data.config import (
    DASHBOARD_CONFIG_PATH,
    DASHBOARD_LAYOUT_CONFIG_PATH,
    LEDGER_ME_MAIN,
    LEDGER_MOM_MAIN,
    LEDGER_PAPA_MAIN,
    PORTFOLIO_DASHBOARD_FILEPATH,
)
from src.service.portfolio.dashboard.account_performance import (
    calculate_account_metrics,
)
from src.service.portfolio.dashboard.retirement_tracker import (
    generate_retirement_tracker,
)
from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)


# Helpers
def is_zero_account(account, zero_list):
    return any(account.startswith(z) for z in zero_list)


def sum_accounts(data, prefix, zero_list):
    return sum(
        entry["amount"]
        for entry in data
        if entry["account"].startswith(prefix)
        and not is_zero_account(entry["account"], zero_list)
    )


def filter_accounts(data, prefix, zero_list):
    return [
        entry
        for entry in data
        if entry["account"].startswith(prefix)
        and not is_zero_account(entry["account"], zero_list)
    ]


def calculate_investment_allocation(
    balance_sheet_data,
    categories_mapping,
    zero_balance_account_config,
):
    allocation_totals = {name: 0 for name in categories_mapping.keys()}
    allocation_totals["Other"] = 0
    total_investment = 0

    for entry in balance_sheet_data:
        account = entry["account"]
        amount = entry["amount"]

        if is_zero_account(account, zero_balance_account_config):
            continue

        if not account.startswith("Assets"):
            continue

        matched = False
        for name, prefix in categories_mapping.items():
            if account.startswith(prefix):
                allocation_totals[name] += amount
                matched = True
                break

        if not matched:
            allocation_totals["Other"] += amount

        total_investment += amount

    allocation_data = []
    for name, amount in allocation_totals.items():
        if amount == 0:
            continue

        allocation_data.append(
            {
                "Category": name,
                "Amount": amount,
                "%": (amount / total_investment) if total_investment else 0,
            }
        )

    return allocation_data


def print_table(
    worksheet, workbook, layout, title, data, start_row, start_col, sort=True
):
    if start_col is None:
        start_col = 0

    if start_row is None:
        start_row = 0

    if not data:
        return start_row, 0

    # Extract headers
    headers = list(data[0].keys())

    # Sort automatically if Amount exists
    if sort and "Amount" in headers:
        data = sorted(data, key=lambda x: x.get("Amount", 0), reverse=True)

    # Detect percent column
    percent_col = headers.index("%") if "%" in headers else None

    # worksheet.write(start_row, start_col, title, layout["section_title_fmt"])
    row = start_row

    # Write headers
    for col, header in enumerate(headers):
        worksheet.write(row, start_col + col, header, layout["header_fmt"])
    row += 1

    percent_fmt = (
        workbook.add_format({"num_format": "0.00%"})
        if percent_col is not None
        else None
    )

    # Write rows
    for entry in data:
        for col, header in enumerate(headers):
            value = entry.get(header, "")

            if percent_col is not None and col == percent_col:
                worksheet.write(row, start_col + col, value, percent_fmt)
            elif header == "Amount":
                worksheet.write(row, start_col + col, value, layout["amount_fmt"])
            else:
                worksheet.write(row, start_col + col, value, layout["account_fmt"])

        row += 1

    row += 1

    width_used = len(headers)

    return row, width_used


# main
if __name__ == "__main__":

    with open(DASHBOARD_CONFIG_PATH, "r") as f:
        dashboard_config = yaml.safe_load(f)

    with open(DASHBOARD_LAYOUT_CONFIG_PATH, "r") as f:
        dashboard_layout_config = yaml.safe_load(f)

    zero_balance_account_config = dashboard_config["dashboard"]["zero_balance_accounts"]
    categories = dashboard_config["dashboard"]["categories"]
    individual_xirr_reports = dashboard_config["dashboard"]["individual_xirr_reports"]
    retirement_tracker_config = dashboard_config["dashboard"]["retirement_tracker"]

    ledger_files = {LEDGER_ME_MAIN, LEDGER_MOM_MAIN, LEDGER_PAPA_MAIN}

    # Fetch data
    balance_sheet_data = get_ledger_cli_output_by_config(
        dashboard_config["dashboard"]["balance_sheet"],
        ledger_files,
    )

    income_statement_data = get_ledger_cli_output_by_config(
        dashboard_config["dashboard"]["income_statement"],
        ledger_files,
    )

    workbook = xlsxwriter.Workbook(PORTFOLIO_DASHBOARD_FILEPATH)

    layout = {
        name: workbook.add_format(props)
        for name, props in dashboard_layout_config.items()
    }

    worksheet = workbook.add_worksheet("Dashboard")
    worksheet.hide_gridlines(2)

    worksheet.write(0, 0, "Portfolio Dashboard", layout["title_fmt"])
    base_row = 2

    # Metrics
    assets = sum_accounts(balance_sheet_data, "Assets", zero_balance_account_config)
    liabilities = sum_accounts(
        balance_sheet_data, "Liabilities", zero_balance_account_config
    )
    liquid_cash = sum_accounts(
        balance_sheet_data, "Assets:Bank", zero_balance_account_config
    )
    income = sum_accounts(income_statement_data, "Income", zero_balance_account_config)
    expenses = sum_accounts(
        income_statement_data, "Expenses", zero_balance_account_config
    )

    summary_data = [
        {"Metric": "Net Worth", "Amount": assets - liabilities},
        {"Metric": "Assets", "Amount": assets},
        {"Metric": "Liabilities", "Amount": liabilities},
        {"Metric": "Liquid Cash", "Amount": liquid_cash},
        {"Metric": "Cashflow (Current Period)", "Amount": income - expenses},
    ]

    allocation_data = calculate_investment_allocation(
        balance_sheet_data,
        categories,
        zero_balance_account_config,
    )

    # Create Retirement Tracker
    generate_retirement_tracker(retirement_tracker_config)

    # Render first row
    col_gap = 1
    current_row = base_row

    # Summary
    end_row_left, width_left = print_table(
        worksheet=worksheet,
        workbook=workbook,
        layout=layout,
        title="Summary",
        data=summary_data,
        start_row=current_row,
        start_col=0,
        sort=False,
    )

    # Allocation
    end_row_right, width_right = print_table(
        worksheet=worksheet,
        workbook=workbook,
        layout=layout,
        title="Investment Allocation",
        data=allocation_data,
        start_row=current_row,
        start_col=width_left + col_gap,
    )

    # Next row starts below the tallest of the two
    current_row = max(end_row_left, end_row_right)

    # Render tables
    tables_in_row = 3
    current_col = 0
    tables_in_current_row = 0
    max_row_in_block = current_row

    remaining_tables = []

    for title, prefix in categories.items():
        data = filter_accounts(balance_sheet_data, prefix, zero_balance_account_config)
        if not data:
            continue

        remaining_tables.append(
            (title, [{"Account": e["account"], "Amount": e["amount"]} for e in data])
        )

    for title, data in remaining_tables:
        end_row, width = print_table(
            worksheet=worksheet,
            workbook=workbook,
            layout=layout,
            title=title,
            data=data,
            start_row=current_row,
            start_col=current_col,
        )

        max_row_in_block = max(max_row_in_block, end_row)

        current_col += width + col_gap
        tables_in_current_row += 1

        if tables_in_current_row == tables_in_row:
            current_row = max_row_in_block
            current_col = 0
            tables_in_current_row = 0

    for report in individual_xirr_reports:
        out = calculate_account_metrics(report, ledger_files)
        ws_xirr = workbook.add_worksheet(report["name"])
        ws_xirr.hide_gridlines(2)
        print_table(
            worksheet=ws_xirr,
            workbook=workbook,
            layout=layout,
            title=report["name"],
            data=out,
            start_row=0,
            start_col=0,
        )

    # Zero Balance Validation
    for zero_account in zero_balance_account_config:
        balance = sum(
            entry["amount"]
            for entry in balance_sheet_data
            if entry["account"].startswith(zero_account)
        )
        if abs(balance) > 0.01:
            print(f"❌ Zero balance validation failed for {zero_account}: {balance}")
            sys.exit(1)

    workbook.close()

    print(f"✅ Dashboard has been written to {PORTFOLIO_DASHBOARD_FILEPATH}")

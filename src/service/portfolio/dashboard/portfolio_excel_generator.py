import sys
from datetime import date, datetime

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
from src.service.portfolio.dashboard.account_metrics_data import (
    calculate_individual_xirr_report_data,
)
from src.service.portfolio.dashboard.dashboard_data import (
    calculate_category_tables_data,
    calculate_investment_allocation,
    calculate_summary_data,
)
from src.service.portfolio.dashboard.gsec_data import (
    calculate_gsec_individual_xirr_report_data,
)
from src.service.portfolio.dashboard.retirement_data import (
    calculate_retirement_data,
)
from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)

# Fields
amount_fields = [
    "INVESTMENT_AMOUNT",
    "CURRENT_VALUE",
    "ABSOLUTE RETURN",
    "XIRR",
    "DAYS SINCE FIRST INVESTMENT",
    "EPS",
    "PE",
    "MEDIAN PE",
    "AMOUNT",
    "INVESTMENT AMOUNT",
    "INFLATION ADJUSTED YEARLY EXPENSES",
    "INCOME",
    "TAX",
    "INVESTMENT AMOUNT FOR NEXT YEAR",
]

percent_fields = ["%", "XIRR", "ABSOLUTE RETURN"]

link_fields = ["NEWS_LINK"]


def print_table(
    worksheet, workbook, layout, title, data, start_row, start_col, sort=True
):
    # Default positions
    start_col = start_col or 0
    start_row = start_row or 0

    if not data:
        return start_row, 0

    # Normalize field names to uppercase for safe comparison
    amount_fields_upper = {field.upper() for field in amount_fields}
    percent_fields_upper = {field.upper() for field in percent_fields}
    link_fields_upper = {field.upper() for field in link_fields}

    # Extract headers
    headers = list(data[0].keys())

    row = start_row

    # Write headers
    for col, header in enumerate(headers):
        worksheet.write(row, start_col + col, header, layout["header_fmt"])
    row += 1

    # Write rows
    for entry in data:
        for col, header in enumerate(headers):
            value = entry.get(header, "")
            header_upper = header.upper()

            # Link fields
            if header_upper in link_fields_upper:
                worksheet.write_url(
                    row,
                    start_col + col,
                    value,
                    layout.get("link_fmt"),
                    string="View",
                )

            # Percent fields
            elif header_upper in percent_fields_upper:
                worksheet.write(row, start_col + col, value, layout["percent_fmt"])

            # Amount fields
            elif header_upper in amount_fields_upper:
                worksheet.write(row, start_col + col, value, layout["amount_fmt"])

            elif isinstance(value, (datetime, date)):
                worksheet.write_datetime(
                    row,
                    start_col + col,
                    datetime.combine(value, datetime.min.time()),
                    layout["date_fmt"],
                )

            # Default format
            else:
                worksheet.write(row, start_col + col, value, layout["account_fmt"])
        row += 1

    row += 1  # spacing after table
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
    individual_xirr_reports_config = dashboard_config["dashboard"][
        "individual_xirr_reports"
    ]
    gsec_individual_xirr_reports_config = dashboard_config["dashboard"][
        "gsec_individual_xirr_reports"
    ]
    retirement_tracker_config = dashboard_config["dashboard"]["retirement_tracker"]
    mutual_funds = dashboard_config["dashboard"]["mutual_funds"]
    ledger_files = {LEDGER_ME_MAIN, LEDGER_MOM_MAIN, LEDGER_PAPA_MAIN}

    # Balance Sheet data
    balance_sheet_data = get_ledger_cli_output_by_config(
        dashboard_config["dashboard"]["balance_sheet"],
        ledger_files,
    )

    # Income Statement Data
    income_statement_data = get_ledger_cli_output_by_config(
        dashboard_config["dashboard"]["income_statement"],
        ledger_files,
    )

    # Metrics & Summary Data
    summary_data = calculate_summary_data(
        balance_sheet_data=balance_sheet_data,
        income_statement_data=income_statement_data,
        zero_balance_account_config=zero_balance_account_config,
    )

    # Allocation Data
    allocation_data = calculate_investment_allocation(
        balance_sheet_data,
        categories,
        zero_balance_account_config,
    )

    # Account - Amount Table Data
    category_tables_data = calculate_category_tables_data(
        balance_sheet_data, categories, zero_balance_account_config
    )

    # Retirement Tracking Data
    retirement_tracking_data = calculate_retirement_data(retirement_tracker_config)

    # Individual XIRR Report Data
    individual_xirr_reports_data = calculate_individual_xirr_report_data(
        ledger_files, individual_xirr_reports_config, mutual_funds
    )

    # GSec Individual XIRR Report Data
    gsec_individual_xirr_reports_data = calculate_gsec_individual_xirr_report_data(
        ledger_files, gsec_individual_xirr_reports_config
    )

    # Generate Workbook
    workbook = xlsxwriter.Workbook(PORTFOLIO_DASHBOARD_FILEPATH)

    # Add layouts
    layout = {
        name: workbook.add_format(props)
        for name, props in dashboard_layout_config.items()
    }

    # Dashboard Worksheet
    worksheet = workbook.add_worksheet("Dashboard")
    worksheet.hide_gridlines(2)

    worksheet.write(0, 0, "Portfolio Dashboard", layout["title_fmt"])
    base_row = 2

    # Render first row
    col_gap = 1
    current_row = base_row

    # Render Summary
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

    # Render Allocation
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

    # Render Remaining tables
    tables_in_row = 3
    current_col = 0
    tables_in_current_row = 0
    max_row_in_block = current_row

    for title, data in category_tables_data:
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

    # Render individual XIRR report on seperate worksheet
    for report in individual_xirr_reports_data:
        report_name = report["name"]
        report_data = report["data"]
        ws_xirr = workbook.add_worksheet(report_name)
        ws_xirr.hide_gridlines(2)
        print_table(
            worksheet=ws_xirr,
            workbook=workbook,
            layout=layout,
            title=report_name,
            data=report_data,
            start_row=0,
            start_col=0,
        )

    # Render GSEC individual XIRR report on seperate worksheet
    for report in gsec_individual_xirr_reports_data:
        report_name = report["name"]
        cashflow_data = report["cashflow_data"]
        ws_cashflow = workbook.add_worksheet(report_name)
        print_table(
            worksheet=ws_cashflow,
            workbook=workbook,
            layout=layout,
            title=report_name,
            data=cashflow_data,
            start_row=0,
            start_col=0,
        )
        report_name_xirr = f"{report["name"]} XIRR Summary"
        xirr_data = report["xirr_data"]
        ws_xirr = workbook.add_worksheet(report_name_xirr)
        print_table(
            worksheet=ws_xirr,
            workbook=workbook,
            layout=layout,
            title=report_name_xirr,
            data=xirr_data,
            start_row=0,
            start_col=0,
        )

    # Render retirement tracking data
    ws_retirement = workbook.add_worksheet("Retirement")
    ws_retirement.hide_gridlines(2)
    print_table(
        worksheet=ws_retirement,
        workbook=workbook,
        layout=layout,
        title="Retirement",
        data=retirement_tracking_data,
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

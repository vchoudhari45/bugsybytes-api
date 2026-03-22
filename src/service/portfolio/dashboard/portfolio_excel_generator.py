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
    RED_BOLD,
    RESET,
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
    # dashboard
    "Amount",
    # retirement data fields
    "INVESTED AMOUNT",
    "INFLATION ADJUSTED YEARLY EXPENSES",
    "INCOME",
    "TAX",
    "INVESTMENT AMOUNT FOR NEXT YEAR",
    "BASE YEARLY EXPENSES",
    # account metrics data field
    "INVESTED",
    "QUANTITY",
    "AVERAGE COST",
    "MARKET VALUE",
    "REALIZED P&L",
    "UNREALIZED P&L",
    "TOTAL P&L",
    "DIVIDEND",
    "HOLDING DAYS",
    "NUMBER OF G-SECS",
    "EPS",
    "PE",
    "MEDIAN PE",
]

percent_fields = [
    # dashboard data fields
    "% Allocation",
    # gsec data fields
    "XIRR",
    # account metrics data field
    "ABSOLUTE RETURN",
    "CAGR",
    "XIRR",
    "INFLATION (%)",
    "RATE OF INTEREST (%)",
    "TAX (%)",
    "LATEST INDEX WEIGHTAGE",
]

link_fields = ["NEWS LINK"]


def print_kpi_cards(
    worksheet, layout, kpi_list, start_row, start_col=0, cards_per_row=4
):
    amount_fields_upper = {f.upper() for f in amount_fields}
    percent_fields_upper = {f.upper() for f in percent_fields}

    CARD_COLS = 2
    CARD_ROWS = 2
    col_gap = 1

    row = start_row
    col = start_col
    cards_in_row = 0
    max_row = start_row

    for item in kpi_list:
        label = item.get("KPI", "")
        value = item.get("VALUE", "")
        label_upper = label.upper()

        worksheet.merge_range(
            row, col, row, col + CARD_COLS - 1, label, layout["kpi_label_fmt"]
        )

        if label_upper in percent_fields_upper:
            val_fmt = "kpi_value_fmt_percent"
            display_val = value / 100 if isinstance(value, (int, float)) else value
        elif label_upper in amount_fields_upper or isinstance(value, (int, float)):
            val_fmt = "kpi_value_fmt_amount"
            display_val = value
        else:
            val_fmt = "kpi_value_fmt_text"
            display_val = value

        worksheet.merge_range(
            row + 1, col, row + 1, col + CARD_COLS - 1, display_val, layout[val_fmt]
        )
        max_row = max(max_row, row + CARD_ROWS)
        col += CARD_COLS + col_gap
        cards_in_row += 1

        if cards_in_row >= cards_per_row:
            row = max_row + 1
            col = start_col
            cards_in_row = 0

    return max_row + 2


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
    headers = []
    for entry in data:
        for key in entry.keys():
            if key != "_style" and key not in headers:  # ✅ skip _style
                headers.append(key)

    row = start_row

    # Write headers
    for col, header in enumerate(headers):
        worksheet.write(row, start_col + col, header, layout["header_fmt"])
    row += 1

    format_cache = {}

    # Write rows
    for entry in data:
        style_map = entry.get("_style", {})

        for col, header in enumerate(headers):
            value = entry.get(header, "")
            header_upper = header.upper()

            if value is None:
                base = layout["account_fmt"]
            elif header_upper in link_fields_upper:
                base = layout.get("link_fmt")
            elif header_upper in percent_fields_upper:
                base = layout["percent_fmt"]
            elif header_upper in amount_fields_upper:
                base = layout["amount_fmt"]
            elif isinstance(value, (datetime, date)):
                base = layout["date_fmt"]
            else:
                base = layout["account_fmt"]

            style = style_map.get(header)
            if style:
                key = (id(base), tuple(sorted(style.items())))
                if key not in format_cache:
                    props = getattr(base, "properties", {}).copy()
                    props.update(style)
                    format_cache[key] = workbook.add_format(props)
                fmt = format_cache[key]
            else:
                fmt = base

            # Handle None values safely
            if value is None:
                worksheet.write(row, start_col + col, "", fmt)
                continue

            # Link fields
            if header_upper in link_fields_upper:
                worksheet.write_url(
                    row,
                    start_col + col,
                    value,
                    fmt,
                    string="View",
                )

            # Percent fields
            elif header_upper in percent_fields_upper:
                worksheet.write(row, start_col + col, value * 100, fmt)

            elif isinstance(value, (datetime, date)):
                worksheet.write_datetime(
                    row,
                    start_col + col,
                    datetime.combine(value, datetime.min.time()),
                    fmt,
                )

            # Default format
            else:
                worksheet.write(row, start_col + col, value, fmt)
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

    zero_balance_accounts_config = (
        dashboard_config["dashboard"]["zero_balance_accounts"] or []
    )
    zero_balance_accounts_pending_config = (
        dashboard_config["dashboard"]["zero_balance_accounts_pending"] or []
    )
    categories = dashboard_config["dashboard"]["categories"]
    individual_xirr_reports_config = dashboard_config["dashboard"][
        "individual_xirr_reports"
    ]
    gsec_individual_xirr_reports_config = dashboard_config["dashboard"][
        "gsec_individual_xirr_reports"
    ]
    retirement_tracker_config = dashboard_config["dashboard"]["retirement_tracker"]
    mutual_funds = dashboard_config["dashboard"]["mutual_funds"]
    nifty_index_threshold = dashboard_config["dashboard"]["nifty_index"]["threshold"]
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

    # Zero Balance Validation
    print("\nValidating Zero Balance Accounts")
    for zero_account in zero_balance_accounts_config:
        balance = sum(
            entry["amount"]
            for entry in balance_sheet_data
            if entry["account"].startswith(zero_account)
        )

        print(f"Account: {zero_account} has balance of {balance}")
        if abs(balance) > 0.01:
            # If account is in pending list, ignore but show in red
            if zero_account in zero_balance_accounts_pending_config:
                print(
                    f"{RED_BOLD}Ignoring pending account {zero_account}: {balance}{RESET}"
                )
                continue

            # Otherwise, fail
            print(
                f"{RED_BOLD}❌ Zero balance validation failed for {zero_account}: {balance}{RESET}"
            )
            sys.exit(1)

    # Metrics & Summary Data
    summary_data = calculate_summary_data(
        balance_sheet_data=balance_sheet_data,
        income_statement_data=income_statement_data,
        zero_balance_accounts_config=zero_balance_accounts_config,
        mutual_funds=mutual_funds,
        ledger_files=ledger_files,
        stock_vs_bond_config=dashboard_config["dashboard"]["stock_vs_bond"],
        categories_threshold=dashboard_config["dashboard"]["categories_threshold"],
    )

    # Allocation Data
    allocation_data = calculate_investment_allocation(
        balance_sheet_data,
        categories,
        zero_balance_accounts_config,
    )

    # Account - Amount Table Data
    category_tables_data = calculate_category_tables_data(
        balance_sheet_data, categories, zero_balance_accounts_config
    )

    # Retirement Tracking Data
    retirement_tracking_data = calculate_retirement_data(retirement_tracker_config)

    # Individual XIRR Report Data
    recommended_stock = next(
        item["Amount"]
        for item in summary_data
        if item["Metric"] == "Recommended Stock Purchase"
    )
    individual_xirr_reports_data = calculate_individual_xirr_report_data(
        ledger_files,
        individual_xirr_reports_config,
        mutual_funds,
        recommended_stock,
        nifty_index_threshold,
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

    for item in category_tables_data:
        title = item["Category"]
        data = item["Entries"]
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
        report_type = report["type"]
        kpi_list = report["kpi_list"]

        if not report_data:
            continue
        ws_xirr = workbook.add_worksheet(report_name)

        if report_type == "Equity":
            kpi_end_row = print_kpi_cards(
                worksheet=ws_xirr,
                layout=layout,
                kpi_list=kpi_list,
                start_row=0,
                start_col=2,
                cards_per_row=4,
            )
            report_data.sort(key=lambda x: x.get("ABSOLUTE RETURN", 0), reverse=False)
            # ws_xirr.freeze_panes(13, 2)
        else:
            kpi_end_row = print_kpi_cards(
                worksheet=ws_xirr,
                layout=layout,
                kpi_list=kpi_list,
                start_row=0,
                start_col=2,
                cards_per_row=3,
            )
            report_data.sort(key=lambda x: x.get("XIRR", 0), reverse=False)
            # ws_xirr.freeze_panes(11, 2)

        print_table(
            worksheet=ws_xirr,
            workbook=workbook,
            layout=layout,
            title=report_name,
            data=report_data,
            start_row=kpi_end_row,
            start_col=0,
        )

    # Render GSEC individual XIRR report on seperate worksheet
    for report in gsec_individual_xirr_reports_data:
        report_name = report["name"]
        cashflow_data = report["cashflow_data"]

        if not cashflow_data:
            continue

        # Cashflow Sheet
        ws_cashflow = workbook.add_worksheet(report_name)
        ws_cashflow.freeze_panes(1, 1)
        print_table(
            worksheet=ws_cashflow,
            workbook=workbook,
            layout=layout,
            title=report_name,
            data=cashflow_data,
            start_row=0,
            start_col=0,
        )

        # Summary Sheet
        report_name_xirr = f"{report['name']} Summary"
        xirr_data = report["xirr_data"]
        kpi_list = report["kpi_list"]
        ws_xirr = workbook.add_worksheet(report_name_xirr)

        kpi_end_row = print_kpi_cards(
            worksheet=ws_xirr,
            layout=layout,
            kpi_list=kpi_list,
            start_row=0,
            start_col=0,
            cards_per_row=3,
        )
        print_table(
            worksheet=ws_xirr,
            workbook=workbook,
            layout=layout,
            title=report_name_xirr,
            data=xirr_data,
            start_row=kpi_end_row,
            start_col=0,
        )

    # Render retirement tracking data
    ws_retirement = workbook.add_worksheet("Retirement")
    kpi_list = [
        {
            "KPI": "RETIREMENT YEAR",
            "VALUE": int(retirement_tracker_config["retirement_year"]),
        },
        {"KPI": "END YEAR", "VALUE": int(retirement_tracker_config["end_year"])},
        {
            "KPI": "INFLATION (%)",
            "VALUE": float(retirement_tracker_config["inflation"]),
        },
        {
            "KPI": "RATE OF INTEREST (%)",
            "VALUE": float(retirement_tracker_config["rate_of_interest"]),
        },
        {"KPI": "TAX (%)", "VALUE": float(retirement_tracker_config["tax"])},
        {
            "KPI": "BASE YEARLY EXPENSES",
            "VALUE": float(retirement_tracker_config["yearly_expenses"]),
        },
        {
            "KPI": "INVESTED AMOUNT",
            "VALUE": float(retirement_tracker_config["investment_amount"]),
        },
    ]
    kpi_end_row = print_kpi_cards(
        worksheet=ws_retirement,
        layout=layout,
        kpi_list=kpi_list,
        start_row=0,
        start_col=1,
        cards_per_row=3,
    )
    print_table(
        worksheet=ws_retirement,
        workbook=workbook,
        layout=layout,
        title="Retirement Projection",
        data=retirement_tracking_data,
        start_row=kpi_end_row,
        start_col=0,
    )
    # Freeze after headers of projection table
    ws_retirement.freeze_panes(kpi_end_row + 1, 1)

    workbook.close()

    print(f"✅ Dashboard has been written to {PORTFOLIO_DASHBOARD_FILEPATH}")

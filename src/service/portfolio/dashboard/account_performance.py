import sys
from datetime import datetime

from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)
from src.service.util.xirr_calculator import xirr


def calculate_account_xirr(report, ledger_files):
    today = datetime.today()
    # Get all the commodities
    commodities = get_ledger_cli_output_by_config(
        report["list_commodity"], ledger_files, None, "commodities"
    )
    # For each of that commodity get cashflow for that commodity
    xirr_output = []
    for commodity in commodities:
        if commodity == "INR":
            continue
        # Get cashflow for each commodity using ledger register command
        cashflow_data = get_ledger_cli_output_by_config(
            report["register"], ledger_files, commodity, "register"
        )
        # Get current value of each commodity using ledger balance command
        current_value = get_ledger_cli_output_by_config(
            report["balance"], ledger_files, commodity, "balance"
        )
        if current_value and len(current_value) > 1:
            print("Can't have multiple values for single commodity ")
            sys.exit(1)

        investment_amount_current_value = 0
        if not current_value:
            cashflow_data.append({"date": today, "amount": 0})
        else:
            investment_amount_current_value = current_value[0]["amount"]
            cashflow_data.append(
                {"date": today, "amount": investment_amount_current_value}
            )

        cashflow_dates = []
        cashflow = []
        investment_amount = 0
        for entry in cashflow_data:
            date_value = entry["date"]
            if isinstance(date_value, datetime):
                date_obj = date_value.date()
            elif isinstance(date_value, str):
                date_obj = datetime.strptime(date_value, "%y-%b-%d").date()
            else:
                raise ValueError(f"Unsupported date type: {type(date_value)}")

            cashflow_dates.append(date_obj)
            cashflow.append(float(entry["amount"]))
            investment_amount += float(entry["amount"])

        # Calculate XIRR
        min_date = min(cashflow_dates)
        max_date = max(cashflow_dates)
        number_of_days_since_first_investment = (max_date - min_date).days
        if number_of_days_since_first_investment >= 365:
            xirr_value = xirr(cashflow_dates, cashflow)
        else:
            xirr_value = 0

        # Calculate absolute return
        abs_return = (
            (investment_amount_current_value - investment_amount) / investment_amount
        ) * 100

        # Append row
        xirr_output.append(
            {
                "SYMBOL": commodity,
                "INVESTMENT_AMOUNT": investment_amount,
                "CURRENT_VALUE": investment_amount_current_value,
                "ABSOLUTE RETURN": abs_return,
                "XIRR": xirr_value * 100,
            }
        )

    # Sort by SYMBOL
    xirr_output.sort(key=lambda x: x["SYMBOL"])

    return xirr_output

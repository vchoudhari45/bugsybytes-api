import concurrent.futures
import sys
from functools import partial

import pandas as pd
from sortedcontainers import SortedDict

from src.data.config import GSEC_DETAILS_FILE, QUANTITY_LAG_DAYS
from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)
from src.service.util.cashflow_generator import (
    apply_coupon_and_principal,
    empty_coupon_slot,
    generate_coupon_dates,
    market_shifted,
)
from src.service.util.date_util import parse_indian_date_format
from src.service.util.holiday_calculator import next_market_day
from src.service.util.xirr_calculator import xirr

gsec_maturity_date_override_df = pd.read_csv(GSEC_DETAILS_FILE)
gsec_maturity_date_override_df.columns = (
    gsec_maturity_date_override_df.columns.str.strip()
)
assert gsec_maturity_date_override_df["SYMBOL"].is_unique
gsec_maturity_date_override_df = gsec_maturity_date_override_df.set_index("SYMBOL")


def normalize_date(date_value):
    date_obj = parse_indian_date_format(date_value)
    return next_market_day(date_obj, QUANTITY_LAG_DAYS)


def compute_for_commodity(commodity, report, ledger_files, account_name):
    register_data = get_ledger_cli_output_by_config(
        report["register"], ledger_files, commodity, "gsec_register"
    )

    # generate date and total running quantity
    cashflow_dates_and_quantity = SortedDict()
    total_investment = 0
    total_quantity = 0
    for entry in register_data:
        date_obj = normalize_date(entry["date"])
        quantity = float(entry["quantity"])
        amount = float(entry["amount"])

        if date_obj not in cashflow_dates_and_quantity:
            cashflow_dates_and_quantity[date_obj] = {
                "quantity": 0.0,
                "transaction_amount": 0.0,
            }
        cashflow_dates_and_quantity[date_obj]["quantity"] += quantity
        cashflow_dates_and_quantity[date_obj]["transaction_amount"] += -1 * amount
        total_investment += amount
        total_quantity += quantity

    if commodity not in gsec_maturity_date_override_df.index:
        print(f"G-Sec '{commodity}' not found in GSEC_DETAILS_FILE.")
        sys.exit(1)

    # generate coupon interest cashflow
    row = gsec_maturity_date_override_df.loc[commodity]
    coupon_rate = float(row["COUPON RATE"])
    maturity_date_str = row["MATURITY DATE"]
    maturity_date = parse_indian_date_format(maturity_date_str)
    coupon_frequency = float(row["COUPON FREQUENCY"])
    isin = row["ISIN"]
    face_value = float(row["FACE VALUE"])
    first_date = min(cashflow_dates_and_quantity.keys())
    for d in generate_coupon_dates(first_date, maturity_date, coupon_frequency):
        cashflow_dates_and_quantity.setdefault(d, empty_coupon_slot())

    cashflow_dates_and_quantity.setdefault(
        market_shifted(maturity_date),
        {"quantity": 0, "coupon_date": True, "maturity": True},
    )
    apply_coupon_and_principal(
        cashflow_dates_and_quantity, coupon_rate, coupon_frequency, face_value
    )

    cashflow_dates = list(cashflow_dates_and_quantity.keys())
    total_cashflows = [
        entry.get("total_cashflow", 0.0)
        for entry in cashflow_dates_and_quantity.values()
    ]

    xirr_value = xirr(dates=cashflow_dates, cashflows=total_cashflows)

    xirr_data = {
        "SYMBOL": commodity,
        "ISIN": isin,
        "XIRR": xirr_value,
        "INVESTMENT": total_investment,
        "QUANTITY": total_quantity,
        "MATURITY DATE": maturity_date,
    }
    return xirr_data, cashflow_dates_and_quantity


def calculate_gsec_individual_xirr_report_data(
    ledger_files, gsec_individual_xirr_reports_config
):
    gsec_individual_xirr_reports_data = []

    for report in gsec_individual_xirr_reports_config:
        results = generate_gsec_portfolio_df(ledger_files, report)

        # Build XIRR Summary
        xirr_rows = []
        for xirr_data, _ in results:
            xirr_rows.append(xirr_data)

        # Build Pivoted Cashflow Table
        all_dates = set()
        symbol_cashflow_map = {}

        for xirr_data, cashflow_data in results:
            symbol = xirr_data["SYMBOL"]
            symbol_cashflow_map[symbol] = {}

            for dt, entry in cashflow_data.items():
                cashflow_value = entry.get("total_cashflow", 0.0)
                symbol_cashflow_map[symbol][dt] = round(cashflow_value, 2)
                all_dates.add(dt)

        sorted_dates = sorted(all_dates)
        sorted_symbols = sorted(symbol_cashflow_map.keys())

        cashflow_rows = []

        for dt in sorted_dates:
            row = {"DATE": dt}

            payday = False
            for symbol in sorted_symbols:
                value = symbol_cashflow_map[symbol].get(dt, 0.0)
                row[symbol] = value

                # If any symbol pays positive cashflow → PAY DAY
                if value > 0:
                    payday = True

            row["PAY DAY"] = payday

            cashflow_rows.append(row)

        gsec_individual_xirr_reports_data.append(
            {
                "name": report["name"],
                "xirr_data": xirr_rows,
                "cashflow_data": cashflow_rows,
            }
        )

    return gsec_individual_xirr_reports_data


def generate_gsec_portfolio_df(ledger_files, report):
    # Get all the commodities
    commodities = get_ledger_cli_output_by_config(
        report["list_commodity"], ledger_files, None, "commodities"
    )

    # Parallelize computation for each commodity
    xirr_output = []
    if commodities:
        compute_func = partial(
            compute_for_commodity,
            report=report,
            ledger_files=ledger_files,
            account_name=report["account_name"],
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(compute_func, commodities)
            xirr_output = [r for r in results if r is not None]

    return xirr_output

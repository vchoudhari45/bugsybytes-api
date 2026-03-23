import concurrent.futures
import sys
from collections import Counter, defaultdict
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


pd.set_option("display.max_columns", None)
pd.set_option("display.max_rows", None)
pd.set_option("display.max_colwidth", None)
pd.set_option("display.width", None)


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
        "INVESTMENT AMOUNT": total_investment,
        "CURRENT QUANTITY": total_quantity,
        "MATURITY DATE": maturity_date,
    }
    return xirr_data, cashflow_dates_and_quantity


def calculate_gsec_kpi(xirr_data, portfolio_xirr_value):
    total_invested = round(sum(row.get("INVESTMENT AMOUNT", 0) for row in xirr_data), 2)
    number_of_gsecs = round(len(xirr_data), 2)
    kpi_list = [
        {"KPI": "INVESTED", "VALUE": total_invested},
        {"KPI": "NUMBER OF G-SECS", "VALUE": number_of_gsecs},
        {"KPI": "PORTFOLIO XIRR", "VALUE": portfolio_xirr_value * 100},
    ]
    return kpi_list


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
        portfolio_cashflow_map = defaultdict(float)

        for xirr_data, cashflow_data in results:
            symbol = xirr_data["SYMBOL"]
            symbol_cashflow_map[symbol] = {}

            for dt, entry in cashflow_data.items():
                cashflow_value = round(entry.get("total_cashflow", 0.0), 2)
                symbol_cashflow_map[symbol][dt] = cashflow_value
                all_dates.add(dt)
                portfolio_cashflow_map[dt] += cashflow_value

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

        portfolio_dates = []
        portfolio_amounts = []
        for dt in sorted(portfolio_cashflow_map.keys()):
            portfolio_dates.append(dt)
            portfolio_amounts.append(portfolio_cashflow_map[dt])

        portfolio_xirr_value = 0.0
        if portfolio_dates and portfolio_amounts and len(portfolio_dates) > 1:
            portfolio_xirr_value = xirr(
                dates=portfolio_dates,
                cashflows=portfolio_amounts,
            )

        # validate gsec coupons
        reconciled_cashflow_data = validate_gsec_coupons(
            cashflow_rows, ledger_files, report
        )
        gsec_individual_xirr_reports_data.append(
            {
                "name": report["name"],
                "xirr_data": xirr_rows,
                "cashflow_data": cashflow_rows,
                "kpi_list": calculate_gsec_kpi(
                    xirr_rows, round(portfolio_xirr_value, 2)
                ),
                "reconciled_cashflow_data": reconciled_cashflow_data,
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
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = executor.map(compute_func, commodities)
            xirr_output = [r for r in results if r is not None]

    return xirr_output


def validate_gsec_coupons(cashflow_rows, ledger_files, gsec_ci_validator, threshold=2):
    register_data = get_ledger_cli_output_by_config(
        gsec_ci_validator["validate"], ledger_files, None, "gsec_register"
    )

    def norm(x):
        return round(float(x), 2)

    # max_paid_date + register counter
    max_paid_date = None
    amount_counter = Counter()

    for entry in register_data:
        amt = norm(entry["amount"])
        amount_counter[amt] += 1

        entry_date = normalize_date(entry["date"])
        if max_paid_date is None or entry_date > max_paid_date:
            max_paid_date = entry_date

    # exceptions
    exceptions_set = set(gsec_ci_validator.get("validate_exceptions", []))

    # build cashflow list
    cashflow_list = []
    non_paid_cashflow_list = []
    for row in cashflow_rows:
        row_date = normalize_date(row["DATE"])

        for key, value in row.items():
            if key in ("DATE", "PAY DAY"):
                continue

            if isinstance(value, (int, float)) and value > 0:
                amt = norm(value)

                lookup_key = f"{row['DATE']}|{key}|{amt}"

                if lookup_key in exceptions_set:
                    continue

                beyond_max_date = max_paid_date and row_date > max_paid_date

                if beyond_max_date:
                    non_paid_cashflow_list.append(
                        {
                            "date": row["DATE"],
                            "field": key,
                            "amount": amt,
                        }
                    )
                else:
                    cashflow_list.append(
                        {
                            "date": row["DATE"],
                            "field": key,
                            "amount": amt,
                        }
                    )

    # sort
    cashflow_list.sort(key=lambda x: x["amount"])

    register_list = []
    for amt, cnt in amount_counter.items():
        register_list.extend([amt] * cnt)

    register_list.sort()

    # comparison
    max_len = max(len(cashflow_list), len(register_list))

    result = []
    all_match = True

    for i in range(max_len):
        cf = cashflow_list[i] if i < len(cashflow_list) else None
        reg = register_list[i] if i < len(register_list) else None

        is_match = False
        expected_amt = cf["amount"] if cf else None
        actual_amt = reg
        diff = (
            round(expected_amt - actual_amt, 2)
            if expected_amt is not None and actual_amt is not None
            else None
        )
        is_match = abs(diff) <= threshold if diff is not None else False

        if not is_match:
            all_match = False

        result.append(
            {
                "DATE": cf["date"] if cf else None,
                "GSEC": cf["field"] if cf else None,
                "EXPECTED CASHFLOW AMOUNT": expected_amt,
                "ACTUAL CASHFLOW AMOUNT": actual_amt,
                "DIFFERENCE": diff,
                "MATCH": is_match,
            }
        )

    # Add non-paid cashflow rows at the end with empty ACTUAL, DIFFERENCE, MATCH
    for r in non_paid_cashflow_list:
        result.append(
            {
                "DATE": r["date"],
                "GSEC": r["field"],
                "EXPECTED CASHFLOW AMOUNT": r["amount"],
                "ACTUAL CASHFLOW AMOUNT": "",
                "DIFFERENCE": "",
                "MATCH": "",
            }
        )

    # Sort by date and GSEC
    result.sort(key=lambda x: (str(x["DATE"]) if x["DATE"] else "", x["GSEC"] or ""))

    # mismatch handling
    if not all_match:
        print(
            "\n❌ Mismatch found GSec coupons reconciliation for "
            f"{gsec_ci_validator.get('name', '')}, till date: {max_paid_date}"
        )
    else:
        print(
            f"✅ All GSec coupons reconciled for {gsec_ci_validator['name']}, till date: {max_paid_date}"
        )

    return result

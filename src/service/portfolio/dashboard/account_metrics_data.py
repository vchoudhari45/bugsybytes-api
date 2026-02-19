import concurrent.futures
import sys
from datetime import datetime
from functools import partial

import requests

from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)
from src.service.util.date_util import parse_indian_date_format
from src.service.util.xirr_calculator import xirr

_AMFI_CACHE = None


def fetch_amfi_isin_scheme_map():
    """
    Fetch NAVAll.txt from AMFI and return dict:
    { ISIN -> Scheme Name }
    """

    global _AMFI_CACHE

    if _AMFI_CACHE is not None:
        return _AMFI_CACHE

    url = "https://portal.amfiindia.com/spages/NAVAll.txt"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    _AMFI_CACHE = {}

    for line in response.text.splitlines():
        # Skip headers, blank lines, section titles
        if not line or ";" not in line:
            continue

        parts = line.split(";")
        if len(parts) < 6:
            continue

        isin_growth = parts[1].strip()
        scheme_name = parts[3].strip()

        if isin_growth and isin_growth != "-":
            _AMFI_CACHE[isin_growth] = scheme_name

    return _AMFI_CACHE


def get_company_id(company_name):
    company_name = company_name.replace("-", " ")
    url = f"https://www.screener.in/api/company/search/?q={requests.utils.quote(company_name)}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        id = data[0]["id"]
        return id
    except Exception as e:
        print(f"Error while fetching company ID for '{company_name}': {e}")
        return None


def get_metrics(company_id):
    if not company_id:
        return {}
    url = (
        f"https://www.screener.in/api/company/{company_id}/chart/"
        "?q=Price+to+Earning-Median+PE-EPS"
        "&days=700"
    )
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        metrics = {}
        datasets = data.get("datasets", [])
        for ds in datasets:
            if ds.get("values"):
                val = ds["values"][-1][1]
                label = ds["label"]
                if label == "PE":
                    metrics["PE"] = val
                elif "Median PE" in label:
                    metrics["MEDIAN PE"] = val
                elif "TTM EPS" in label:
                    metrics["EPS"] = val
        return metrics
    except Exception:
        return {}


def get_google_finance_link(is_mf, google_finance_code, commodity):
    if is_mf:
        return f"https://www.google.com/finance/quote/{google_finance_code}:MUTF_IN"
    else:
        return f"https://www.google.com/finance/quote/{commodity}:NSE"


def find_fund_by_isin(mutual_funds, account_name, isin):
    for isin_key, fund in mutual_funds.get(account_name, {}).items():
        if isin.lower() == isin_key.lower():
            return fund
    return None


def compute_for_commodity(
    commodity, amfi_isin_map, report, ledger_files, today, mutual_funds, account_name
):
    display_name = amfi_isin_map.get(commodity, commodity)

    # Get cashflow for each commodity using ledger register command
    cashflow_data = get_ledger_cli_output_by_config(
        report["register"], ledger_files, commodity, "register"
    )
    # Get current value of each commodity using ledger balance command
    current_value = get_ledger_cli_output_by_config(
        report["balance"], ledger_files, commodity, "balance"
    )
    # Validate single current value
    if current_value and len(current_value) > 1:
        print("Error: Can't have multiple values for single commodity ")
        sys.exit(1)

    cashflow_dates = []
    cashflow = []
    investment_amount = 0.0
    dividend = 0.0
    for entry in cashflow_data:
        date_value = entry["date"]
        if isinstance(date_value, str):
            date_obj = parse_indian_date_format(date_value)
        else:
            date_obj = date_value.date()

        account = entry["account"].lower()
        amount = float(entry["amount"])

        # negate income amount
        if account.startswith("income"):
            amount = -amount

        # dividends
        if account.startswith("income:dividends"):
            dividend += abs(amount)
        else:
            investment_amount += amount

        # cashflow logic
        cashflow_dates.append(date_obj)
        cashflow.append(amount)

    # If no cashflows, skip safely
    if not cashflow_dates:
        return None

    # add investment_amount_current_value to cashflow
    investment_amount_current_value = (
        float(current_value[0]["amount"]) if current_value else 0.0
    )

    investment_amount = abs(investment_amount)

    # ignore commodities which don't have any investments anymore
    if investment_amount <= 1 or investment_amount_current_value <= 1:
        return None

    cashflow_dates.append(today)
    cashflow.append(investment_amount_current_value)

    # Calculate XIRR
    min_date = min(cashflow_dates)
    max_date = max(cashflow_dates)
    number_of_days_since_first_investment = (max_date - min_date).days
    if number_of_days_since_first_investment > 180:
        xirr_value = xirr(cashflow_dates, cashflow)
        # Add dividend and calculate XIRR with dividend
        if dividend > 0:
            cashflow_with_dividend = cashflow.copy()
            cashflow_with_dividend[-1] += dividend
            xirr_value_with_dividend = xirr(cashflow_dates, cashflow_with_dividend)
        else:
            xirr_value_with_dividend = xirr_value
    else:
        xirr_value = 0
        xirr_value_with_dividend = 0

    # Calculate absolute return
    abs_return = (
        investment_amount_current_value - investment_amount
    ) / investment_amount

    # Calculate absolute return with dividend
    abs_return_with_dividend = (
        (investment_amount_current_value + dividend) - investment_amount
    ) / investment_amount

    # Get metrics
    company_id = get_company_id(display_name)
    metrics = get_metrics(company_id)

    # Append row
    output = {"SYMBOL": commodity}

    # adding display name for mutual fund
    is_mf = display_name.lower() != commodity.lower()
    google_finance_code = ""
    if is_mf:
        output["NAME"] = display_name
        fund = find_fund_by_isin(mutual_funds, account_name, commodity)
        fl_number = fund.get("fl_number", "") if fund else ""
        google_finance_code = fund.get("google_finance_code", "") if fund else ""
        if fl_number is not None and str(fl_number).strip() != "":
            output["FL NUMBER"] = fl_number

    output.update(
        {
            "INVESTMENT_AMOUNT": round(investment_amount, 2),
            "CURRENT_VALUE": round(investment_amount_current_value, 2),
            "ABSOLUTE RETURN": round(abs_return, 2),
            "XIRR": round(xirr_value, 2),
            "DAYS SINCE FIRST INVESTMENT": number_of_days_since_first_investment,
        }
    )

    if not is_mf:
        output.update(
            {
                "DIVIDEND": dividend,
                "ABSOLUTE RETURN WITH DIVIDEND": round(abs_return_with_dividend, 2),
                "XIRR WITH DIVIDEND": round(xirr_value_with_dividend, 2),
            }
        )

    output.update(metrics)
    output.update(
        {"NEWS LINK": get_google_finance_link(is_mf, google_finance_code, commodity)}
    )

    return output


def calculate_individual_xirr_report_data(
    ledger_files, individual_xirr_reports_config, mutual_funds
):
    individual_xirr_reports_data = []
    for report in individual_xirr_reports_config:
        data = get_account_performance_metrics_data(report, ledger_files, mutual_funds)
        data.sort(key=lambda x: x.get("XIRR", 0), reverse=False)
        individual_xirr_reports_data.append(
            {
                "name": report["name"],
                "data": data,
            }
        )
    return individual_xirr_reports_data


def get_account_performance_metrics_data(report, ledger_files, mutual_funds):
    today = datetime.today().date()

    amfi_isin_map = fetch_amfi_isin_scheme_map()

    # Get all the commodities
    commodities = get_ledger_cli_output_by_config(
        report["list_commodity"], ledger_files, None, "commodities"
    )

    # Parallelize computation for each commodity
    xirr_output = []
    if commodities:
        compute_func = partial(
            compute_for_commodity,
            amfi_isin_map=amfi_isin_map,
            report=report,
            ledger_files=ledger_files,
            today=today,
            mutual_funds=mutual_funds,
            account_name=report["account_name"],
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(compute_func, commodities)
            xirr_output = [r for r in results if r is not None]

    # Sort by SYMBOL
    xirr_output.sort(key=lambda x: x["SYMBOL"])

    return xirr_output

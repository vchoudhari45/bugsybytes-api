import concurrent.futures
import sys
from datetime import datetime
from functools import partial

import requests

from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)
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


def compute_for_commodity(commodity, amfi_isin_map, report, ledger_files, today):
    display_name = amfi_isin_map.get(commodity, commodity)

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

    cashflow_dates = []
    cashflow = []
    investment_amount = 0.0
    for entry in cashflow_data:
        date_value = entry["date"]
        if isinstance(date_value, str):
            date_obj = datetime.strptime(date_value, "%y-%b-%d").date()
        else:
            date_obj = date_value.date()

        amount = float(entry["amount"])

        cashflow_dates.append(date_obj)
        cashflow.append(amount)
        investment_amount += amount

    # ignore investment which don't have any investments anymore
    investment_amount = abs(investment_amount)
    if investment_amount <= 5:
        return None

    # add investment_amount_current_value to cashflow
    investment_amount_current_value = (
        float(current_value[0]["amount"]) if current_value else 0.0
    )
    if investment_amount_current_value != 0:
        cashflow_dates.append(today)
        cashflow.append(investment_amount_current_value)

    # Calculate XIRR
    min_date = min(cashflow_dates)
    max_date = max(cashflow_dates)
    number_of_days_since_first_investment = (max_date - min_date).days
    if number_of_days_since_first_investment > 180:
        xirr_value = xirr(cashflow_dates, cashflow)
    else:
        xirr_value = 0

    # Calculate absolute return
    abs_return = (
        investment_amount_current_value - investment_amount
    ) / investment_amount

    # Get metrics
    company_id = get_company_id(display_name)
    metrics = get_metrics(company_id)

    # Append row
    output = {"SYMBOL": commodity}

    # adding display name for mutual fund
    if display_name.lower() != commodity.lower():
        output["NAME"] = display_name

    output.update(
        {
            "INVESTMENT_AMOUNT": round(investment_amount, 2),
            "CURRENT_VALUE": round(investment_amount_current_value, 2),
            "ABSOLUTE RETURN": round(abs_return, 2),
            "XIRR": round(xirr_value, 2),
            "DAYS SINCE FIRST INVESTMENT": number_of_days_since_first_investment,
        }
    )
    output.update(metrics)

    return output


def calculate_individual_xirr_report_data(ledger_files, individual_xirr_reports_config):
    individual_xirr_reports_data = []
    for report in individual_xirr_reports_config:
        data = get_account_performance_metrics_data(report, ledger_files)
        data.sort(key=lambda x: x.get("XIRR", 0), reverse=False)
        individual_xirr_reports_data.append(
            {
                "name": report["name"],
                "data": data,
            }
        )
    return individual_xirr_reports_data


def get_account_performance_metrics_data(report, ledger_files):
    today = datetime.today().date()

    amfi_isin_map = fetch_amfi_isin_scheme_map()

    # Get all the commodities
    commodities = get_ledger_cli_output_by_config(
        report["list_commodity"], ledger_files, None, "commodities"
    )

    # Filter commodities
    filtered_commodities = [c for c in commodities if c != "INR"]

    # Parallelize computation for each commodity
    xirr_output = []
    if filtered_commodities:
        compute_func = partial(
            compute_for_commodity,
            amfi_isin_map=amfi_isin_map,
            report=report,
            ledger_files=ledger_files,
            today=today,
        )
        with concurrent.futures.ThreadPoolExecutor() as executor:
            results = executor.map(compute_func, filtered_commodities)
            xirr_output = [r for r in results if r is not None]

    # Sort by SYMBOL
    xirr_output.sort(key=lambda x: x["SYMBOL"])

    return xirr_output

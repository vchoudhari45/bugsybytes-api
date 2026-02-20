import concurrent.futures
import sys
from datetime import date, datetime
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
    except Exception as e:
        print(f"Error while fetching company ID for '{company_id}': {e}")
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

    # Get income flow for each commodity
    income_flow = get_ledger_cli_output_by_config(
        report["register"]["income_flow"], ledger_files, commodity, "register"
    )
    # Get equity flow for each commodity
    equity_flow = get_ledger_cli_output_by_config(
        report["register"]["equity_flow"], ledger_files, commodity, "register"
    )
    combined_flow = equity_flow + income_flow
    combined_flow.sort(key=lambda x: parse_indian_date_format(x["date"]))

    # Get current value of each commodity using ledger balance command
    balance = get_ledger_cli_output_by_config(
        report["balance"], ledger_files, commodity, "balance"
    )
    # Validate single current value
    if balance and len(balance) > 1:
        print(
            "Error: A single commodity cannot have more than one balance value, Please check account_metrics_data.py"
        )
        sys.exit(1)

    total_inflow = 0.0
    total_invested = sys.float_info.min
    current_invested = 0.0
    current_quantity = 0.0
    realized_pl = 0.0
    dividend = 0.0
    first_investment_date = datetime.max.date()
    current_capital_first_investment_date = datetime.max.date()
    cashflow_dates = []
    cashflows = []
    current_cashflow_dates = []
    current_cashflows = []
    for entry in combined_flow:
        date_value = entry["date"]
        if isinstance(date_value, str):
            date_obj = parse_indian_date_format(date_value)
        else:
            date_obj = date_value.date()

        first_investment_date = min(first_investment_date, date_obj)
        account = entry["account"].lower()
        quantity = float(entry["quantity"])
        amount = float(entry["amount"])

        if account.startswith("income:dividends"):
            dividend += amount
        elif account.startswith("income:capitalgains"):
            cashflow_dates.append(date_obj)
            cashflows.append(amount)
            current_cashflow_dates.append(date_obj)
            current_cashflows.append(amount)
            realized_pl += amount
        else:
            cashflow_dates.append(date_obj)
            cashflows.append(amount)
            current_cashflow_dates.append(date_obj)
            current_cashflows.append(amount)
            current_invested += amount
            current_quantity += quantity
            if amount < 0:
                total_inflow += amount
            if current_quantity == 0:
                total_invested = max(total_invested, abs(total_inflow))
                total_inflow = 0.0
                current_cashflow_dates = []
                current_cashflows = []
                current_capital_first_investment_date = date_obj

    # handling for float issue
    if current_quantity <= 0.1 and current_invested <= 0.1:
        return None

    if current_capital_first_investment_date == datetime.max.date():
        current_capital_first_investment_date = first_investment_date

    # handling cases where quantity never becomes zero and total_invested is never assigned
    if total_invested == sys.float_info.min:
        total_invested = abs(total_inflow)

    # handling cases where current_invested > total_invested
    current_invested = abs(current_invested)
    total_invested = max(total_invested, current_invested)

    current_market_value = float(balance[0]["amount"]) if balance else 0.0

    average_cost = (
        round(current_invested / current_quantity, 2) if current_quantity else 0.0
    )
    unrealized_pl = round(current_market_value - current_invested, 2)
    total_pl = realized_pl + unrealized_pl

    absolute_return = (total_pl / total_invested) if total_invested != 0 else 0.0
    current_absolute_return = (
        unrealized_pl / current_invested if current_invested != 0 else 0.0
    )

    dividend_yield = (dividend / total_invested) if total_invested != 0 else 0.0

    holding_days = (date.today() - first_investment_date).days
    current_holding_days = (date.today() - current_capital_first_investment_date).days

    cagr = (1 + absolute_return) ** (365 / holding_days) - 1
    current_cagr = (1 + current_absolute_return) ** (365 / current_holding_days) - 1

    # add current market value
    cashflow_dates.append(date.today())
    current_cashflow_dates.append(date.today())
    cashflows.append(current_market_value)
    current_cashflows.append(current_market_value)

    print(
        f"Symbol: {commodity}",
        f"Cashflows Dates Overall: {[d.strftime('%Y-%m-%d') for d in cashflow_dates]}",
        f"Cashflows Overall: {cashflows}",
        f"Current Cashflows Dates: {[d.strftime('%Y-%m-%d') for d in current_cashflow_dates]}",
        f"Current Cashflow: {current_cashflows}",
        f"TOTAL INVESTED: {total_invested}",
        f"CURRENT INVESTED: {current_invested}",
        f"CURRENT QUANTITY: {current_quantity}",
        f"AVERAGE COST (OPEN POSITION): {average_cost}",
        f"CURRENT MARKET VALUE: {current_market_value}",
        f"REALIZED P&L: {realized_pl}",
        f"UNREALIZED P&L: {unrealized_pl}",
        f"DIVIDEND: {dividend}",
        f"ABSOLUTE RETURN (OVERALL): {absolute_return * 100}",
        f"CURRENT ABSOLUTE RETURN: {current_absolute_return * 100}",
        f"CAGR(OVERALL): {cagr * 100}",
        f"CURRENT CAGR: {current_cagr * 100}",
        f"DIVIDEND YIELD(OVERALL): {dividend_yield * 100}",
        f"HOLDING PERIOD(OVERALL): {holding_days}",
        f"CURRENT HOLDING PERIOD: {current_holding_days}",
        sep="\n",
    )

    xirr_value = xirr(dates=cashflow_dates, cashflows=cashflows)
    current_xirr_value = xirr(dates=current_cashflow_dates, cashflows=current_cashflows)

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
            "TOTAL INVESTED": total_invested,
            "CURRENT INVESTED": current_invested,
            "CURRENT QUANTITY": current_quantity,
            "AVERAGE COST (OPEN POSITION)": average_cost,
            "CURRENT MARKET VALUE": current_market_value,
            "REALIZED P&L": realized_pl,
            "UNREALIZED P&L": unrealized_pl,
            "DIVIDEND": dividend,
            "ABSOLUTE RETURN (OVERALL)": absolute_return,
            "CURRENT ABSOLUTE RETURN": current_absolute_return,
            "CAGR(OVERALL)": cagr,
            "CURRENT CAGR": current_cagr,
            "XIRR(OVERALL)": xirr_value,
            "CURRENT XIRR": current_xirr_value,
            "DIVIDEND YIELD(OVERALL)": dividend_yield,
            "HOLDING PERIOD(OVERALL)": holding_days,
            "CURRENT HOLDING PERIOD": current_holding_days,
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

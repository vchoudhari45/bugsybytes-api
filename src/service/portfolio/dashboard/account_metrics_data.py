import concurrent.futures
import sys
from datetime import datetime
from functools import partial

import requests

from src.service.portfolio.dashboard.nifty_index_data import fetch_nse_stocks
from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)
from src.service.util.date_util import parse_indian_date_format
from src.service.util.xirr_calculator import xirr

_AMFI_CACHE = None
_NIFTY_INDEX_CACHE = None


def fetch_nifty_index():
    global _NIFTY_INDEX_CACHE
    if _NIFTY_INDEX_CACHE is None:
        _NIFTY_INDEX_CACHE = fetch_nse_stocks()
    return _NIFTY_INDEX_CACHE


def fetch_amfi_isin_scheme_map(session):
    global _AMFI_CACHE

    if _AMFI_CACHE is not None:
        return _AMFI_CACHE

    url = "https://portal.amfiindia.com/spages/NAVAll.txt"
    response = session.get(url, timeout=30)
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


def get_company_id(company_name, session):
    company_name = company_name.replace("-", " ")
    url = f"https://www.screener.in/api/company/search/?q={requests.utils.quote(company_name)}"
    try:
        response = session.get(url, timeout=15)
        response.raise_for_status()
        data = response.json()
        if not data:
            return None
        id = data[0]["id"]
        return id
    except Exception as e:
        print(f"Error while fetching company ID for '{company_name}': {e}")
        return None


def get_metrics(company_id, session):
    if not company_id:
        return {}
    url = (
        f"https://www.screener.in/api/company/{company_id}/chart/"
        "?q=Price+to+Earning-Median+PE-EPS"
        "&days=700"
    )
    try:
        response = session.get(url, timeout=15)
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
    commodity,
    amfi_isin_map,
    nifty_index_data,
    report,
    ledger_files,
    today,
    mutual_funds,
    account_name,
    session,
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
            "Error: A single commodity cannot have more than one balance value, "
            "Please check account_metrics_data.py"
        )
        sys.exit(1)

    current_invested = 0.0
    current_quantity = 0.0
    realized_pl = 0.0
    dividend = 0.0
    first_investment_date = datetime.max.date()
    cashflow_dates = []
    cashflows = []
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
            realized_pl += amount
        else:
            cashflow_dates.append(date_obj)
            cashflows.append(amount)
            current_invested += amount
            current_quantity += quantity

    current_invested = abs(current_invested)
    current_market_value = float(balance[0]["amount"]) if balance else 0.0

    average_cost = (
        round(current_invested / current_quantity, 2) if current_invested > 1 else 0.0
    )
    unrealized_pl = round(current_market_value - current_invested, 2)
    total_pl = realized_pl + unrealized_pl + dividend

    current_absolute_return = (
        unrealized_pl / current_invested if current_invested > 1 else 0.0
    )

    current_holding_days = (
        (today - first_investment_date).days
        if first_investment_date != datetime.max.date()
        else 0
    )
    if current_holding_days > 0 and abs(current_absolute_return) > 0:
        current_cagr = (1 + current_absolute_return) ** (365 / current_holding_days) - 1
    else:
        current_cagr = 0.0

    if abs(current_market_value) > 0.1:
        cashflow_dates.append(today)
        cashflows.append(current_market_value)

    xirr_value = xirr(dates=cashflow_dates, cashflows=cashflows)

    is_mf = display_name.lower() != commodity.lower()

    # Get metrics
    metrics = {}
    if not is_mf:
        company_id = get_company_id(display_name, session)
        metrics = get_metrics(company_id, session)

    # Get Nifty Index Data
    index_data = nifty_index_data.get(commodity, {}) if not is_mf else {}

    # adding display name for mutual fund
    google_finance_code = ""
    fl_number = ""
    if is_mf:
        fund = find_fund_by_isin(mutual_funds, account_name, commodity)
        fl_number = fund.get("fl_number", "") if fund else ""
        google_finance_code = fund.get("google_finance_code", "") if fund else ""

    cashflow_date_strings = [d.strftime("%Y-%m-%d") for d in cashflow_dates]
    print(
        f"\nSYMBOL: {commodity}",
        f"DISPLAY NAME: {display_name}",
        f"CASHFLOW DATES: {cashflow_date_strings}",
        f"CASHFLOW: {cashflows}",
        f"CURRENT INVESTED: {current_invested}",
        f"CURRENT QUANTITY: {current_quantity}",
        f"AVERAGE COST: {average_cost}",
        f"CURRENT MARKET VALUE: {current_market_value}",
        f"REALIZED P&L: {realized_pl}",
        f"UNREALIZED P&L: {unrealized_pl}",
        f"TOTAL P&L: {total_pl}",
        f"DIVIDEND: {dividend}",
        f"CURRENT ABSOLUTE RETURN: {current_absolute_return * 100}",
        f"CURRENT CAGR: {current_cagr * 100}",
        f"XIRR: {xirr_value * 100}",
        f"CURRENT HOLDING DAYS: {current_holding_days}",
        f"METRICS: {metrics}",
        sep="\n",
    )

    output = {}

    # 1. SYMBOL
    output["SYMBOL"] = commodity

    # 2. MF-specific fields (must come right after SYMBOL)
    if is_mf:
        output["NAME"] = display_name
        if fl_number is not None and str(fl_number).strip():
            output["FL NUMBER"] = fl_number
    else:
        output["NAME"] = index_data.get("COMPANY NAME", "")
        output["NIFTY INDEX"] = index_data.get("NIFTY INDEX", "")

    # 3. Core fields
    output["INVESTED"] = current_invested
    output["QUANTITY"] = current_quantity
    output["AVERAGE COST"] = average_cost
    output["MARKET VALUE"] = current_market_value
    output["REALIZED P&L"] = realized_pl
    output["UNREALIZED P&L"] = unrealized_pl
    output["TOTAL P&L"] = total_pl
    output["DIVIDEND"] = dividend
    output["ABSOLUTE RETURN"] = current_absolute_return
    output["CAGR"] = current_cagr if current_holding_days > 180 else 0.0

    # 4. XIRR (must come right after CAGR)
    if is_mf:
        output["XIRR"] = xirr_value if current_holding_days > 180 else 0.0

    # 5. Holding days
    output["HOLDING DAYS"] = current_holding_days

    # 6. Metrics
    for key, value in metrics.items():
        output[key] = value

    # 7. Adding Nifty Index
    if not is_mf:
        output["NEAR 52W HIGH %"] = index_data.get("NEAR 52W HIGH %", 0.0)
        output["NEAR 52W LOW %"] = index_data.get("NEAR 52W LOW %", 0.0)
        output["PREV CLOSE"] = index_data.get("PREV CLOSE", 0.0)
        output["YEAR LOW"] = index_data.get("YEAR LOW", 0.0)
        output["YEAR HIGH"] = index_data.get("YEAR HIGH", 0.0)
        output["FREE FLOATING MARKET CAP"] = index_data.get(
            "FREE FLOATING MARKET CAP", 0.0
        )
        output["TARGET INDEX WEIGHTAGE"] = index_data.get("TARGET INDEX WEIGHTAGE", 0.0)

    # 8. News link
    output["NEWS LINK"] = get_google_finance_link(is_mf, google_finance_code, commodity)

    return output


def calculate_account_metrics_kpi(
    report_data, report_type, report_name, recommended_stock, nifty_index_threshold
):
    total_invested = round(sum(row.get("INVESTED", 0) for row in report_data), 2)
    total_market_value = round(
        sum(row.get("MARKET VALUE", 0) for row in report_data), 2
    )
    total_realized = round(sum(row.get("REALIZED P&L", 0) for row in report_data), 2)
    total_unrealized = round(
        sum(row.get("UNREALIZED P&L", 0) for row in report_data), 2
    )
    total_pl = round(sum(row.get("TOTAL P&L", 0) for row in report_data), 2)
    total_dividend = round(sum(row.get("DIVIDEND", 0) for row in report_data), 2)
    max_holding_days = round(max(row.get("HOLDING DAYS", 0) for row in report_data), 2)

    portfolio_absolute_return = 0.0
    if total_invested > 0:
        portfolio_absolute_return = (
            total_market_value - total_invested
        ) / total_invested

    portfolio_cagr = 0.0
    if total_invested > 0 and max_holding_days > 0:
        portfolio_cagr = (total_market_value / total_invested) ** (
            365 / max_holding_days
        ) - 1

    kpi_list = [
        {"KPI": "INVESTED", "VALUE": total_invested},
        {"KPI": "MARKET VALUE", "VALUE": total_market_value},
        {"KPI": "REALIZED P&L", "VALUE": total_realized},
        {"KPI": "UNREALIZED P&L", "VALUE": total_unrealized},
        {"KPI": "TOTAL P&L", "VALUE": total_pl},
        {"KPI": "DIVIDEND", "VALUE": total_dividend},
        {"KPI": "ABSOLUTE RETURN", "VALUE": round(portfolio_absolute_return * 100, 2)},
        {"KPI": "CAGR", "VALUE": round(portfolio_cagr * 100, 2)},
    ]

    # if it is equity report add nifty index % allocation
    if report_type == "Equity":
        index_market_value_totals = {}

        for row in report_data:
            index_name = row.get("NIFTY INDEX")
            current_value = row.get("MARKET VALUE", 0)
            if index_name:
                index_market_value_totals[index_name] = (
                    index_market_value_totals.get(index_name, 0) + current_value
                )

        for row in report_data:
            news_link = row.pop("NEWS LINK", None)

            # calculate buy and sell quantity based on index weightage
            current_value = row.get("MARKET VALUE", 0)
            price = row.get("PREV CLOSE", 0)
            benchmark_weight = row.get("TARGET INDEX WEIGHTAGE", 0)

            index_name = row.get("NIFTY INDEX")

            index_total_market_value = index_market_value_totals.get(index_name, 0)
            if index_total_market_value > 0:
                row["PORTFOLIO WEIGHT"] = round(
                    current_value / index_total_market_value, 6
                )
            else:
                row["PORTFOLIO WEIGHT"] = 0.0

            target_value = index_total_market_value * benchmark_weight
            difference = target_value - current_value

            rebalance_qty = 0.0
            if price > 0:
                rebalance_qty = difference / price

            row["BUY/SELL"] = rebalance_qty

            if news_link is not None:
                row["NEWS LINK"] = news_link

        # assign index allocation KPIs
        index_allocation_kpis = []
        for index_name, invested_amount in index_market_value_totals.items():
            if total_market_value > 0:
                allocation_percent = round(
                    (invested_amount / total_market_value) * 100, 2
                )
            else:
                allocation_percent = 0.0
            index_allocation_kpis.append(
                {
                    "KPI": f"{index_name} ALLOCATION",
                    "VALUE": f"{round(invested_amount, 2)}|{allocation_percent}",
                }
            )
        kpi_list.extend(index_allocation_kpis)

        if report_name == "Consolidated Equity" and recommended_stock > 0:
            current_total = total_market_value
            new_total = current_total + recommended_stock
            recommendation_kpis = []
            for index_name, target_weight in nifty_index_threshold.items():
                current_value = index_market_value_totals.get(index_name, 0)
                target_value = new_total * target_weight
                additional_investment = target_value - current_value
                recommendation_kpis.append(
                    {
                        "KPI": f"RECOMMENDED {index_name} PURCHASE",
                        "VALUE": round(additional_investment, 2),
                    }
                )
            kpi_list.extend(recommendation_kpis)

    return kpi_list


def calculate_individual_xirr_report_data(
    ledger_files,
    individual_xirr_reports_config,
    mutual_funds,
    recommended_stock,
    nifty_index_threshold,
):
    individual_xirr_reports_data = []
    for report in individual_xirr_reports_config:
        data = get_account_performance_metrics_data(report, ledger_files, mutual_funds)
        kpi_list = calculate_account_metrics_kpi(
            data,
            report["type"],
            report["name"],
            recommended_stock,
            nifty_index_threshold,
        )
        individual_xirr_reports_data.append(
            {
                "name": report["name"],
                "type": report["type"],
                "data": data,
                "kpi_list": kpi_list,
            }
        )
    return individual_xirr_reports_data


def get_account_performance_metrics_data(report, ledger_files, mutual_funds):
    today = datetime.today().date()
    session = requests.Session()
    amfi_isin_map = fetch_amfi_isin_scheme_map(session)
    nifty_index_data = fetch_nifty_index()

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
            nifty_index_data=nifty_index_data,
            report=report,
            ledger_files=ledger_files,
            today=today,
            mutual_funds=mutual_funds,
            account_name=report["account_name"],
            session=session,
        )
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            results = executor.map(compute_func, commodities)
            xirr_output = [r for r in results if r is not None]

    # Sort by SYMBOL
    xirr_output.sort(key=lambda x: x["SYMBOL"])

    return xirr_output

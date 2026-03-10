import sys

from src.data.config import RED_BOLD, RESET
from src.service.portfolio.ledger.ledger_cli_output_parser import (
    get_ledger_cli_output_by_config,
)


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


def calculate_category_tables_data(
    balance_sheet_data, categories, zero_balance_accounts_config
):
    category_tables_data = []
    for title, prefix in categories.items():
        data = filter_accounts(balance_sheet_data, prefix, zero_balance_accounts_config)
        if not data:
            continue

        sorted_data = sorted(
            [{"Account": e["account"], "Amount": e["amount"]} for e in data],
            key=lambda x: x["Amount"],
            reverse=True,
        )
        category_tables_data.append((title, sorted_data))

    return category_tables_data


def calculate_summary_data(
    balance_sheet_data,
    income_statement_data,
    zero_balance_accounts_config,
    mutual_funds,
    ledger_files,
    stock_vs_bond_config,
    categories_threshold,
):
    assets = sum_accounts(balance_sheet_data, "Assets", zero_balance_accounts_config)
    liabilities = sum_accounts(
        balance_sheet_data, "Liabilities", zero_balance_accounts_config
    )
    liquid_cash = sum_accounts(
        balance_sheet_data, "Assets:Bank", zero_balance_accounts_config
    )
    income = sum_accounts(income_statement_data, "Income", zero_balance_accounts_config)
    expenses = sum_accounts(
        income_statement_data, "Expenses", zero_balance_accounts_config
    )

    # stock vs bond total calculation logic
    stock_total = 0
    bond_total = 0
    gsec_balance = sum_accounts(
        balance_sheet_data, "Assets:Investments:GSec", zero_balance_accounts_config
    )
    stock_balance = sum_accounts(
        balance_sheet_data, "Assets:Investments:Equity", zero_balance_accounts_config
    )
    stock_total += stock_balance
    bond_total += gsec_balance
    mutual_funds_commodities_data = get_ledger_cli_output_by_config(
        config=stock_vs_bond_config["commodities"],
        ledger_files=ledger_files,
        commodity=None,
        command_type="commodities",
    )
    mutual_fund_config = mutual_funds["Assets:Investments:MutualFunds"]
    for commodity in mutual_funds_commodities_data:
        if commodity == "INR":
            continue
        balance_for_commodity = get_ledger_cli_output_by_config(
            config=stock_vs_bond_config["balance"],
            ledger_files=ledger_files,
            commodity=commodity,
            command_type="balance",
        )
        # Validate single current value
        if balance_for_commodity and len(balance_for_commodity) > 1:
            print(
                "Error: A single commodity cannot have more than one balance value, "
                "Please check dashboard_data.py"
            )
            sys.exit(1)

        stock_allocation = (
            mutual_fund_config[commodity]["approximate_allocation"]["equity"] / 100
        )
        bond_allocation = (
            mutual_fund_config[commodity]["approximate_allocation"]["debt"] / 100
        )
        balance = (
            float(balance_for_commodity[0]["amount"]) if balance_for_commodity else 0.0
        )
        stock_total += stock_allocation * balance
        bond_total += bond_allocation * balance

    total = stock_total + bond_total
    if total:
        stock_pct = stock_total / total
        bond_pct = bond_total / total
    else:
        stock_pct = 0
        bond_pct = 0

    suggested_equity = 0
    suggested_bond = 0
    cash_threshold = categories_threshold.get("Liquid Cash", 0)
    target_stock = categories_threshold.get("Target Stock", 0)

    if liquid_cash > cash_threshold:
        excess_cash = liquid_cash - cash_threshold
        investment = excess_cash

        current_total = stock_total + bond_total
        if current_total + investment > 0:
            # Calculate how much stock we SHOULD have
            # after investing this new money.
            #
            # Formula:
            # target_stock_value =
            #     target_stock % * (current portfolio + new investment)
            #
            # Example:
            # target_stock = 30%
            # current_total = 10L
            # investment = 8L
            # target_stock_value = 0.30 * 18L = 5.4L
            target_stock_value = target_stock * (current_total + investment)

            # How much additional stock is required
            # to reach the target allocation
            # current stock = 8L
            # target stock = 5.4L
            # difference = -2.6L (stocks already overweight)

            suggested_equity = max(0, target_stock_value - stock_total)

            # Prevent investing more in stocks than available cash
            # If calculation suggested 10L stock purchase
            # but we only have 8L investment cash
            # we cap it at 8L.
            suggested_equity = min(suggested_equity, investment)

            # Remaining investment goes to bonds
            # investment = 8L
            # equity purchase = 0
            # bond purchase = 8L
            suggested_bond = investment - suggested_equity

    summary_data = [
        {"Metric": "Assets", "Amount": assets},
        {"Metric": "Liabilities", "Amount": liabilities},
        {"Metric": "Liquid Cash", "Amount": liquid_cash},
        {"Metric": "Income (Current Period)", "Amount": abs(income)},
        {"Metric": "Expenses (Current Period)", "Amount": abs(expenses)},
        {"Metric": "Stock", "Amount": round(stock_total, 2)},
        {"Metric": "Bond", "Amount": round(bond_total, 2)},
        {
            "Metric": "Stock:Bond",
            "Amount": f"{stock_pct * 100:.0f}:{bond_pct * 100:.0f}",
        },
        {
            "Metric": "Recommended Stock Purchase",
            "Amount": round(suggested_equity, 2),
        },
        {
            "Metric": "Recommended Bond Purchase",
            "Amount": round(suggested_bond, 2),
        },
    ]
    return summary_data


def calculate_investment_allocation(
    balance_sheet_data,
    categories_mapping,
    zero_balance_accounts_config,
):
    allocation_totals = {name: 0 for name in categories_mapping.keys()}
    allocation_totals["Other"] = 0
    total_investment = 0

    for entry in balance_sheet_data:
        account = entry["account"]
        amount = entry["amount"]

        if is_zero_account(account, zero_balance_accounts_config):
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
            print(
                f"{RED_BOLD}WARNING: Unmatched account -> "
                f"Account: {account}, Amount: {amount}{RESET}"
            )
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
                "% Allocation": (amount / total_investment) if total_investment else 0,
            }
        )

    allocation_data.sort(key=lambda x: x["Amount"], reverse=True)
    return allocation_data

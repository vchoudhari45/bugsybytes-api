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
    balance_sheet_data, income_statement_data, zero_balance_accounts_config
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
    summary_data = [
        {"Metric": "Net Worth", "Amount": assets - liabilities},
        {"Metric": "Assets", "Amount": assets},
        {"Metric": "Liabilities", "Amount": liabilities},
        {"Metric": "Liquid Cash", "Amount": liquid_cash},
        {"Metric": "Income (Current Period)", "Amount": income},
        {"Metric": "Expenses (Current Period)", "Amount": expenses},
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

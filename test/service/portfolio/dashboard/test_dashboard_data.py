import json
from unittest.mock import patch

import yaml

from src.data.config import DASHBOARD_CONFIG_PATH
from src.service.portfolio.dashboard.dashboard_data import (
    calculate_category_tables_data,
    calculate_investment_allocation,
    calculate_summary_data,
)
from src.service.util.df_util import assert_dataframes_equal, json_to_df

# read json input
with open("test/data/portfolio/dashboard/dashboard_test_data.json") as f:
    dashboard_test_data = json.load(f)

# read yaml config
with open(DASHBOARD_CONFIG_PATH, "r") as f:
    dashboard_config = yaml.safe_load(f)

    # read json output
    with open("test/data/portfolio/dashboard/dashboard_expected_output.json") as f:
        dashboard_expected_output = json.load(f)


@patch("src.service.portfolio.dashboard.dashboard_data.get_ledger_cli_output_by_config")
def test_calculate_summary_data(mock_get_ledger_cli):
    # mocking
    mock_get_ledger_cli.side_effect = [
        ["INF846K01CX4", "INF846K01Q62"],
        [
            {
                "currency": "INR",
                "amount": 100.0,
                "account": "Assets:MutualFunds",
                "level": 0,
            }
        ],
        [
            {
                "currency": "INR",
                "amount": 100.0,
                "account": "Assets:MutualFunds",
                "level": 0,
            }
        ],
    ]

    sample_balance_sheet = dashboard_test_data["balance_sheet_data"]
    sample_income_statement = dashboard_test_data["income_statement_data"]
    zero_accounts = dashboard_config["dashboard"]["zero_balance_accounts"]
    stock_vs_bond_config = dashboard_config["dashboard"]["stock_vs_bond"]
    mutual_funds_config = dashboard_config["dashboard"]["mutual_funds"]
    categories_threshold = dashboard_config["dashboard"]["categories_threshold"]

    result = calculate_summary_data(
        balance_sheet_data=sample_balance_sheet,
        income_statement_data=sample_income_statement,
        zero_balance_accounts_config=zero_accounts,
        mutual_funds=mutual_funds_config,
        ledger_files=["dummy.ledger"],
        stock_vs_bond_config=stock_vs_bond_config,
        categories_threshold=categories_threshold,
    )
    result_df = json_to_df(result)
    expected_df = json_to_df(dashboard_expected_output["summary_data"])
    assert_dataframes_equal(result_df, expected_df)


def test_calculate_category_tables_data():
    sample_balance_sheet = dashboard_test_data["balance_sheet_data"]
    categories = dashboard_config["dashboard"]["categories"]
    zero_accounts = dashboard_config["dashboard"]["zero_balance_accounts"]
    result = calculate_category_tables_data(
        sample_balance_sheet, categories, zero_accounts
    )
    result_df = json_to_df(result)
    expected_df = json_to_df(dashboard_expected_output["categories_data"])
    assert_dataframes_equal(result_df, expected_df)


def test_calculate_investment_allocation():
    sample_balance_sheet = dashboard_test_data["balance_sheet_data"]
    categories_mapping = dashboard_config["dashboard"]["categories"]
    zero_accounts = dashboard_config["dashboard"]["zero_balance_accounts"]
    result = calculate_investment_allocation(
        sample_balance_sheet, categories_mapping, zero_accounts
    )
    result_df = json_to_df(result)
    expected_df = json_to_df(dashboard_expected_output["investment_allocation_data"])
    assert_dataframes_equal(result_df, expected_df)

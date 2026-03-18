import csv
import json
import sys
from unittest.mock import patch

import yaml

from src.service.portfolio.dashboard.account_metrics_data import (
    calculate_individual_xirr_report_data,
)
from src.service.util.df_util import assert_dataframes_equal, json_to_df


def debug_account_metrics_data(data):
    rows = []

    for section in data:
        section_name = section.get("name")
        section_type = section.get("type")

        for item in section.get("data", []):
            row = {
                "SECTION": section_name,
                "TYPE": section_type,
            }

            for k, v in item.items():
                try:
                    row[k] = float(v)
                except (TypeError, ValueError):
                    row[k] = v

            rows.append(row)

    # collect headers
    fieldnames = set()
    for r in rows:
        fieldnames.update(r.keys())

    fieldnames = ["SECTION", "TYPE"] + sorted(
        f for f in fieldnames if f not in {"SECTION", "TYPE"}
    )

    # print CSV to console
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)


def test_get_account_performance_metrics_data():
    # read yaml
    with open("test/data/portfolio/lookup/test_dashboard_config.yaml", "r") as f:
        dashboard_config = yaml.safe_load(f)

    # read json
    with open("test/data/portfolio/account-metrics/ledger_test_data.json") as f:
        ledger_data = json.load(f)

    # read nifty index mock data
    with open("test/data/portfolio/account-metrics/nifty_index_test_data.json") as f:
        nifty_index_data = json.load(f)

    # read account metrics expected output
    with open(
        "test/data/portfolio/account-metrics/account_metrics_expected_output.json"
    ) as f:
        account_metrics_expected_output = json.load(f)

    # configs
    individual_xirr_reports_config = dashboard_config["dashboard"][
        "individual_xirr_reports"
    ]
    mutual_funds = dashboard_config["dashboard"]["mutual_funds"]
    nifty_index_threshold = dashboard_config["dashboard"]["nifty_index"]["threshold"]
    ledger_files = {"dummy_ledger_file.ledger"}

    # mocking
    def mock_get_ledger_cli_output_by_config(
        config, ledger_files, commodity=None, command_type="balance"
    ):
        cmd = config["command"].strip()
        if command_type.lower() == "commodities":
            # matches parse_ledger_cli_commodities_output → returns list[str]
            if cmd == "list equity":
                output = ledger_data["equity_list"]
            elif cmd == "list mutual_fund":
                output = ledger_data["mutual_fund_list"]
        elif command_type.lower() == "register":
            # matches parse_ledger_cli_register_output
            if cmd == "equity income register":
                output = ledger_data["equity_register"][commodity]["income"]
            elif cmd == "equity purchase register":
                output = ledger_data["equity_register"][commodity]["purchase_or_sell"]
            elif cmd == "mutual_fund income register":
                output = ledger_data["mutual_fund_register"][commodity]["income"]
            elif cmd == "mutual_fund purchase register":
                output = ledger_data["mutual_fund_register"][commodity][
                    "purchase_or_sell"
                ]
        else:
            # default = balance → matches parse_ledger_cli_balance_output → leaf_accounts
            if cmd == "balance equity":
                output = ledger_data["equity_balance"][commodity]
            elif cmd == "balance mutual_fund":
                output = ledger_data["mutual_fund_balance"][commodity]
        return output

    # mocking amfi_isin_map
    amfi_isin_map = {
        "INF179KB1HP9": "HDFC Liquid Fund - Growth Option - Direct Plan",
        "INF179KB1HR5": "HDFC Money Market Fund - Growth Option- Direct Plan",
    }

    # mocking nifty_index_cache
    company_map = {
        "TCS": 1,
        "HDFCBANK": 2,
        "ICICIBANK": 3,
        "VEDL": 4,
        "GAIL": 5,
        "TATAPOWER": 6,
        "ADANIENSOL": 7,
    }

    def mock_get_company_id(company_name, session):
        return company_map.get(company_name)

    with patch(
        "src.service.portfolio.dashboard.account_metrics_data.get_ledger_cli_output_by_config",
        side_effect=mock_get_ledger_cli_output_by_config,
    ), patch(
        "src.service.portfolio.dashboard.account_metrics_data.fetch_amfi_isin_scheme_map",
        return_value=amfi_isin_map,
    ), patch(
        "src.service.portfolio.dashboard.account_metrics_data.fetch_nifty_index",
        return_value=nifty_index_data,
    ), patch(
        "src.service.portfolio.dashboard.account_metrics_data.get_company_id",
        side_effect=mock_get_company_id,
    ), patch(
        "src.service.portfolio.dashboard.account_metrics_data.get_metrics",
        return_value={},
    ):
        # function call
        individual_xirr_reports_data = calculate_individual_xirr_report_data(
            ledger_files=ledger_files,
            individual_xirr_reports_config=individual_xirr_reports_config,
            mutual_funds=mutual_funds,
            recommended_stock=10000,
            nifty_index_threshold=nifty_index_threshold,
        )
        # debug_account_metrics_data(individual_xirr_reports_data)
        result_df = json_to_df(individual_xirr_reports_data)
        expected_df = json_to_df(account_metrics_expected_output)
        assert_dataframes_equal(result_df, expected_df)

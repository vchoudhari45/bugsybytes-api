import json
from unittest.mock import patch

import yaml
from freezegun import freeze_time

from src.service.portfolio.dashboard.gsec_data import (
    calculate_gsec_individual_xirr_report_data,
)
from src.service.util.df_util import assert_dataframes_equal, json_to_df


@freeze_time("2026-03-18")
def test_calculate_gsec_individual_xirr_report_data():
    # read yaml
    with open("test/data/portfolio/lookup/test_dashboard_config.yaml", "r") as f:
        dashboard_config = yaml.safe_load(f)

    # read json
    with open("test/data/portfolio/gsec/ledger_test_data.json") as f:
        ledger_data = json.load(f)

    # read json
    with open("test/data/portfolio/gsec/gsec_data_expected_output.json") as f:
        gsec_expected_data = json.load(f)

    # configs
    gsec_individual_xirr_reports_config = dashboard_config["dashboard"][
        "gsec_individual_xirr_reports"
    ]

    ledger_files = {"dummy_ledger_file.ledger"}

    # mocking
    def mock_get_ledger_cli_output_by_config(
        config, ledger_files, commodity=None, command_type="balance"
    ):
        cmd = config["command"].strip()
        if command_type.lower() == "commodities":
            if cmd == "list gsec":
                return ledger_data["gsec_list"]
        elif command_type.lower() == "gsec_register":
            if cmd == "gsec register":
                return ledger_data["gsec_register"][commodity]["purchase_or_sell"]

    with patch(
        "src.service.portfolio.dashboard.gsec_data.get_ledger_cli_output_by_config",
        side_effect=mock_get_ledger_cli_output_by_config,
    ):
        # function call
        result = calculate_gsec_individual_xirr_report_data(
            ledger_files=ledger_files,
            gsec_individual_xirr_reports_config=gsec_individual_xirr_reports_config,
        )
        result_cashflow_df = json_to_df(result[0]["cashflow_data"])
        result_kpi_list_df = json_to_df(result[0]["kpi_list"])
        result_xirr_data_df = json_to_df(result[0]["xirr_data"])

        expected_cashflow_df = json_to_df(gsec_expected_data["cashflow_data"])
        expected_kpi_list_df = json_to_df(gsec_expected_data["kpi_list"])
        expected_xirr_data_df = json_to_df(gsec_expected_data["xirr_data"])

        assert_dataframes_equal(result_cashflow_df, expected_cashflow_df)
        assert_dataframes_equal(result_kpi_list_df, expected_kpi_list_df)
        assert_dataframes_equal(result_xirr_data_df, expected_xirr_data_df)

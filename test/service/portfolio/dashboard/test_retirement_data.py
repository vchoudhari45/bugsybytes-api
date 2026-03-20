import json

import yaml

from src.data.config import DASHBOARD_CONFIG_PATH
from src.service.portfolio.dashboard.retirement_data import calculate_retirement_data
from src.service.util.df_util import assert_dataframes_equal, json_to_df

# read yaml config
with open(DASHBOARD_CONFIG_PATH, "r") as f:
    dashboard_config = yaml.safe_load(f)

with open("test/data/portfolio/retirement/retirement_data_expected_output.json") as f:
    retirement_data = json.load(f)


def test_calculate_retirement_data_basic():
    result = calculate_retirement_data(
        dashboard_config["dashboard"]["retirement_tracker"]
    )
    result_df = json_to_df(result)
    expected_df = json_to_df(retirement_data)
    assert_dataframes_equal(result_df, expected_df)

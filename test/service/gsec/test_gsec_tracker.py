import pandas as pd
import pytest
from pandas.errors import MergeError

from src.service.gsec.gsec_tracker import process_nse_gsec_csv
from src.service.util.df_util import assert_dataframes_equal

FOLDER_PATH = "test/data/gsec/nse_live_data/"
LOOKUP_PATH = "test/data/gsec/lookup"
EXPECTED_FILE = "test/data/gsec/gsec_tracker_expected_output.csv"


# Golden path
def test_process_nse_gsec_csv_golden_path():
    result_df = process_nse_gsec_csv(
        folder_path=FOLDER_PATH,
        override_file=f"{LOOKUP_PATH}/gsec_details_golden_path.csv",
        include_historical=False,
    )
    expected_df = pd.read_csv(
        EXPECTED_FILE,
        parse_dates=["DATE", "MATURITY DATE"],
    )
    assert_dataframes_equal(result_df, expected_df)


# Error cases
@pytest.mark.parametrize(
    "file, expected_error",
    [
        pytest.param("gsec_details_duplicate.csv", MergeError, id="duplicate_isin"),
        pytest.param(
            "gsec_details_invalid_coupon_rate.csv", SystemExit, id="invalid_coupon_rate"
        ),
        pytest.param(
            "gsec_details_invalid_year.csv", SystemExit, id="invalid_maturity_year"
        ),
        pytest.param(
            "gsec_details_missing_coupon_rate.csv", SystemExit, id="missing_coupon_rate"
        ),
        pytest.param(
            "gsec_details_missing_maturity_date.csv",
            SystemExit,
            id="missing_maturity_date",
        ),
    ],
)
def test_process_nse_gsec_csv_error_cases(file, expected_error):
    with pytest.raises(expected_error):
        process_nse_gsec_csv(
            folder_path=FOLDER_PATH,
            override_file=f"{LOOKUP_PATH}/{file}",
            include_historical=False,
        )

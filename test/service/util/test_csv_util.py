import io

import pandas as pd

from src.service.util import csv_util as cu
from src.service.util.df_util import assert_dataframes_equal


def test_normalized_dict_reader_trims_headers():
    csv_content = io.StringIO("  Name , Age , City \nAlice,30,NY\nBob,25,LA")
    reader = cu.normalized_dict_reader(csv_content)
    headers = reader.fieldnames
    assert headers == ["Name", "Age", "City"]
    rows = list(reader)
    assert rows[0] == {"Name": "Alice", "Age": "30", "City": "NY"}


def test_non_comment_lines_skips_comments_and_empty_lines():
    csv_content = io.StringIO(
        "\n# Comment line\nData line 1\n  # Another comment\n\nData line 2"
    )
    lines = list(cu.non_comment_lines(csv_content))
    assert lines == ["Data line 1\n", "Data line 2"]


def test_read_all_dated_csv_files_from_folder():
    result_df = cu.read_all_dated_csv_files_from_folder("test/data/csv/input/")
    expected_df = pd.read_csv(
        "test/data/csv/csv_util_expected_output.csv",
        parse_dates=["DATE"],
    )
    assert_dataframes_equal(result_df, expected_df)

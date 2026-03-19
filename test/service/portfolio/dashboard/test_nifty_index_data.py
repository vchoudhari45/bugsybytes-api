import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from src.service.portfolio.dashboard.nifty_index_data import (
    fetch_nse_stocks,
    get_with_retry,
)
from src.service.util.df_util import assert_dataframes_equal, json_to_df


def test_get_with_retry_success():
    session = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None

    session.get.return_value = response

    result = get_with_retry(session, "http://test.com")

    assert result == response
    session.get.assert_called_once()


@patch("time.sleep", return_value=None)
def test_get_with_retry_retries_then_success(mock_sleep):
    session = MagicMock()
    response = MagicMock()
    response.raise_for_status.return_value = None

    session.get.side_effect = [
        requests.exceptions.RequestException("fail1"),
        requests.exceptions.RequestException("fail2"),
        response,
    ]

    result = get_with_retry(session, "http://test.com", max_retries=3)

    assert result == response
    assert session.get.call_count == 3


@patch("time.sleep", return_value=None)
def test_get_with_retry_failure(mock_sleep):
    session = MagicMock()
    session.get.side_effect = requests.exceptions.RequestException("fail")

    with pytest.raises(requests.exceptions.RequestException):
        get_with_retry(session, "http://test.com", max_retries=3)


@patch("src.service.portfolio.dashboard.nifty_index_data.requests.Session")
@patch("src.service.portfolio.dashboard.nifty_index_data.yaml.safe_load")
def test_fetch_nse_stocks(mock_yaml, mock_session_cls):

    with open("test/data/portfolio/nifty-index/nifty_index_test_data.json") as f:
        nifty_index_data = json.load(f)

    with open("test/data/portfolio/nifty-index/nifty_index_expected_output.json") as f:
        nifty_index_data_output = json.load(f)

    mock_yaml.return_value = {
        "dashboard": {
            "nifty_index": {
                "base_url": "https:test.com/",
                "api_endpoint": "api",
                "indices": {
                    "NIFTY 50": "NIFTY%2050",
                    "NIFTY NEXT 50": "NIFTY%20NEXT%2050",
                },
            }
        }
    }

    def mock_get(url, timeout=60):
        mock_resp = MagicMock()
        mock_resp.raise_for_status.return_value = None
        url = str(url)
        if "index=NIFTY%2050" in url:
            mock_resp.json.return_value = nifty_index_data["NIFTY 50"]
        elif "index=NIFTY%20NEXT%2050" in url:
            mock_resp.json.return_value = nifty_index_data["NIFTY NEXT 50"]
        else:
            mock_resp.json.return_value = {}

        return mock_resp

    mock_session = MagicMock()
    mock_session.get.side_effect = mock_get
    mock_session_cls.return_value = mock_session

    # call function
    result_df = json_to_df(fetch_nse_stocks())
    expected_df = json_to_df(nifty_index_data_output)

    assert_dataframes_equal(result_df, expected_df)

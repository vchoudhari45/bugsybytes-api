from datetime import datetime

import numpy as np
import pandas as pd
import pytest

from src.service.util import symbol_parsers as sp


@pytest.mark.parametrize(
    "symbol,expected",
    [
        ("763MH36", 7.63),
        ("74GS2035", 7.4),
        ("90GS2035", 9.00),
        ("1018GS2035", 10.18),
        ("ABCD", np.nan),
        ("12345", np.nan),
        ("", np.nan),
        (None, np.nan),
    ],
)
def test_extract_coupon_from_symbol(symbol, expected):
    result = sp.extract_coupon_from_symbol(symbol)
    if expected is None:
        assert (
            result is None
        ), f"Failed for symbol={symbol}: got {result}, expected None"
    elif isinstance(expected, float) and np.isnan(expected):
        assert isinstance(result, float) and np.isnan(
            result
        ), f"Failed for symbol={symbol}: got {result}, expected NaN"
    else:
        assert (
            result == expected
        ), f"Failed for symbol={symbol}: got {result}, expected {expected}"


@pytest.mark.parametrize(
    "symbol,expected_year",
    [
        ("763MH36", 2036),
        ("74GS2035", 2035),
        ("123AB99", 2099),
        ("ABCD", None),
        ("12345", None),
        ("", None),
        (None, None),
    ],
)
def test_extract_maturity_date_from_symbol(symbol, expected_year):
    result = sp.extract_maturity_date_from_symbol(symbol)
    if expected_year is None or expected_year < 1678 or expected_year > 2262:
        assert pd.isna(
            result
        ), f"Failed for symbol={symbol}: expected NaT, got {result}"
    else:
        expected_date = pd.Timestamp(datetime(expected_year, 3, 31))
        assert (
            result == expected_date
        ), f"Failed for symbol={symbol}: got {result}, expected {expected_date}"

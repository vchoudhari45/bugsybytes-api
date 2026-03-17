from datetime import date

import pandas as pd
import pytest

from src.service.util import date_util as du


@pytest.mark.parametrize(
    "input_val,expected",
    [
        ("18-02-2025", date(2025, 2, 18)),
        ("18/02/2025", date(2025, 2, 18)),
        ("18/02/25", date(2025, 2, 18)),
        ("18 Feb 2025", date(2025, 2, 18)),
        ("18-Feb-2025", date(2025, 2, 18)),
        ("18 February 2025", date(2025, 2, 18)),
        ("Sep 11 2026", date(2026, 9, 11)),
        ("September 11 2026", date(2026, 9, 11)),
        ("18-02-2025 09:19 AM", date(2025, 2, 18)),
        ("18/02/2025 09:19 AM", date(2025, 2, 18)),
        ("2025-01-23", date(2025, 1, 23)),
        ("2025-01-23 09:19:00", date(2025, 1, 23)),
        ("2025/01/23", date(2025, 1, 23)),
        ("23.01.2025", date(2025, 1, 23)),
        (pd.Timestamp("2025-02-18"), date(2025, 2, 18)),
        (date(2025, 2, 18), date(2025, 2, 18)),
    ],
)
def test_parse_indian_date_format(input_val, expected):
    result = du.parse_indian_date_format(input_val)
    assert result == expected

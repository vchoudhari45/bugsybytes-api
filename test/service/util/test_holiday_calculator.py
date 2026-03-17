from datetime import date

import pytest

from src.service.util import holiday_calculator as hc


@pytest.mark.parametrize(
    "start_date,lag_days,expected_date",
    [
        # golden path 0 day lag
        (date(2026, 3, 16), 0, date(2026, 3, 16)),
        # golden path 1 day lag
        (date(2026, 3, 16), 1, date(2026, 3, 17)),
        # day is holiday and lag is 0
        (date(2026, 1, 26), 0, date(2026, 1, 27)),
        # next day is holiday and lag is 1
        (date(2026, 1, 25), 1, date(2026, 1, 27)),
        # day is friday and lag day is 0
        (date(2026, 3, 13), 0, date(2026, 3, 13)),
        # day is friday and lag day is 1
        (date(2026, 3, 13), 1, date(2026, 3, 16)),
        # day is friday and lag day is 2
        (date(2026, 3, 13), 2, date(2026, 3, 17)),
    ],
)
def test_next_market_day(start_date, lag_days, expected_date):
    result = hc.next_market_day(start_date, lag_days)
    assert result == expected_date, (
        f"Failed for start_date={start_date}, lag_days={lag_days}: "
        f"got {result}, expected {expected_date}"
    )

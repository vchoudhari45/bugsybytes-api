from datetime import date, timedelta

import holidays

india_holidays = set(
    holidays.India(
        years=[2024, 2025, 2026, 2027, 2028, 2029, 2030, 2031, 2032, 2033, 2034, 2035]
    ).keys()
)
market_holidays = india_holidays


def next_market_day(start_date: date, lag_days: int = 1) -> date:
    """
    Returns the date after lag_days, skipping weekends and holidays.
    """
    current_date = start_date
    days_counted = 0

    while days_counted < lag_days:
        current_date += timedelta(days=1)
        if current_date.weekday() < 5 and current_date not in market_holidays:
            days_counted += 1

    # Ensure the resulting date is also a market day
    while current_date.weekday() >= 5 or current_date in market_holidays:
        current_date += timedelta(days=1)

    return current_date

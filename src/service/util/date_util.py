from datetime import date, datetime

import pandas as pd


def parse_indian_date_format(val):
    """
    Normalize any date-like input to datetime.date.

    Supported input types:
        - pandas.Timestamp
        - datetime.date
        - str in strict Indian or common formats

    Raises:
        TypeError if input is not a string, Timestamp, or date
        ValueError if string format is unsupported
    """
    # Handle pandas.Timestamp
    if isinstance(val, pd.Timestamp):
        return val.date()

    # Handle datetime.date
    if isinstance(val, date):
        return val

    # Handle string input
    if not isinstance(val, str):
        raise TypeError("Input must be a string, datetime.date, or pandas.Timestamp")

    date_str = val.strip()
    if not date_str:
        raise ValueError("Empty date string")

    SUPPORTED_FORMATS = (
        "%d-%m-%Y",  # 18-02-2025
        "%d/%m/%Y",  # 18/02/2025
        "%d/%m/%y",  # 18/02/25
        "%d %b %Y",  # 18 Feb 2025
        "%d-%b-%Y",  # 18-Feb-2025
        "%d %B %Y",  # 18 February 2025
        "%b %d %Y",  # Sep 11 2026
        "%B %d %Y",  # September 11 2026
        "%d-%m-%Y %I:%M %p",  # 18-02-2025 09:19 AM
        "%d/%m/%Y %I:%M %p",  # 18/02/2025 09:19 AM
        "%Y-%m-%d",  # 2025-01-23
        "%Y-%m-%d %H:%M:%S",  # 2025-01-23 09:19:00
        "%Y/%m/%d",  # 2025/01/23
        "%d.%m.%Y",  # 23.01.2025
    )

    for fmt in SUPPORTED_FORMATS:
        try:
            return datetime.strptime(date_str, fmt).date()
        except ValueError:
            continue

    raise ValueError(
        f"Unsupported date format: '{date_str}'. "
        "Add the explicit format if this is expected input."
    )

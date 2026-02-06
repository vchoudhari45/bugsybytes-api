from datetime import date, datetime

import pandas as pd
from dateutil.parser import parse


def parse_date(date_str):
    date_str = date_str.strip()
    for fmt in (
        "%Y-%m-%d",  # 2026-01-09
        "%d-%m-%Y",  # 09-01-2026
        "%d/%m/%Y",  # 09/01/2026
        "%d/%m/%y",  # 09/01/26
    ):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Unrecognized date format: {date_str}")


def to_date(val):
    """Normalize any date-like input to datetime.date"""
    if isinstance(val, pd.Timestamp):
        return val.date()
    if isinstance(val, date):
        return val
    return parse(str(val)).date()

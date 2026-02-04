from datetime import date, datetime
from dateutil.parser import parse
import pandas as pd

def parse_date(date_str):
    for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
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
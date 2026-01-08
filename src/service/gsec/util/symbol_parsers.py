import re
from datetime import datetime

import numpy as np
import pandas as pd


def extract_coupon_from_symbol(symbol: str) -> int:
    # If symbol is NaN, None, or not a string, regex cannot be applied safely
    if not isinstance(symbol, str):
        return np.nan

    # Extract leading digits from the beginning of SYMBOL
    match = re.match(r"^(\d+)", symbol)

    # Get the extracted digits as a string
    digits = match.group(1)
    if len(digits) == 2:
        digits = float(digits) * 10

    return float(digits) / 100


def extract_maturity_date_from_symbol(
    symbol: str, is_tbill: bool, is_gsec_stripped: bool, century=2000
) -> pd.Timestamp:
    # If symbol is NaN, None, or not a string, regex cannot be applied safely
    if not isinstance(symbol, str):
        return pd.NaT

    # Treasury bill: 364D261225, 364D100426
    if is_tbill:
        match = re.search(r"D(\d{6})$", symbol)
        date_str = match.group(1)
        day = int(date_str[:2])
        month = int(date_str[2:4])
        year = int(date_str[4:])
        if year < 100:
            year += century

    # Stripped GSec: example GS220261P
    elif is_gsec_stripped:
        # Match DDMMYY optionally followed by a character
        match = re.search(r"GS(\d{6})\D?$", symbol)
        date_str = match.group(1)
        day = int(date_str[:2])
        month = int(date_str[2:4])
        year = int(date_str[4:])
        if year < 100:
            year += century

    # GSec: example 763MH36, 74GS2035
    else:
        match = re.search(r"(\d{2,4})\D?$", symbol)
        # Default maturity date: march 31 with extracted year
        day = 31
        month = 3
        year = int(match.group(1))
        if year < 100:
            year += century

    # Reject years outside pandas range
    if year < 1678 or year > 2262:
        return pd.NaT

    return pd.Timestamp(datetime(year, month, day))

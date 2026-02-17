import re
from datetime import datetime

import numpy as np
import pandas as pd


def extract_coupon_from_symbol(symbol: str) -> float:
    """
    Extract coupon from GSEC symbol.
    Example:
        763MH36  -> 7.63
        74GS2035 -> 7.40
    """
    if not isinstance(symbol, str):
        return np.nan

    match = re.match(r"^(\d+)", symbol)
    if not match:
        return np.nan

    digits = match.group(1)

    # Handle 2-digit coupon like 74GS2035 -> 7.4
    if len(digits) == 2:
        digits = float(digits) * 10

    return float(digits) / 100


def extract_maturity_date_from_symbol(
    symbol: str,
    century: int = 2000,
) -> pd.Timestamp:
    """
    Extract maturity for standard coupon GSEC only.

    Example:
        763MH36  -> 31-Mar-2036
        74GS2035 -> 31-Mar-2035
    """

    if not isinstance(symbol, str):
        return pd.NaT

    match = re.search(r"(\d{2,4})\D?$", symbol)
    if not match:
        return pd.NaT

    year = int(match.group(1))

    if year < 100:
        year += century

    # GSEC default maturity convention
    day = 31
    month = 3

    if year < 1678 or year > 2262:
        return pd.NaT

    return pd.Timestamp(datetime(year, month, day))

from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy.optimize import newton


def __ytm_tbill(price, face_value, days):
    """Yield for T-Bills (discounted instruments)."""
    return ((face_value - price) / price) * (365 / days)


def __ytm_stripped_gsec(price, face_value, years):
    """Yield for zero-coupon / stripped G-Secs."""
    if years <= 0:
        return np.nan
    return (face_value / price) ** (1 / years) - 1


# The `ytm_coupon_bond` function may show small inaccuracies due to:
#
# 1. G-Secs in the input CSV do not include exact maturity dates.
#    We extract only the maturity year from the symbol and assume
#    a default maturity date of 31st March for that year.
#
# 2. Because the actual maturity month/day is unknown, the calculated
#    number of periods to maturity is always approximate.
#
# 3. The function uses `ceil` when computing `n_period`, which slightly
#    overestimates the remaining periods and results in a higher YTM.
#    This is intentional so the bonds appear in our G-Sec tracker; the
#    exact YTM can be recomputed later in the portfolio tracker once
#    the precise maturity date is manually looked up and updated in gsec_maturity_date_override_df
def __ytm_coupon_bond(
    price, coupon_rate, face_value, settlement_date, maturity_date, coupon_frequency=2
):
    """
    Yield to maturity for coupon-bearing G-Secs using SciPy's newton method.
    """
    if price <= 0 or coupon_rate < 0 or settlement_date >= maturity_date:
        return np.nan

    years_to_maturity = (maturity_date - settlement_date).days / 365.0
    n_periods = int(np.ceil(years_to_maturity * coupon_frequency))
    C = float(coupon_rate) * (float(face_value) / int(coupon_frequency))

    # Define function which calculates value of y which makes,
    # present value of future coupons + present value principal payment in future
    # equal to price of the bond
    #         N         C               F
    # f(y) =  ∑    -----------  +  ----------- - P
    #        t=1   (1 + y/f)^t     (1 + y/f)^N
    #
    # Where:
    #   First term: Present value of coupons
    #   Second term: Present value of principal
    #   C = coupon per period
    #   F = face value
    #   P = price
    #   N = total periods
    #   f = coupon frequency per year
    #   y = yield to maturity (unknown)
    def f(y):
        return (
            sum([C / (1 + y / coupon_frequency) ** t for t in range(1, n_periods + 1)])
            + float(face_value) / (1 + y / coupon_frequency) ** n_periods
            - price
        )

    try:
        ytm = newton(f, x0=coupon_rate)
        return ytm
    except (RuntimeError, OverflowError):
        return np.nan


def calculate_gsec_ytm(
    price: float,
    coupon_rate: float,
    maturity_date: datetime,
    is_tbill: bool,
    is_stripped_gsec: bool,
    face_value: float = 100.0,
    coupon_frequency: int = 2,
    settlement_date: date = None,
) -> float:
    """
    Calculate YTM for a bond depending on its type:
    - T-Bill (discounted)
    - Stripped G-Sec (zero-coupon)
    - Coupon-bearing G-Sec (full YTM via Newton)
    """

    if pd.isna(price) or price <= 0:
        return np.nan

    if settlement_date is None:
        settlement_date = datetime.today().date()

    # Normalize maturity_date to a python date
    if isinstance(maturity_date, pd.Timestamp):
        maturity_date = maturity_date.date()

    elif isinstance(maturity_date, datetime):
        maturity_date = maturity_date.date()

    elif isinstance(maturity_date, str):
        maturity_date = pd.to_datetime(maturity_date, errors="raise").date()

    elif isinstance(maturity_date, date):
        pass  # already a date
    else:
        raise ValueError(f"Unsupported maturity_date type: {type(maturity_date)}")

    days_to_maturity = (maturity_date - settlement_date).days
    years_to_maturity = days_to_maturity / 365.0

    # --- T-Bill ---
    if is_tbill:
        if days_to_maturity <= 0:
            return np.nan
        return __ytm_tbill(price, face_value, days_to_maturity) * 100

    # --- Stripped G-Sec (zero-coupon) ---
    if is_stripped_gsec:
        return __ytm_stripped_gsec(price, face_value, years_to_maturity) * 100

    # --- Coupon-bearing G-Sec ---
    if pd.isna(coupon_rate) or coupon_rate <= 0:
        return np.nan

    # Use full YTM via Newton-Raphson
    return (
        __ytm_coupon_bond(
            price,
            coupon_rate / 100,
            face_value,
            settlement_date,
            maturity_date,
            coupon_frequency,
        )
        * 100
    )

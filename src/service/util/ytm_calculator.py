from datetime import date, datetime

import numpy as np
import pandas as pd
from scipy.optimize import newton


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
    price,
    coupon_rate,
    face_value,
    settlement_date,
    maturity_date,
    coupon_frequency=2,
):
    """
    Yield to maturity for coupon-bearing G-Secs using SciPy's newton method.
    """

    years_to_maturity = (maturity_date - settlement_date).days / 365.0
    n_periods = int(np.ceil(years_to_maturity * coupon_frequency))

    C = float(coupon_rate) * (float(face_value) / int(coupon_frequency))

    def f(y):
        return (
            sum([C / (1 + y / coupon_frequency) ** t for t in range(1, n_periods + 1)])
            + float(face_value) / (1 + y / coupon_frequency) ** n_periods
            - price
        )

    # No try/except — if Newton fails, it fails
    ytm = newton(f, x0=coupon_rate)
    return ytm


def calculate_gsec_ytm(
    price: float,
    coupon_rate: float,
    maturity_date: datetime,
    face_value: float = 100.0,
    coupon_frequency: int = 2,
    settlement_date: date = None,
) -> float:
    """
    Calculate YTM for coupon-bearing G-Sec (Newton full solution).
    """

    if settlement_date is None:
        settlement_date = datetime.today().date()

    # Normalize maturity_date to python date
    if isinstance(maturity_date, pd.Timestamp):
        maturity_date = maturity_date.date()

    elif isinstance(maturity_date, datetime):
        maturity_date = maturity_date.date()

    elif isinstance(maturity_date, str):
        maturity_date = pd.to_datetime(maturity_date, errors="raise").date()

    elif isinstance(maturity_date, date):
        pass
    else:
        raise ValueError(f"Unsupported maturity_date type: {type(maturity_date)}")

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

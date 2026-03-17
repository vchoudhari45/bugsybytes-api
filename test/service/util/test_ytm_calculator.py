import math
from datetime import date

import pytest

from src.service.util import ytm_calculator as yc


@pytest.mark.parametrize(
    "price,coupon_rate,maturity_date,face_value,coupon_frequency,settlement_date,expected_ytm",
    [
        # 1-year bond, 10% coupon, price 100 → YTM ≈ coupon rate
        (100, 10, date(2026, 1, 1), 100, 2, date(2025, 1, 1), 10.0),
        # 2-year bond, 8% coupon, price at par → YTM ≈ 8%
        (100, 8, date(2027, 1, 1), 100, 2, date(2025, 1, 1), 8.0),
        # 1-year bond, 10% coupon, price < par → YTM > coupon
        (95, 10, date(2026, 1, 1), 100, 2, date(2025, 1, 1), 15.59),
        # 1-year bond, 10% coupon, price > par → YTM < coupon
        (105, 10, date(2026, 1, 1), 100, 2, date(2025, 1, 1), 4.81),
        # Maturity_date as string → handled correctly
        (100, 10, "2026-01-01", 100, 2, date(2025, 1, 1), 10.0),
    ],
)
def test_calculate_gsec_ytm(
    price,
    coupon_rate,
    maturity_date,
    face_value,
    coupon_frequency,
    settlement_date,
    expected_ytm,
):
    result = yc.calculate_gsec_ytm(
        price=price,
        coupon_rate=coupon_rate,
        maturity_date=maturity_date,
        face_value=face_value,
        coupon_frequency=coupon_frequency,
        settlement_date=settlement_date,
    )

    # Allow 1% tolerance for small numerical errors
    assert math.isclose(
        result, expected_ytm, rel_tol=0.01
    ), f"Expected {expected_ytm}, got {result}"

import math
from datetime import date

import pytest

from src.service.util import xirr_calculator as xc


@pytest.mark.parametrize(
    "dates,cashflows,expected",
    [
        # Golden path: 10% XIRR
        ([date(2025, 1, 1), date(2026, 1, 1)], [-1000, 1100], 0.1),
        # Multiple periods
        (
            [date(2025, 1, 1), date(2025, 7, 1), date(2026, 1, 1)],
            [-1000, 50, 1100],
            0.1537376628,
        ),
        # Edge case: all negative → should raise
        ([date(2025, 1, 1), date(2026, 1, 1)], [-100, -50], "ValueError"),
        # Edge case: all positive → should raise
        ([date(2025, 1, 1), date(2026, 1, 1)], [100, 50], "ValueError"),
        # Zero XIRR: outflow equals inflow, no gain
        ([date(2025, 1, 1), date(2026, 1, 1)], [-100, 100], 0.0),
        # Negative XIRR: get back less than invested
        ([date(2025, 1, 1), date(2026, 1, 1)], [-1000, 900], -0.1),
        # Very high XIRR: doubles in one year
        ([date(2025, 1, 1), date(2026, 1, 1)], [-100, 200], 1.0),
        # Sub-year holding period (~6 months)
        ([date(2025, 1, 1), date(2025, 7, 1)], [-1000, 1050], 0.1033),
        # Long horizon: 5-year bond with annual coupons at par → 10% XIRR
        (
            [
                date(2025, 1, 1),
                date(2026, 1, 1),
                date(2027, 1, 1),
                date(2028, 1, 1),
                date(2029, 1, 1),
                date(2030, 1, 1),
            ],
            [-1000, 100, 100, 100, 100, 1100],
            0.1,
        ),
        # Irregular spacing: cashflows on non-uniform dates
        (
            [date(2025, 1, 1), date(2025, 4, 15), date(2026, 3, 10)],
            [-500, 100, 480],
            0.1562,
        ),
        # Multiple outflows (reinvestment scenario)
        (
            [date(2025, 1, 1), date(2025, 6, 1), date(2026, 1, 1)],
            [-600, -400, 1200],
            0.2427,
        ),
        # Single large inflow at end, small interim coupons
        (
            [
                date(2025, 1, 1),
                date(2025, 7, 1),
                date(2026, 1, 1),
                date(2026, 7, 1),
                date(2027, 1, 1),
            ],
            [-1000, 30, 30, 30, 1030],
            0.06090,
        ),
        # Zero cashflow in middle → should still compute correctly
        (
            [date(2025, 1, 1), date(2025, 7, 1), date(2026, 1, 1)],
            [-1000, 0, 1100],
            0.1,
        ),
        # Mismatched lengths → should raise
        ([date(2025, 1, 1), date(2026, 1, 1)], [-1000, 500, 700], "ValueError"),
        # Single cashflow only → should raise (need at least one sign change)
        ([date(2025, 1, 1)], [-1000], "ValueError"),
    ],
)
def test_xirr_input_expected(dates, cashflows, expected):
    if expected == "ValueError":
        with pytest.raises(ValueError):
            xc.xirr(dates, cashflows)
    else:
        result = xc.xirr(dates, cashflows)
        assert abs(result - expected) < 1e-4, f"Expected {expected}, got {result}"


@pytest.mark.parametrize(
    "dates,cashflow_template,target_xirr,expected",
    [
        # Single period cashflow → price should be exactly 100
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 110], 0.1, 100),
        # Single period cashflow → cashflow too low, it should return None
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 50], 0.1, None),
        # Multi-period cashflows → price should be exactly 100
        ([date(2025, 1, 1), date(2025, 7, 1), date(2026, 1, 1)], [0, 25, 84], 0.1, 100),
        # Multi-period cashflows → cashflow too low, it should return None
        ([date(2025, 1, 1), date(2025, 7, 1), date(2026, 1, 1)], [0, 2, 8], 0.1, None),
        # Zero target XIRR → price equals sum of all cashflows
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 100], 0.0, 100),
        # High target XIRR → price significantly discounted
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 150], 0.5, 100),
        # Negative target XIRR → price should be above total cashflow (premium)
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 100], -0.05, 105.26),
        # Long horizon (5 years), steady cashflows → heavily discounted price
        (
            [
                date(2025, 1, 1),
                date(2026, 1, 1),
                date(2027, 1, 1),
                date(2028, 1, 1),
                date(2029, 1, 1),
                date(2030, 1, 1),
            ],
            [0, 10, 10, 10, 10, 110],
            0.1,
            100,
        ),
        # Short horizon (sub-year, ~6 months) → price close to discounted FV
        ([date(2025, 1, 1), date(2025, 7, 1)], [0, 105], 0.1, 100.47),
        # Irregular cashflow spacing → still converges to correct price
        (
            [date(2025, 1, 1), date(2025, 4, 1), date(2025, 10, 1), date(2026, 6, 1)],
            [0, 5, 10, 90],
            0.1,
            92.83,
        ),
        # Very low target XIRR (1%) → price close to sum of cashflows
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 110], 0.01, 108.91),
        # Cashflow exactly at breakeven for target → boundary, should not return None
        ([date(2025, 1, 1), date(2026, 1, 1)], [0, 110], 0.1, 100),
        # All cashflows zero except last → pure zero-coupon bond pricing
        (
            [date(2025, 1, 1), date(2026, 1, 1), date(2027, 1, 1), date(2028, 1, 1)],
            [0, 0, 0, 133.1],
            0.1,
            100,
        ),
    ],
)
def test_calculate_price_for_target_xirr_binary(
    dates, cashflow_template, target_xirr, expected
):
    result = xc.calculate_price_for_target_xirr_binary(
        dates=dates, cashflow_template=cashflow_template, target_xirr=target_xirr
    )
    if expected is None:
        assert result is None, f"Expected None, got {result}"
    else:
        assert math.isclose(
            result, expected, rel_tol=0.01
        ), f"Expected {expected}, got {result}"

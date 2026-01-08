import math
import warnings
from datetime import date

import numpy as np
import pandas as pd
from scipy.optimize import newton

from src.data.config import DEFAULT_TARGET_XIRR


def xirr(dates, cashflows, guess=0.1):
    """Standard XIRR using Newton-Raphson."""
    # Convert each date to number of days from the first cashflow date
    # This is needed to calculate the time fraction for discounting

    days = np.array([(d - dates[0]).days for d in dates], dtype=float)

    # Define Net Present Value (NPV) function at a given rate
    # This calculates the present value of all cashflows discounted at rate
    def npv(rate):
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.sum(cashflows / (1 + rate) ** (days / 365.0))

    # Derivative of NPV with respect to rate (needed for Newton-Raphson)
    def d_npv(rate):
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.sum(
                -(days / 365.0) * cashflows / (1 + rate) ** (days / 365.0 + 1)
            )

    try:
        return newton(func=npv, x0=guess, fprime=d_npv, maxiter=100)
    except RuntimeError:
        # Fallback: if Newton-Raphson fails with initial guess, try a different starting
        return newton(func=npv, x0=0.2, fprime=d_npv, maxiter=100)


def forward_xirr(dates, cashflows, portfolio_value, guess=0.1):
    """
    Calculate forward XIRR from start_date onward using:
    - negative cashflow = portfolio_value on start_date
    - all future cashflows (d > start_date)
    """
    start_date = date.today()

    # Select future cashflows
    future = [(d, cf) for d, cf in zip(dates, cashflows) if d.date() > start_date]

    if len(future) == 0:
        return float("nan")

    # Insert portfolio investment as of today
    new_dates = [start_date] + [d for d, _ in future]
    new_cashflows = [-portfolio_value] + [cf for _, cf in future]

    # Convert to day counts (same way as xirr)
    new_dates = pd.to_datetime(new_dates)
    days = np.array([(d - new_dates[0]).days for d in new_dates], dtype=float)

    def npv(rate):
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.sum(new_cashflows / (1 + rate) ** (days / 365.0))

    def d_npv(rate):
        with np.errstate(invalid="ignore", divide="ignore"):
            return np.sum(
                -(days / 365.0) * new_cashflows / (1 + rate) ** (days / 365.0 + 1)
            )

    try:
        return newton(func=npv, x0=guess, fprime=d_npv, maxiter=100)
    except RuntimeError:
        return newton(func=npv, x0=0.2, fprime=d_npv, maxiter=100)


def calculate_price_for_target_xirr_binary(
    dates,
    cashflow_template,
    target_xirr=DEFAULT_TARGET_XIRR,
    start=80.0,
    end=110.0,
    tolerance=0.1,
):
    """
    Binary search to find the highest price that achieves XIRR >= target_xirr.
    Much faster than linear search.
    """
    try:
        best_price = None
        left, right = start, end

        # First check if end price meets target (unlikely but possible)
        cashflows_end = cashflow_template.copy()
        cashflows_end[0] = -end
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)
            try:
                irr_end = xirr(dates=dates, cashflows=cashflows_end)
                if irr_end is not None and not (
                    isinstance(irr_end, float) and math.isnan(irr_end)
                ):
                    if irr_end >= target_xirr:
                        return end
            except Exception:
                pass

        # Binary search
        while right - left > tolerance:
            mid = (left + right) / 2
            cashflows = cashflow_template.copy()
            cashflows[0] = -mid
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", RuntimeWarning)
                try:
                    irr = xirr(dates=dates, cashflows=cashflows)
                    if irr is None or (isinstance(irr, float) and math.isnan(irr)):
                        # Can't determine, assume it doesn't meet target
                        right = mid
                        continue
                    if irr >= target_xirr:
                        # IRR meets target, try higher price (lower IRR)
                        best_price = mid
                        left = mid
                    else:
                        # IRR below target, need lower price (higher IRR)
                        right = mid
                except Exception:
                    # On exception, assume it doesn't meet target
                    right = mid
                    continue
        # Round to 2 decimal places if we found a price
        if best_price is not None:
            best_price = round(best_price, 2)
        return best_price
    except Exception as e:
        print(f"Target Price for XIRR calculation failed: {e}")
        return None

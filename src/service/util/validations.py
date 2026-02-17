import sys

import pandas as pd

from src.data.config import RED_BOLD, RESET


def validate_coupon_rate_match(
    symbol: str,
    provided_coupon,
    derived_coupon,
    isin: str,
):
    """
    Rules:
    1. Both provided and derived can be None/NaN → OK
    2. Only one missing → FAIL
    3. Both present but mismatch → FAIL
    """

    provided_missing = pd.isna(provided_coupon)
    derived_missing = pd.isna(derived_coupon)

    # ✅ Both missing → allowed
    if provided_missing and derived_missing:
        return

    # ❌ Only one missing
    if provided_missing != derived_missing:
        print(f"\n{RED_BOLD}❌ COUPON PRESENCE MISMATCH ❌{RESET}")
        print(
            f"SYMBOL: {symbol} | ISIN: {isin} | "
            f"Provided Missing: {provided_missing} | "
            f"Derived Missing: {derived_missing}"
        )
        sys.exit(1)

    # ❌ Both present but mismatch
    if round(float(provided_coupon), 4) != round(float(derived_coupon), 4):
        print(f"\n{RED_BOLD}❌ COUPON MISMATCH ❌{RESET}")
        print(
            f"SYMBOL: {symbol} | ISIN: {isin} | "
            f"Provided: {provided_coupon} | "
            f"Derived: {derived_coupon}"
        )
        sys.exit(1)


def validate_maturity_year_consistency(
    symbol: str,
    provided_maturity,
    derived_maturity,
    isin: str,
):
    """
    Rules:
    1. Both provided and derived can be None/NaT → OK
    2. Only one missing → FAIL
    3. Both present but year mismatch → FAIL
    """

    provided_missing = pd.isna(provided_maturity)
    derived_missing = pd.isna(derived_maturity)

    # ✅ Both missing → allowed
    if provided_missing and derived_missing:
        return

    # ❌ Only one missing
    if provided_missing != derived_missing:
        print(f"\n{RED_BOLD}❌ MATURITY PRESENCE MISMATCH ❌{RESET}")
        print(
            f"SYMBOL: {symbol} | ISIN: {isin} | "
            f"Provided Missing: {provided_missing} | "
            f"Derived Missing: {derived_missing}"
        )
        sys.exit(1)

    # ❌ Both present but year mismatch
    provided_year = pd.to_datetime(provided_maturity).year
    derived_year = pd.to_datetime(derived_maturity).year

    if provided_year != derived_year:
        print(f"\n{RED_BOLD}❌ MATURITY YEAR MISMATCH ❌{RESET}")
        print(
            f"SYMBOL: {symbol} | ISIN: {isin} | "
            f"Provided: {provided_year} | "
            f"Derived: {derived_year}"
        )
        sys.exit(1)

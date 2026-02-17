import pandas as pd


def validate_coupon_rate_match(
    symbol_coupon: float,
    provided_coupon: float,
    symbol: str,
    isin: str,
) -> float:
    """
    Validates that coupon extracted from symbol
    matches provided coupon rate.

    Returns validated coupon rate.
    Raises ValueError if mismatch.
    """

    if provided_coupon is None:
        return symbol_coupon

    if symbol_coupon is None:
        raise ValueError(f"Missing coupon from symbol for {symbol} ({isin})")

    if round(symbol_coupon, 4) != round(provided_coupon, 4):
        raise ValueError(
            f"Coupon mismatch for {symbol} ({isin}) | "
            f"Symbol: {symbol_coupon} | Provided: {provided_coupon}"
        )

    return symbol_coupon


def validate_maturity_year_consistency(
    symbol: str,
    provided_maturity: str,
    derived_maturity: pd.Timestamp,
    isin: str,
):
    """
    Validates that maturity year from symbol
    matches provided maturity date year.
    """

    if not provided_maturity:
        raise ValueError(f"Missing maturity date for {symbol} ({isin})")

    provided_year = pd.to_datetime(provided_maturity).year
    derived_year = derived_maturity.year

    if provided_year != derived_year:
        raise ValueError(
            f"Maturity year mismatch for {symbol} ({isin}) | "
            f"Provided: {provided_year} | Derived: {derived_year}"
        )

import sys

import numpy as np
import pandas as pd

from src.data.config import RED_BOLD, RESET
from src.service.gsec.util.symbol_parsers import extract_maturity_date_from_symbol


def validate_coupon_rate_match(df: pd.DataFrame):
    required_cols = {"COUPON RATE_x", "COUPON RATE_y"}

    if not required_cols.issubset(df.columns):
        print(f"{RED_BOLD}ERROR: Missing COUPON RATE_x or COUPON RATE_y column{RESET}")
        sys.exit(1)

    # Validation column
    df["_coupon_rate_match"] = df["COUPON RATE_x"] == df["COUPON RATE_y"]
    # ignore those records which are not in portfolio
    df["_coupon_rate_match"] = np.where(
        df["COUPON RATE_y"].isna(), True, df["_coupon_rate_match"]
    )

    if not df["_coupon_rate_match"].all():
        mismatches = df[~df["_coupon_rate_match"]]
        print(
            f"{RED_BOLD}ERROR: Coupon rate mismatch "
            f"(exact match required up to 2 decimal places){RESET}"
        )
        print(
            mismatches[["SYMBOL", "ISIN", "COUPON RATE_x", "COUPON RATE_y"]].to_string(
                index=False
            )
        )
        print()

        sys.exit(1)

    # Cleanup temp columns
    df.drop(
        columns=["_coupon_rate_match"],
        inplace=True,
    )
    df.rename(columns={"COUPON RATE_x": "COUPON RATE"}, inplace=True)
    df.drop(columns=["COUPON RATE_y"], inplace=True)


def validate_maturity_year_consistency(df: pd.DataFrame) -> None:
    """
    Validate that the maturity YEAR derived from symbol logic matches
    the YEAR of maturity date from override file.

    This function is STRICTLY validation-only:
    - Does NOT modify MATURITY DATE
    - Prints mismatched records
    - Hard fails the process if mismatches are found
    """

    # Derive maturity date again using symbol logic (same formula)
    df["_DERIVED_MATURITY_DATE"] = df.apply(
        lambda row: extract_maturity_date_from_symbol(
            symbol=row["SYMBOL"],
            is_tbill=row["IS TBILL"],
            is_gsec_stripped=row["IS STRIPPED GSEC"],
        ),
        axis=1,
    )

    # Compare only YEAR component
    df["_MATURITY_YEAR_MATCH"] = (
        pd.to_datetime(df["MATURITY DATE"]).dt.year
        == df["_DERIVED_MATURITY_DATE"].dt.year
    )

    # Identify mismatches (ignore rows where either date is missing)
    mismatch_df = df[
        df["MATURITY DATE"].notna()
        & df["_DERIVED_MATURITY_DATE"].notna()
        & ~df["_MATURITY_YEAR_MATCH"]
    ]

    # Fail fast if mismatches exist
    if not mismatch_df.empty:
        print(f"\n{RED_BOLD}❌ MATURITY YEAR VALIDATION FAILED ❌{RESET}")
        print(
            mismatch_df.assign(
                MATURITY_YEAR=pd.to_datetime(mismatch_df["MATURITY DATE"]).dt.year,
                DERIVED_MATURITY_YEAR=mismatch_df["_DERIVED_MATURITY_DATE"].dt.year,
            )[
                [
                    "ISIN",
                    "SYMBOL",
                    "MATURITY_YEAR",
                    "DERIVED_MATURITY_YEAR",
                ]
            ].to_string(
                index=False
            )
        )
        print()
        sys.exit(1)

    # Cleanup validation-only columns
    df.drop(
        columns=["_DERIVED_MATURITY_DATE", "_MATURITY_YEAR_MATCH"],
        inplace=True,
    )

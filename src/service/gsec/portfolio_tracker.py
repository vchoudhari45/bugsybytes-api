import sys

import pandas as pd

from src.data.config import (
    GSEC_MATURITY_DATE_OVERRIDE_FILE,
    GSEC_PORTFOLIO_CASHFLOW_BY_YEAR_FILE,
    GSEC_PORTFOLIO_CASHFLOW_FILE,
    GSEC_PORTFOLIO_FILE,
)
from src.service.gsec.util.cashflow_generator import generate_cashflows


def portfolio_tracker():
    # Read CSVs
    gsec_maturity_date_override_df = pd.read_csv(GSEC_MATURITY_DATE_OVERRIDE_FILE)
    gsec_portfolio_df = pd.read_csv(GSEC_PORTFOLIO_FILE)

    # Strip column names
    gsec_portfolio_df.columns = gsec_portfolio_df.columns.str.strip()
    gsec_maturity_date_override_df.columns = (
        gsec_maturity_date_override_df.columns.str.strip()
    )

    # --- INNER JOIN ---
    joined_df = gsec_portfolio_df.merge(
        gsec_maturity_date_override_df,
        how="left",
        on="ISIN",
        indicator=True,
        validate="many_to_one",
    )

    # print error message in case GSEC_MATURITY_DATE_OVERRIDE_FILE doesn't have ISIN
    missing_isin = joined_df.loc[joined_df["_merge"] == "left_only", "ISIN"].unique()
    if len(missing_isin) > 0:
        print("ERROR: These ISINs are missing in GSEC_MATURITY_DATE_OVERRIDE_FILE:")
        for isin in missing_isin:
            print("  -", isin)
            sys.exit(1)

    # cleanup: drop _merge column
    joined_df = joined_df.drop(columns=["_merge"])
    joined_df = joined_df.drop(columns=["SYMBOL_x"])
    joined_df = joined_df.rename(columns={"SYMBOL_y": "SYMBOL"})

    # create cashflow_df
    cashflow_df, cashflow_metadata_df = generate_cashflows(joined_df)

    # debug
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    print("")
    print(f"Portfolio INVESTMENT: {cashflow_df.attrs["PORTFOLIO INVESTMENT"]}")
    print(f"Portfolio XIRR: {cashflow_df.attrs["PORTFOLIO XIRR"]:.6%}")
    print(f"Portfolio FORWARD XIRR: {cashflow_df.attrs["PORTFOLIO FORWARD XIRR"]:.6%}")

    print("")
    print("=" * 70)
    print("Cashflow Metadata: ")
    print("=" * 70)
    print(cashflow_metadata_df.to_string(index=False))

    cashflow_df["DATE"] = pd.to_datetime(cashflow_df["DATE"])

    # Aggregate by year
    yearly_cashflow = (
        cashflow_df.groupby(cashflow_df["DATE"].dt.to_period("Y"))["TOTAL CASHFLOW"]
        .sum()
        .reset_index()
    )
    yearly_cashflow["YEAR"] = yearly_cashflow["DATE"].dt.strftime("%Y")
    yearly_cashflow = yearly_cashflow.drop(columns=["DATE"])
    yearly_cashflow["TOTAL CASHFLOW"] = (
        yearly_cashflow["TOTAL CASHFLOW"]
        .astype(str)
        .str.replace(",", "", regex=False)
        .astype(float)
        .round(2)
    )

    # Aggregate by month
    monthly_cashflow = (
        cashflow_df.groupby(cashflow_df["DATE"].dt.to_period("M"))["TOTAL CASHFLOW"]
        .sum()
        .reset_index()
    )
    # Convert Period to Timestamp
    monthly_cashflow["DATE"] = monthly_cashflow["DATE"].dt.to_timestamp()
    # Extract month number and name
    monthly_cashflow["MONTH_NUM"] = monthly_cashflow["DATE"].dt.month
    monthly_cashflow["MONTH_NAME"] = monthly_cashflow["DATE"].dt.strftime("%b")
    # Take MIN cashflow per month across all years
    result = (
        monthly_cashflow.groupby(["MONTH_NUM", "MONTH_NAME"], as_index=False)[
            "TOTAL CASHFLOW"
        ]
        .agg(lambda x: x.mode().iloc[0])
        .sort_values("MONTH_NUM")
    )
    print("")
    print("=" * 70)
    print("Monthly Cashflow (Most Common per Month across all years):")
    print("=" * 70)
    for _, row in result.iterrows():
        print(f"{row['MONTH_NAME']}: {row['TOTAL CASHFLOW']:>12.2f}")

    print("")
    print("=" * 70)
    print("Transactions: ")
    # debug print
    for _, row in joined_df.iterrows():
        print("=" * 70)
        print(row)

    # debug save
    cashflow_df.to_csv(GSEC_PORTFOLIO_CASHFLOW_FILE, sep="\t", index=False)
    yearly_cashflow[["YEAR", "TOTAL CASHFLOW"]].to_csv(
        GSEC_PORTFOLIO_CASHFLOW_BY_YEAR_FILE, sep="\t", index=False
    )


# RUN/TEST USING __main__
if __name__ == "__main__":
    portfolio_tracker()

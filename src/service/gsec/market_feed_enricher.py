import sys
import traceback

import numpy as np
import pandas as pd

from src.data.config import DEFAULT_TARGET_XIRR, RED_BOLD, RESET
from src.service.gsec.util.cashflow_generator import build_gsec_cashflows
from src.service.gsec.util.validations import validate_coupon_rate_match
from src.service.gsec.util.ytm_calculator import calculate_gsec_ytm
from src.service.util.xirr_calculator import (
    calculate_price_for_target_xirr_binary,
    xirr,
)


def apply_ytm(row):
    try:
        # Skip if ASK PRICE is missing or invalid
        if row.get("ASK PRICE") is None or pd.isna(row["ASK PRICE"]):
            return None
        ytm = calculate_gsec_ytm(
            price=row["ASK PRICE"],
            coupon_rate=row["COUPON RATE"],
            maturity_date=row["MATURITY DATE"],
            is_tbill=row["IS TBILL"],
            is_stripped_gsec=row["IS STRIPPED GSEC"],
            face_value=row["FACE VALUE"],
        )
        return ytm
    except Exception as e:
        traceback.print_exc()
        print(f"YTM calculation failed for ISIN {row['ISIN']}: {e}")
        return None


def apply_xirr(row):
    try:
        # Skip if ASK PRICE is missing or invalid
        if row.get("ASK PRICE") is None or pd.isna(row["ASK PRICE"]):
            return None

        dates, cashflows = build_gsec_cashflows(row)
        # overwrite settlement cashflow with ask price
        cashflows[0] = -float(row["ASK PRICE"])
        result_xirr = xirr(dates=dates, cashflows=cashflows)
        return result_xirr * 100
    except Exception as e:
        # traceback.print_exc()
        print(f"XIRR calculation failed for ISIN {row.get('ISIN', 'UNKNOWN')}: {e}")
        return None


def apply_price_for_target_xirr(row, target_xirr):
    try:
        dates, cashflows = build_gsec_cashflows(row)
        return calculate_price_for_target_xirr_binary(
            dates=dates,
            cashflow_template=cashflows,
            target_xirr=target_xirr,
        )
    except Exception as e:
        traceback.print_exc()
        print(
            f"Price-for-XIRR calculation failed for ISIN {row.get('ISIN', 'UNKNOWN')}: {e}"
        )
        return None


def enrich_gsec_market_feed(
    message, nse_gsec_df, gsec_portfolio_df, target_xirr=DEFAULT_TARGET_XIRR
) -> pd.DataFrame:
    try:
        feeds = message.get("feeds", {})
        rows = []

        for key, value in feeds.items():
            isin = key.replace("NSE_EQ|", "")
            quotes = value["fullFeed"]["marketFF"]["marketLevel"]["bidAskQuote"]

            best_ask = quotes[0].get("askP")
            best_bid = quotes[0].get("bidP")
            ask_json = quotes

            rows.append(
                {
                    "ISIN": isin,
                    "ASK PRICE": best_ask,
                    "BID PRICE": best_bid,
                    "ASK DETAILS": ask_json,
                }
            )

        # If no valid rows, return empty DataFrame with same columns
        if not rows:
            return pd.DataFrame(
                columns=list(nse_gsec_df.columns)
                + ["ASK PRICE", "BID PRICE", "ASK DETAILS", "YTM"]
            )

        # print(rows)
        feeds_df = pd.DataFrame(rows)
        pd.set_option("display.max_rows", None)
        pd.set_option("display.max_columns", None)
        pd.set_option("display.width", None)
        pd.set_option("display.max_colwidth", None)

        # Use inner join to keep only matching ISINs
        enriched_df = nse_gsec_df.merge(
            feeds_df, on="ISIN", how="inner", validate="one_to_one"
        )
        enriched_df["YTM"] = enriched_df.apply(apply_ytm, axis=1)
        # Add column to calculate XIRR
        enriched_df["XIRR"] = enriched_df.apply(apply_xirr, axis=1)
        enriched_df["PRICE FOR TARGET XIRR"] = enriched_df.apply(
            apply_price_for_target_xirr, axis=1, target_xirr=target_xirr
        )
        enriched_df = enriched_df.sort_values(by="XIRR", ascending=False).reset_index(
            drop=True
        )

        # Signed units
        gsec_portfolio_df["signed_units"] = np.where(
            gsec_portfolio_df["EVENT TYPE"].str.upper() == "BUY",
            gsec_portfolio_df["UNITS"],
            -gsec_portfolio_df["UNITS"],
        )

        gsec_portfolio_df["buy_amount"] = np.where(
            gsec_portfolio_df["EVENT TYPE"].str.upper() == "BUY",
            gsec_portfolio_df["UNITS"] * gsec_portfolio_df["PRICE PER UNIT"],
            0.0,
        )

        summary_df = gsec_portfolio_df.groupby(
            ["SYMBOL", "ISIN", "COUPON RATE"], as_index=False
        ).agg(
            current_quantity=("signed_units", "sum"),
            total_buy_units=(
                "UNITS",
                lambda x: x[
                    gsec_portfolio_df.loc[x.index, "EVENT TYPE"] == "BUY"
                ].sum(),
            ),
            total_buy_amount=("buy_amount", "sum"),
        )

        summary_df["avg_price_paid"] = (
            summary_df["total_buy_amount"] / summary_df["total_buy_units"]
        )

        summary_df = summary_df[summary_df["current_quantity"] > 0]

        enriched_with_portfolio_df = enriched_df.merge(
            summary_df, how="left", on="ISIN", validate="one_to_one"
        )

        enriched_with_portfolio_df["current_quantity"] = enriched_with_portfolio_df[
            "current_quantity"
        ].fillna(0)

        enriched_with_portfolio_df["BID PRICE"] = (
            enriched_with_portfolio_df["BID PRICE"]
            .replace("-", None)
            .pipe(pd.to_numeric, errors="raise")
            .fillna(0)
        )

        # cleanup
        enriched_with_portfolio_df = enriched_with_portfolio_df.drop(
            columns=["SYMBOL_y", "total_buy_units"]
        )
        enriched_with_portfolio_df = enriched_with_portfolio_df.rename(
            columns={"SYMBOL_x": "SYMBOL"}
        )
        enriched_with_portfolio_df = enriched_with_portfolio_df.rename(
            columns={"current_quantity": "NET UNITS"}
        )
        enriched_with_portfolio_df = enriched_with_portfolio_df.rename(
            columns={"avg_price_paid": "AVG. PRICE PAID"}
        )
        enriched_with_portfolio_df["AVG. PRICE PAID"] = enriched_with_portfolio_df[
            "AVG. PRICE PAID"
        ].fillna(0)

        enriched_with_portfolio_df["PROFIT IF SOLD"] = np.where(
            (enriched_with_portfolio_df["NET UNITS"] > 0)
            & (enriched_with_portfolio_df["BID PRICE"] > 0)
            & (
                enriched_with_portfolio_df["BID PRICE"]
                > enriched_with_portfolio_df["AVG. PRICE PAID"]
            ),
            (
                enriched_with_portfolio_df["BID PRICE"]
                - enriched_with_portfolio_df["AVG. PRICE PAID"]
            )
            * enriched_with_portfolio_df["NET UNITS"],
            0.0,
        )

        # validation to match calculated coupon rate
        # to manually entered coupon rate in portfolio file
        validate_coupon_rate_match(enriched_with_portfolio_df)
        return enriched_with_portfolio_df
    except Exception as e:
        # Catch any unexpected errors in the function
        print(f"{RED_BOLD}ERROR in enrich_gsec_market_feed:{e}{RESET}")
        traceback.print_exc()
        sys.exit(1)

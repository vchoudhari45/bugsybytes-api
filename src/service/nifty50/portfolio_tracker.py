import pandas as pd

from src.data.config import (
    NIFTY50_NSE_PORTFOLIO_FILE,
    NSE_NIFTY50_PRE_MARKET_DATA_DIR,
    NSE_NIFTY50_UPSTOX_HOLDINGS_FILE,
)
from src.service.util.csv_util import read_all_dated_csv_files_from_folder


def convert_to_float(df: pd.DataFrame, columns: list[str]) -> None:
    """
    Remove commas, strip spaces and convert columns to float in-place
    """
    df[columns] = (
        df[columns]
        .astype(str)
        .replace("nan", pd.NA)
        .apply(lambda s: s.str.replace(",", "", regex=False).str.strip())
        .astype(float)
    )


def rebalance_with_additional_cash(
    df: pd.DataFrame, additional_cash: float
) -> tuple[pd.DataFrame, float, float]:
    result = df.copy()

    # ------------------------------------------------
    # STEP 1: current portfolio value
    # ------------------------------------------------
    current_total = result["CURRENT TOTAL PRICE"].sum()
    final_total = current_total + additional_cash
    first_time = False

    if final_total == 0:
        first_time = True
        nifty_spot_proxy = result["IEP"].mean()
        nifty_lot_size = 50
        final_total = nifty_spot_proxy * nifty_lot_size

    # ------------------------------------------------
    # STEP 2: target value as per index
    # ------------------------------------------------
    result["TARGET VALUE"] = final_total * result["LATEST INDEX WEIGHTAGE"]

    # Positive = underweight (buy), Negative = overweight (sell)
    result["DELTA VALUE"] = result["TARGET VALUE"] - result["CURRENT TOTAL PRICE"]

    # Initialize
    result["SUGGESTED SELL QUANTITY"] = 0
    result["SUGGESTED BUY QUANTITY"] = 0

    # ------------------------------------------------
    # STEP 3: SELL overweight stocks
    # ------------------------------------------------
    sells = result[result["DELTA VALUE"] < 0]

    for idx, row in sells.iterrows():
        price = row["IEP"]
        excess_value = -row["DELTA VALUE"]

        # How many shares can we sell
        sell_qty = min(row["NET QUANTITY"], int(excess_value // price))
        result.at[idx, "SUGGESTED SELL QUANTITY"] = sell_qty

    # ------------------------------------------------
    # STEP 4: BUY underweight stocks using available cash
    # ------------------------------------------------
    buys = result[result["DELTA VALUE"] > 0].sort_values("DELTA VALUE", ascending=False)

    for idx, row in buys.iterrows():
        price = row["IEP"]
        gap_value = row["DELTA VALUE"]

        # Shares required to fully close the gap
        required_qty = int(gap_value // price)
        required_qty = max(required_qty, 0)
        if first_time and required_qty == 0:
            required_qty = 1
        result.at[idx, "SUGGESTED BUY QUANTITY"] = required_qty

    # ------------------------------------------------
    # STEP 5: final portfolio
    # ------------------------------------------------
    result["SUGGESTED TOTAL QUANTITY"] = (
        result["NET QUANTITY"]
        - result["SUGGESTED SELL QUANTITY"]
        + result["SUGGESTED BUY QUANTITY"]
    )

    result["SUGGESTED INVESTMENT AMOUNT"] = (
        (result["SUGGESTED BUY QUANTITY"] * result["IEP"])
        - (result["SUGGESTED SELL QUANTITY"] * result["IEP"])
        + result["CURRENT TOTAL PRICE"]
    )

    total_amount_after_suggested_investment = result[
        "SUGGESTED INVESTMENT AMOUNT"
    ].sum()

    result["SUGGESTED INDEX WEIGHTAGE"] = (
        result["SUGGESTED INVESTMENT AMOUNT"] / total_amount_after_suggested_investment
    )

    projected_tracking_error = (
        (result["SUGGESTED INDEX WEIGHTAGE"] - result["CURRENT INDEX WEIGHTAGE"])
        .abs()
        .sum()
    )

    return result, projected_tracking_error, total_amount_after_suggested_investment


def process_nse_nifty_50_file():
    # settings
    pd.set_option("display.max_rows", None)
    pd.set_option("display.max_columns", None)
    pd.set_option("display.width", None)
    pd.set_option("display.max_colwidth", None)

    # Read lot order data
    nifty_50_holdings = pd.read_csv(NSE_NIFTY50_UPSTOX_HOLDINGS_FILE)

    # Normalize SYMBOL column
    nifty_50_holdings.columns = [
        "SYMBOL" if col.startswith("Symbol") else col
        for col in nifty_50_holdings.columns
    ]

    # Clean numeric fields
    convert_to_float(nifty_50_holdings, ["Avg. Price", "Current Value", "Net Qty"])

    # renaming columns
    nifty_50_holdings.rename(
        columns={
            "Avg. Price": "AVG. PRICE",
            "Net Qty": "NET QUANTITY",
            "Current Value": "CURRENT TOTAL PRICE",
        },
        inplace=True,
    )

    # print(nifty_50_holdings.to_string(index=False))

    # Read NIFTY 50 pre-market data
    nifty_50_pre_market_df = read_all_dated_csv_files_from_folder(
        NSE_NIFTY50_PRE_MARKET_DATA_DIR
    )
    nifty_50_pre_market_df.columns = nifty_50_pre_market_df.columns.str.upper()
    nifty_50_pre_market_df = nifty_50_pre_market_df.drop(
        columns=["DATE"], errors="raise"
    )
    nifty_50_pre_market_df = nifty_50_pre_market_df.rename(
        columns={"FFM CAP  (₹ CRORES)": "FFM CAP (₹ CRORES)"}
    )
    convert_to_float(nifty_50_pre_market_df, ["FFM CAP (₹ CRORES)", "IEP"])
    nifty_50_pre_market_df = nifty_50_pre_market_df[
        ["SYMBOL", "IEP", "FFM CAP (₹ CRORES)"]
    ]

    # Merge
    nifty_50_df = pd.merge(
        nifty_50_holdings,
        nifty_50_pre_market_df,
        on="SYMBOL",
        how="outer",
        validate="one_to_one",
    )
    # fillna after joining
    nifty_50_df["FFM CAP (₹ CRORES)"] = nifty_50_df["FFM CAP (₹ CRORES)"].fillna(0.0)
    nifty_50_df["AVG. PRICE"] = nifty_50_df["AVG. PRICE"].fillna(0.0)
    nifty_50_df["NET QUANTITY"] = nifty_50_df["NET QUANTITY"].fillna(0.0)
    nifty_50_df["CURRENT TOTAL PRICE"] = nifty_50_df["CURRENT TOTAL PRICE"].fillna(0.0)

    # calculate index weightage
    total_fcm_cap = nifty_50_df["FFM CAP (₹ CRORES)"].sum()
    nifty_50_df["LATEST INDEX WEIGHTAGE"] = (
        nifty_50_df["FFM CAP (₹ CRORES)"] / total_fcm_cap
    )

    # calculate actual index weightage
    total_investment = nifty_50_df["CURRENT TOTAL PRICE"].sum()
    nifty_50_df["CURRENT INDEX WEIGHTAGE"] = (
        nifty_50_df["CURRENT TOTAL PRICE"] / total_investment
    )

    tracking_error = (
        nifty_50_df["CURRENT INDEX WEIGHTAGE"]
        .sub(nifty_50_df["LATEST INDEX WEIGHTAGE"])
        .abs()
        .sum()
    )

    print(f"Current Total Investment: {total_investment:.2f}")
    print(f"Current Tracking Error: {tracking_error:.4f}")

    # nifty_50_df = pre-market + lot data
    nifty_50_df = nifty_50_df.sort_values("SYMBOL")
    user_input = input(
        "Enter investment amount (₹) or 0 to auto-calculate near current NIFTY 50 value: "
    ).strip()
    investment_amount = float(user_input) if user_input else 0.0

    # rebalance with new amount
    rebalance_df, projected_tracking_error, total_amount_after_suggested_investment = (
        rebalance_with_additional_cash(nifty_50_df, investment_amount)
    )
    print(f"Projected Tracking Error: {projected_tracking_error:.4}")
    print(
        f"Suggested Investment Amount: {(total_amount_after_suggested_investment - total_investment):.2f}"
    )
    print(
        f"Total Amount After Suggested Investment: {total_amount_after_suggested_investment:.2f}"
    )

    rebalance_df[
        [
            "SYMBOL",
            "AVG. PRICE",
            "NET QUANTITY",
            "CURRENT TOTAL PRICE",
            "CURRENT INDEX WEIGHTAGE",
            "FFM CAP (₹ CRORES)",
            "LATEST INDEX WEIGHTAGE",
            "SUGGESTED SELL QUANTITY",
            "SUGGESTED BUY QUANTITY",
            "SUGGESTED TOTAL QUANTITY",
            "IEP",
            "SUGGESTED INVESTMENT AMOUNT",
            "SUGGESTED INDEX WEIGHTAGE",
        ]
    ].to_csv(NIFTY50_NSE_PORTFOLIO_FILE, index=False)

    return rebalance_df


if __name__ == "__main__":
    process_nse_nifty_50_file()

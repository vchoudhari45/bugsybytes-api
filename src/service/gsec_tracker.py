import argparse
import signal
import sys
import traceback
from datetime import datetime

import pandas as pd
import upstox_client

from src.data.config import (
    DEFAULT_TARGET_XIRR,
    GSEC_MATURITY_DATE_OVERRIDE_FILE,
    NSE_GSEC_LIVE_DATA_DIR,
    RED_BOLD,
    RESET,
    UPSTOX_ACCESS_TOKEN,
)
from src.service.util.cashflow_generator import build_gsec_cashflows
from src.service.util.csv_util import read_all_dated_csv_files_from_folder
from src.service.util.symbol_parsers import extract_maturity_date_from_symbol
from src.service.util.validations import validate_maturity_year_consistency
from src.service.util.xirr_calculator import (
    calculate_price_for_target_xirr_binary,
    xirr,
)
from src.service.util.ytm_calculator import calculate_gsec_ytm


# NSE csv processor
def process_nse_gsec_csv(folder_path, override_file, include_historical=False):
    override_df = pd.read_csv(override_file)
    df = read_all_dated_csv_files_from_folder(folder_path)

    df["ISIN"] = df["ISIN"].str.strip()
    df["SYMBOL"] = df["SYMBOL"].str.strip()

    df = df[
        df["ISIN"].notna()
        & df["SYMBOL"].notna()
        & (df["ISIN"] != "")
        & (df["SYMBOL"] != "")
    ]

    # Keep only coupon GSEC
    df = df[(df["SERIES"] != "TB") & (~df["SYMBOL"].str.startswith("GS", na=False))]

    if not include_historical:
        df = df.sort_values(["SYMBOL", "DATE"]).drop_duplicates(
            subset=["ISIN"], keep="last"
        )

    df = df.merge(
        override_df[["ISIN", "MATURITY DATE", "COUPON RATE"]],
        on="ISIN",
        how="left",
        validate="one_to_one",
    )

    if df["MATURITY DATE"].isna().any():
        print(f"\n{RED_BOLD}❌ MISSING MATURITY DATE ❌{RESET}")
        print(df[df["MATURITY DATE"].isna()][["SYMBOL", "ISIN"]])
        sys.exit(1)

    if df["COUPON RATE"].isna().any():
        print(f"\n{RED_BOLD}❌ MISSING COUPON RATE ❌{RESET}")
        print(df[df["COUPON RATE"].isna()][["SYMBOL", "ISIN"]])
        sys.exit(1)

    df["MATURITY DATE"] = pd.to_datetime(df["MATURITY DATE"])

    # Strict Maturity Validation
    for _, row in df.iterrows():

        derived_maturity = extract_maturity_date_from_symbol(symbol=row["SYMBOL"])

        validate_maturity_year_consistency(
            symbol=row["SYMBOL"],
            provided_maturity=row["MATURITY DATE"],
            derived_maturity=derived_maturity,
            isin=row["ISIN"],
        )

    return df


# Market feed enricher
def enrich_gsec_market_feed(message, nse_gsec_df, target_xirr=DEFAULT_TARGET_XIRR):
    try:
        feeds = message.get("feeds", {})
        if not feeds:
            return pd.DataFrame()

        rows = [
            {
                "ISIN": k.replace("NSE_EQ|", ""),
                "ASK PRICE": v["fullFeed"]["marketFF"]["marketLevel"]["bidAskQuote"][
                    0
                ].get("askP"),
                "BID PRICE": v["fullFeed"]["marketFF"]["marketLevel"]["bidAskQuote"][
                    0
                ].get("bidP"),
            }
            for k, v in feeds.items()
        ]

        df = nse_gsec_df.merge(pd.DataFrame(rows), on="ISIN", how="inner")

        if df.empty:
            return df

        def compute(r):
            if pd.isna(r["ASK PRICE"]):
                return None, None, None

            ytm = calculate_gsec_ytm(
                price=r["ASK PRICE"],
                coupon_rate=r["COUPON RATE"],
                maturity_date=r["MATURITY DATE"],
                face_value=r["FACE VALUE"],
            )

            dates, cfs = build_gsec_cashflows(
                maturity_date=r["MATURITY DATE"],
                coupon_rate=r["COUPON RATE"],
            )

            cfs[0] = -float(r["ASK PRICE"])
            xirr_val = xirr(dates=dates, cashflows=cfs) * 100

            target_price = calculate_price_for_target_xirr_binary(
                dates=dates,
                cashflow_template=cfs,
                target_xirr=target_xirr,
            )

            return ytm, xirr_val, target_price

        df[["YTM", "XIRR", "PRICE FOR TARGET XIRR"]] = df.apply(
            compute, axis=1, result_type="expand"
        )

        df["BID PRICE"] = pd.to_numeric(df["BID PRICE"], errors="coerce").fillna(0)

        return df.sort_values("XIRR", ascending=False).reset_index(drop=True)

    except Exception as e:
        print(f"{RED_BOLD}ERROR: {e}{RESET}")
        traceback.print_exc()
        sys.exit(1)


# Streaming Entrypoint
nse_gsec_df = process_nse_gsec_csv(
    NSE_GSEC_LIVE_DATA_DIR, GSEC_MATURITY_DATE_OVERRIDE_FILE
)

today = datetime.today().strftime("%b %d %Y")
streamer = None

parser = argparse.ArgumentParser()
parser.add_argument("--target_xirr", type=float, default=DEFAULT_TARGET_XIRR)
args = parser.parse_args()


def on_message(message):
    df = enrich_gsec_market_feed(message, nse_gsec_df, args.target_xirr)
    df = df[df["XIRR"] > 7].sort_values("XIRR", ascending=False)

    if not df.empty:
        print("=" * 100)
        print(
            df[
                [
                    "SYMBOL",
                    "ISIN",
                    "YTM",
                    "XIRR",
                    "ASK PRICE",
                    "BID PRICE",
                    "PRICE FOR TARGET XIRR",
                ]
            ].to_string(index=False)
        )
        print("=" * 100)

        for _, row in df.iterrows():
            line = ",".join(
                [
                    f'"{x}"'
                    for x in [
                        "BUY",
                        row["SYMBOL"],
                        row["ISIN"],
                        row["COUPON RATE"],
                        2,
                        row["PRICE FOR TARGET XIRR"],
                        100,
                        100,
                        today,
                    ]
                ]
            )
            print(line)


def signal_handler(sig, frame):
    if streamer:
        streamer.disconnect()
    sys.exit(0)


def track_gsec():
    global streamer

    config = upstox_client.Configuration()
    config.access_token = UPSTOX_ACCESS_TOKEN

    keys = ("NSE_EQ|" + nse_gsec_df["ISIN"]).tolist()

    streamer = upstox_client.MarketDataStreamerV3(
        upstox_client.ApiClient(config), keys, "full"
    )

    streamer.on("message", on_message)
    signal.signal(signal.SIGINT, signal_handler)
    streamer.connect()


if __name__ == "__main__":
    track_gsec()

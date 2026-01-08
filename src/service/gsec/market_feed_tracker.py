import argparse
import signal
import sys
from datetime import datetime

import pandas as pd
import upstox_client

from src.data.config import (
    DEFAULT_TARGET_XIRR,
    GSEC_MATURITY_DATE_OVERRIDE_FILE,
    GSEC_PORTFOLIO_FILE,
    NSE_GSEC_LIVE_DATA_DIR,
    UPSTOX_ACCESS_TOKEN,
)
from src.service.gsec.market_feed_enricher import enrich_gsec_market_feed
from src.service.gsec.nse_csv_processor import process_nse_gsec_csv

nse_gsec_df = process_nse_gsec_csv(
    NSE_GSEC_LIVE_DATA_DIR, GSEC_MATURITY_DATE_OVERRIDE_FILE
)
gsec_portfolio_df = pd.read_csv(GSEC_PORTFOLIO_FILE)
today = datetime.today().strftime("%b %d %Y")
streamer = None
parser = argparse.ArgumentParser(description="Market Feed Enricher Parser")
parser.add_argument(
    "--target_xirr",
    type=float,
    default=DEFAULT_TARGET_XIRR,
    help=f"Target XIRR value (default: {DEFAULT_TARGET_XIRR})",
)
args = parser.parse_args()


def on_message(message):
    enriched_with_portfolio_df = enrich_gsec_market_feed(
        message, nse_gsec_df, gsec_portfolio_df, target_xirr=args.target_xirr
    )

    filtered_df = enriched_with_portfolio_df.loc[enriched_with_portfolio_df["XIRR"] > 7]
    if filtered_df is not None and not filtered_df.empty:
        print("=" * 130)
        print(
            filtered_df[
                [
                    "SYMBOL",
                    "ISIN",
                    "YTM",
                    "XIRR",
                    "ASK PRICE",
                    "BID PRICE",
                    "PRICE FOR TARGET XIRR",
                    "NET UNITS",
                    "AVG. PRICE PAID",
                    "PROFIT IF SOLD",
                ]
            ].to_string(index=False)
        )

        print("=" * 130)
        for _, row in filtered_df.iterrows():
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
    """Handle Ctrl+C gracefully"""
    print("\nDisconnecting...")
    if streamer:
        streamer.disconnect()
    print("Disconnected!")
    sys.exit(0)


def track_gsec():
    global streamer

    configuration = upstox_client.Configuration()
    configuration.access_token = UPSTOX_ACCESS_TOKEN

    isin_keys = ("NSE_EQ|" + nse_gsec_df["ISIN"]).tolist()
    print(f"Subscribing to {len(isin_keys)} instruments")
    streamer = upstox_client.MarketDataStreamerV3(
        upstox_client.ApiClient(configuration), isin_keys, "full"
    )

    streamer.on("message", on_message)

    signal.signal(signal.SIGINT, signal_handler)

    print("Connecting to Upstox... (Press Ctrl+C to stop)")
    streamer.connect()


# RUN/TEST USING __main__
if __name__ == "__main__":
    track_gsec()

from datetime import datetime

import pandas as pd

from src.service.gsec.market_feed_enricher import enrich_gsec_market_feed


def test_enrich_gsec_market_feed():
    # -------------------------
    # Market feed JSON
    # -------------------------
    message = {
        "feeds": {
            "NSE_EQ|IN002025Y198": {
                "fullFeed": {
                    "marketFF": {
                        "marketLevel": {
                            "bidAskQuote": [
                                {
                                    "bidQ": "10000",
                                    "bidP": 98.79,
                                    "askQ": "500",
                                    "askP": 98.2,
                                }
                            ]
                        }
                    }
                }
            },
            "NSE_EQ|IN0020110055": {
                "fullFeed": {
                    "marketFF": {
                        "marketLevel": {
                            "bidAskQuote": [
                                {
                                    "bidQ": "1",
                                    "bidP": 111.7,
                                    "askQ": "7459",
                                    "askP": 111.85,
                                }
                            ]
                        }
                    }
                }
            },
            "NSE_EQ|IN002024Z487": {
                "fullFeed": {
                    "marketFF": {
                        "marketLevel": {
                            "bidAskQuote": [
                                {
                                    "bidQ": "10000",
                                    "bidP": 98.15,
                                }  # no askP → skipped
                            ]
                        }
                    }
                }
            },
        }
    }

    # -------------------------
    # NSE GSEC master data
    # -------------------------
    nse_gsec_df = pd.DataFrame(
        {
            "SYMBOL": ["709GS2030", "709GS2031"],
            "ISIN": ["IN002025Y198", "IN0020110055"],
            "FACE VALUE": [100, 100],
            "COUPON RATE": [7.5, 6.5],
            "MATURITY DATE": [
                datetime(2030, 3, 31),
                datetime(2031, 3, 31),
            ],
            "IS TBILL": [False, False],
            "IS STRIPPED GSEC": [False, False],
        }
    )

    # -------------------------
    # Portfolio data (BUY + SELL)
    # -------------------------
    gsec_portfolio_df = pd.DataFrame(
        {
            "EVENT TYPE": ["BUY", "SELL"],
            "SYMBOL": ["709GS2030", "709GS2030"],
            "ISIN": ["IN002025Y198", "IN002025Y198"],
            "COUPON RATE": [7.5, 7.5],
            "COUPON FREQUENCY": [2, 2],
            "PRICE PER UNIT": [96.52, 99.10],
            "UNITS": [10000, 2000],
            "FACE VALUE": [100, 100],
            "EVENT DATE": [
                datetime(2025, 12, 1),
                datetime(2026, 1, 15),
            ],
        }
    )

    # -------------------------
    # Execute
    # -------------------------
    result_df = enrich_gsec_market_feed(
        message=message,
        nse_gsec_df=nse_gsec_df,
        gsec_portfolio_df=gsec_portfolio_df,
    )

    # -------------------------
    # Assertions
    # -------------------------

    # Expected columns from market enrichment
    for col in ["ASK PRICE", "BID PRICE", "ASK DETAILS", "YTM"]:
        assert col in result_df.columns

    # Inner join behavior
    assert set(result_df["ISIN"]) == {
        "IN002025Y198",
        "IN0020110055",
    }

    # ASK price preserved
    row = result_df[result_df["ISIN"] == "IN002025Y198"].iloc[0]
    assert row["ASK PRICE"] == 98.2
    assert row["ASK DETAILS"][0]["askP"] == 98.2

    row = result_df[result_df["ISIN"] == "IN0020110055"].iloc[0]
    assert row["ASK PRICE"] == 111.85
    assert row["ASK DETAILS"][0]["askP"] == 111.85

    # YTM calculated
    assert pd.notna(result_df["YTM"]).all()

    # ISIN without askP skipped
    assert "IN002024Z487" not in set(result_df["ISIN"])

from pathlib import Path

import numpy as np
import pandas as pd

from src.data.config import GSEC_MATURITY_DATE_OVERRIDE_FILE, NSE_GSEC_LIVE_DATA_DIR
from src.service.gsec.nse_csv_processor import (
    extract_coupon_from_symbol,
    process_nse_gsec_csv,
)


def test_extract_coupon_from_symbol_cases():
    test_cases = {
        "767GA33A": 7.67,
        "74GS2062": 7.4,
        "828GS2027": 8.28,
        "774AP32A": 7.74,
        "781GJ32": 7.81,
        "824GS2027": 8.24,
        "667GS2035": 6.67,
        "692GS2039": 6.92,
        "702GS2027": 7.02,
        "746GS2073": 7.46,
        "726GS2033": 7.26,
        "734GS2064": 7.34,
        "690GS2065": 6.9,
        "77MH33A": 7.7,
        "732GS2030": 7.32,
        "654GS2032": 6.54,
        "709GS2074": 7.09,
        "754GS2036": 7.54,
        "726GS2029": 7.26,
        "727MH39": 7.27,
        "704MH40": 7.04,
        "706GS2028": 7.06,
        "726GS2032": 7.26,
        "679GS2034A": 6.79,
        "724GS2055": 7.24,
        "76TN32": 7.6,
        "83GS2040": 8.3,
        "773GJ32": 7.73,
        "828GS2032": 8.28,
        "610GS2031": 6.1,
        "813GS2045": 8.13,
        "727GS2026": 7.27,
        "1018GS2026": 10.18,
        "733GS2026": 7.33,
        "769KA33A": 7.69,
        "679GS2034": 6.79,
        "74GJ26": 7.4,
        "833GS2036": 8.33,
        "679GS2029": 6.79,
        "645GS2029": 6.45,
        "733MH44": 7.33,
        "788GS2030": 7.88,
        "784TN52": 7.84,
        "76TN31": 7.6,
        "832GS2032": 8.32,
        "692AP41": 6.92,
        "706GS2046": 7.06,
        "726MH49": 7.26,
        "897GS2030": 8.97,
        "619GS2034": 6.19,
        "729TN54": 7.29,
        "683GS2039": 6.83,
        "824GS2033": 8.24,
        "766GJ32": 7.66,
        "585GS2030": 5.85,
        "733KL54": 7.33,
        "700TN29": 7,
        "707TN38": 7.07,
        "679GS2031": 6.79,
        "75AP32": 7.5,
        "815GS2026": 8.15,
        "759GS2026": 7.59,
        "613GS2028": 6.13,
        "74GS2035": 7.4,
        "746MH33": 7.46,
        "591GS2028": 5.91,
        "563GS2026": 5.63,
        "759GS2029": 7.59,
        "769GS2043": 7.69,
        "923GS2043": 9.23,
        "763MH36": 7.63,
        "676GS2061": 6.76,
        "622GS2035": 6.22,
        "702GS2031": 7.02,
        "739TN32": 7.39,
        "716GS2050": 7.16,
        "75TN31": 7.5,
        "795GS2032": 7.95,
        "718GS2033": 7.18,
        "577GS2030": 5.77,
        "648GS2035": 6.48,
        "601GS2028": 6.01,
        "717GS2030": 7.17,
        "762GS2039": 7.62,
        "883GS2041": 8.83,
        "723MH35": 7.23,
        "579GS2030": 5.79,
        "772GS2049": 7.72,
        "718GS2037": 7.18,
        "574GS2026": 5.74,
        "736GS2052": 7.36,
        "723GS2039": 7.23,
        "826GS2027": 8.26,
        "833GS2032": 8.33,
        "757GS2033": 7.57,
        "601GS2030": 6.01,
        "697RJ51A": 6.97,
        "722MH49": 7.22,
        "733AP43A": 7.33,
        "73GS2053": 7.3,
        "77AP29A": 7.7,
        "657GS2033": 6.57,
        "715AP37": 7.15,
        "703TN51": 7.03,
        "763GS2059": 7.63,
        "744AP36": 7.44,
        "709GS2054": 7.09,
        "741GS2036": 7.41,
        "833GS2026": 8.33,
        "633GS2035": 6.33,
        "68GS2060": 6.8,
        "695GS2061": 6.95,
        "741RJ34": 7.41,
        "92GS2030": 9.2,
        "679GS2027": 6.79,
        "710GS2029": 7.1,
        "748MH35": 7.48,
        "704GJ32": 7.04,
        "761GS2030": 7.61,
        "725GS2063": 7.25,
        "773GS2034": 7.73,
        "745AP37": 7.45,
        "736TN54": 7.36,
        "699GS2051": 6.99,
        "668GS2040": 6.68,
        "86GS2028": 8.6,
        "737GS2028": 7.37,
        "701MH32": 7.01,
        "717GS2028": 7.17,
        "776KL38": 7.76,
        "726MH41": 7.26,
        "767WB37": 7.67,
        "74CG30": 7.4,
        "668GS2031": 6.68,
        "749TN34": 7.49,
        "763GJ34": 7.63,
        "765TN30": 7.65,
        "697GR2034": 6.97,
        "664GS2027": 6.64,
        "664GS2035": 6.64,
        "772GS2055": 7.72,
        "697GS2026": 6.97,
        "817GS2044": 8.17,
        "794TN32": 7.94,
        "723GJ27": 7.23,
        "71GS2034": 7.1,
        "83GS2042": 8.3,
        "737GR2054": 7.37,
        "747WB44": 7.47,
        "675GS2029": 6.75,
        "75GS2034": 7.5,
        "738GS2027": 7.38,
        "698GR2054": 6.98,
        "704GS2029": 7.04,
        "628GS2032": 6.28,
        "699GS2026": 6.99,
        "667GS2050": 6.67,
        "719GS2060": 7.19,
    }

    for symbol, expected in test_cases.items():
        result = extract_coupon_from_symbol(symbol)
        if isinstance(expected, float) and np.isnan(expected):
            assert np.isnan(
                result
            ), f"FAILED for symbol={symbol} | expected NaN but got {result}"
        else:
            assert (
                result == expected
            ), f"FAILED for symbol={symbol} | expected {expected} but got {result}"


def test_process_nse_gsec_csv_using_src_data():
    # Ensure the folder exists
    assert Path(
        NSE_GSEC_LIVE_DATA_DIR
    ).exists(), f"{NSE_GSEC_LIVE_DATA_DIR} folder does not exist!"

    # Run your function on real CSV files
    df = process_nse_gsec_csv(NSE_GSEC_LIVE_DATA_DIR, GSEC_MATURITY_DATE_OVERRIDE_FILE)

    # --- Basic sanity checks ---

    # Must produce a non-empty dataframe
    assert not df.empty, "Returned dataframe is empty"

    # Should not contain duplicate ISINs
    assert df["ISIN"].is_unique, "ISIN duplicates found after processing"

    # Must contain coupon rate column
    assert "COUPON RATE" in df.columns

    # TBs and GS STRIPS must have NaN
    mask_nan = (df["SERIES"] == "TB") | (df["SYMBOL"].str.startswith("GS", na=False))
    assert (
        df.loc[mask_nan, "COUPON RATE"].isna().all()
    ), "COUPON RATE should be NaN for TBs or GS STRIPS"

    # Other GSec rows must have numeric coupon
    mask_coupon = ~mask_nan
    coupon_values = df.loc[mask_coupon, "COUPON RATE"]
    assert (
        coupon_values.notna().all()
    ), "COUPON RATE missing for some coupon-bearing GSecs"
    assert np.issubdtype(
        coupon_values.dtype, np.number
    ), "COUPON RATE must be numeric for coupon-bearing GSecs"

    # --- Check override file dates match ---
    override_df = pd.read_csv(GSEC_MATURITY_DATE_OVERRIDE_FILE)
    override_df["MATURITY DATE"] = pd.to_datetime(
        override_df["MATURITY DATE"], errors="raise"
    ).dt.date

    df["MATURITY DATE"] = pd.to_datetime(df["MATURITY DATE"], errors="raise").dt.date
    # Assert all dates are present
    assert df["MATURITY DATE"].notna().all(), "Some maturity dates are NaN!"

    for _, row in override_df.iterrows():
        isin = row["ISIN"]
        override_date = row["MATURITY DATE"]

        # skip ISINs not present in df
        if isin not in df["ISIN"].values:
            continue  # Ignore missing ones

        df_date = df.loc[df["ISIN"] == isin, "MATURITY DATE"].iloc[0]

        assert (
            df_date == override_date
        ), f"Maturity date override mismatch for {isin}: {df_date} != {override_date}"

    # print for debugging
    # pd.set_option("display.max_rows", None)
    # print(df.columns)
    # print(df.to_string(index=False))

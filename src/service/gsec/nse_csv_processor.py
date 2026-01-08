from typing import List

import pandas as pd

from src.data.config import RED_BOLD, RESET
from src.service.gsec.util.symbol_parsers import (
    extract_coupon_from_symbol,
    extract_maturity_date_from_symbol,
)
from src.service.gsec.util.validations import validate_maturity_year_consistency
from src.service.util.csv_util import read_all_dated_csv_files_from_folder


def process_nse_gsec_csv(
    nse_gsec_folder_path: str,
    gsec_maturity_date_override_file: str,
    include: List[str] = None,  # default GSEC only
    include_historical: bool = False,  # default = do NOT include history
) -> pd.DataFrame:

    include = [x.upper() for x in include] if include else ["GSEC"]

    # Read override file
    gsec_maturity_date_override_df = pd.read_csv(gsec_maturity_date_override_file)

    # Get CSV files
    nse_gsec_df = read_all_dated_csv_files_from_folder(nse_gsec_folder_path)

    # Cleanup
    nse_gsec_df["ISIN"] = nse_gsec_df["ISIN"].str.strip()
    nse_gsec_df["SYMBOL"] = nse_gsec_df["SYMBOL"].str.strip()

    # Apply Historical Behavior(default: No History)
    if not include_historical:
        nse_gsec_df = nse_gsec_df.sort_values(["SYMBOL", "DATE"])
        nse_gsec_df = nse_gsec_df.drop_duplicates(subset=["ISIN"], keep="last")

    # Classification
    nse_gsec_df["IS TBILL"] = nse_gsec_df["SERIES"].eq("TB")
    nse_gsec_df["IS STRIPPED GSEC"] = nse_gsec_df["SYMBOL"].str.startswith(
        "GS", na=False
    )
    nse_gsec_df["IS GSEC"] = ~(
        nse_gsec_df["IS TBILL"] | nse_gsec_df["IS STRIPPED GSEC"]
    )

    # Coupon rate logic
    nse_gsec_df["COUPON RATE"] = 0.0
    # Apply extract_coupon_from_symbol function only for GSEC
    mask_coupon = nse_gsec_df["IS GSEC"]
    nse_gsec_df.loc[mask_coupon, "COUPON RATE"] = (
        nse_gsec_df.loc[mask_coupon, "SYMBOL"]
        .astype(str)
        .apply(extract_coupon_from_symbol)
    )

    # Maturity date override merge
    nse_gsec_df = nse_gsec_df.merge(
        gsec_maturity_date_override_df[["ISIN", "MATURITY DATE"]],
        on="ISIN",
        how="left",
        indicator="MATURITY DATE OVERRIDE STATUS",
        validate="one_to_one",
    )
    nse_gsec_df["MATURITY DATE OVERRIDE STATUS"] = nse_gsec_df[
        "MATURITY DATE OVERRIDE STATUS"
    ].map({"both": True, "left_only": False})

    # Derive maturity date where missing
    nse_gsec_df["MATURITY DATE"] = nse_gsec_df["MATURITY DATE"].fillna(
        nse_gsec_df.apply(
            lambda row: extract_maturity_date_from_symbol(
                symbol=row["SYMBOL"],
                is_tbill=row["IS TBILL"],
                is_gsec_stripped=row["IS STRIPPED GSEC"],
            ),
            axis=1,
        )
    )

    # Format maturity date
    nse_gsec_df["MATURITY DATE"] = pd.to_datetime(
        nse_gsec_df["MATURITY DATE"], errors="raise"
    ).dt.strftime("%b %d, %Y")

    # validation for maturity year
    validate_maturity_year_consistency(nse_gsec_df)

    # Apply include filter
    type_to_column = {
        "TBILL": "IS TBILL",
        "STRIPPED": "IS STRIPPED GSEC",
        "GSEC": "IS GSEC",
    }
    cols = [type_to_column[t] for t in include]
    final_filter = nse_gsec_df[cols].any(axis=1)
    nse_gsec_df = nse_gsec_df[final_filter]

    # print records where maturity date is missing
    missing_maturity_override_date_df = nse_gsec_df[
        nse_gsec_df["MATURITY DATE OVERRIDE STATUS"].eq(False)
    ]
    if not missing_maturity_override_date_df.empty:
        print(
            f"\n{RED_BOLD}WARNING: Maturity date is not defined for the following records:{RESET}"
        )
        print(
            missing_maturity_override_date_df[["SYMBOL", "ISIN"]].to_string(index=False)
        )

    # print(nse_gsec_df)
    return nse_gsec_df

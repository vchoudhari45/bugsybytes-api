import numpy as np
import pandas as pd


def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    # Normalize column names
    df.columns = df.columns.str.strip()

    # Replace "-" with NaN globally
    df = df.replace("-", np.nan)

    # Strip all string/object columns
    for col in df.select_dtypes(include=["object", "string"]).columns:
        df[col] = df[col].astype(str).str.strip()

    # Try converting columns to numeric
    for col in df.columns:
        converted = pd.to_numeric(df[col], errors="coerce")

        # if most values are numeric → treat as numeric
        if converted.notna().sum() > 0 and (converted.notna().sum() / len(df) > 0.7):
            df[col] = converted

    # Try converting columns to datetime
    for col in df.columns:
        if df[col].dtype == "object":
            converted = pd.to_datetime(df[col], errors="coerce")

            if converted.notna().sum() > 0 and (
                converted.notna().sum() / len(df) > 0.7
            ):
                df[col] = converted

    return df


def assert_dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    import numpy as np
    import pandas as pd

    # Normalize first
    df1 = normalize_df(df1)
    df2 = normalize_df(df2)

    # Shape check
    assert df1.shape == df2.shape, f"Shape mismatch: {df1.shape} != {df2.shape}"

    # Column check
    assert list(df1.columns) == list(
        df2.columns
    ), f"Column mismatch:\n{df1.columns}\n!=\n{df2.columns}"

    # Sort (important for deterministic compare)
    sort_cols = list(df1.columns)
    df1 = df1.sort_values(by=sort_cols).reset_index(drop=True)
    df2 = df2.sort_values(by=sort_cols).reset_index(drop=True)

    # Compare column-wise
    for col in df1.columns:
        s1 = df1[col]
        s2 = df2[col]

        # dtype check
        assert s1.dtype == s2.dtype, f"dtype mismatch in column '{col}'"

        if pd.api.types.is_numeric_dtype(s1):
            if not np.allclose(s1.fillna(0), s2.fillna(0)):
                raise AssertionError(f"Numeric mismatch in column '{col}'")

        elif pd.api.types.is_datetime64_any_dtype(s1):
            if not s1.fillna(pd.Timestamp(0)).equals(s2.fillna(pd.Timestamp(0))):
                raise AssertionError(f"Datetime mismatch in column '{col}'")

        else:
            if not s1.fillna("NA").equals(s2.fillna("NA")):
                raise AssertionError(f"Value mismatch in column '{col}'")

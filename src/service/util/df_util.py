import numpy as np
import pandas as pd


def json_to_df(data, parent_keys=None):
    rows = []

    def flatten(obj, parent=None):
        parent = parent or {}

        if isinstance(obj, dict):
            new_parent = parent.copy()
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    flatten(v, {**new_parent, k: None})
                else:
                    new_parent[k] = v
            rows.append(new_parent)

        elif isinstance(obj, list):
            for item in obj:
                flatten(item, parent)

    flatten(data)

    df = pd.DataFrame(rows)

    # try converting numeric columns
    for col in df.columns:
        try:
            df[col] = pd.to_numeric(df[col])
        except (ValueError, TypeError):
            pass

    return df


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

        if converted.notna().sum() > 0 and (converted.notna().sum() / len(df) > 0.7):
            original_non_null = df[col].dropna()
            all_numeric_compatible = (
                pd.to_numeric(original_non_null, errors="coerce").notna().all()
            )
            if all_numeric_compatible:
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


def _format_side_by_side(r1: pd.Series, r2: pd.Series) -> str:
    lines = []
    for col in r1.index:
        v1 = "NA" if pd.isna(r1[col]) else r1[col]
        v2 = "NA" if pd.isna(r2[col]) else r2[col]
        lines.append(f"{col}:\n  df1: {v1}\n  df2: {v2}")
    return "\n".join(lines)


def assert_dataframes_equal(df1: pd.DataFrame, df2: pd.DataFrame):
    # Normalize first
    df1 = normalize_df(df1)
    df2 = normalize_df(df2)

    # Shape check
    assert df1.shape == df2.shape, f"Shape mismatch: {df1.shape} != {df2.shape}"

    # Column check
    assert list(df1.columns) == list(
        df2.columns
    ), f"Column mismatch:\n{df1.columns}\n!=\n{df2.columns}"

    # Sort for deterministic compare
    sort_cols = list(df1.columns)
    df1 = df1.sort_values(by=sort_cols).reset_index(drop=True)
    df2 = df2.sort_values(by=sort_cols).reset_index(drop=True)

    # Row-wise comparison
    for i in range(len(df1)):
        r1 = df1.iloc[i]
        r2 = df2.iloc[i]

        for col in df1.columns:
            v1 = r1[col]
            v2 = r2[col]

            if pd.api.types.is_numeric_dtype(df1[col]):
                try:
                    f1 = float(v1) if not pd.isna(v1) else 0.0
                    f2 = float(v2) if not pd.isna(v2) else 0.0
                except (ValueError, TypeError):
                    f1, f2 = str(v1), str(v2)
                    equal = f1 == f2
                else:
                    equal = np.isclose(f1, f2, atol=0.01, rtol=0, equal_nan=True)

            elif pd.api.types.is_datetime64_any_dtype(df1[col]):
                t1 = pd.Timestamp(0) if pd.isna(v1) else v1
                t2 = pd.Timestamp(0) if pd.isna(v2) else v2
                equal = t1 == t2
            else:
                try:
                    v1 = "NA" if pd.isna(v1) else str(v1)
                    v2 = "NA" if pd.isna(v2) else str(v2)
                except (TypeError, ValueError):
                    v1 = str(v1)
                    v2 = str(v2)
                equal = v1 == v2

            if not equal:
                raise AssertionError(
                    f"\n❌ Row {i}, Column '{col}' mismatch\n\n"
                    f"{_format_side_by_side(r1, r2)}\n"
                )

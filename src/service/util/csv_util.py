import csv
from pathlib import Path

import pandas as pd


def normalized_dict_reader(file_obj, **kwargs):
    reader = csv.DictReader(file_obj, **kwargs)
    reader.fieldnames = [h.strip() for h in reader.fieldnames]
    return reader


def non_comment_lines(file_obj):
    """Yield only non-comment, non-empty lines"""
    for line in file_obj:
        if line.lstrip().startswith("#"):
            continue
        if not line.strip():
            continue
        yield line


def read_all_dated_csv_files_from_folder(
    csv_folder: str | Path, sep: str | None = None
) -> pd.DataFrame:
    csv_folder = Path(csv_folder)

    if not csv_folder.exists() or not csv_folder.is_dir():
        raise FileNotFoundError(f"Folder does not exist: {csv_folder}")

    csv_files = sorted(csv_folder.glob("*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV files found in {csv_folder}")

    df_list = []

    for file_path in csv_files:
        date_str = file_path.stem

        # Try multiple date formats
        file_date = None
        for fmt in ("%d-%b-%Y", "%Y%m%d"):
            try:
                file_date = pd.to_datetime(date_str, format=fmt)
                break
            except ValueError:
                continue

        if file_date is None:
            raise ValueError(
                f"Filename does not match supported date formats: {file_path.name}"
            )

        if sep is None:
            df = pd.read_csv(
                file_path,
                encoding="utf-8-sig",
            )
        else:
            df = pd.read_csv(file_path, encoding="utf-8-sig", sep=sep)

        # Column name cleaning
        df.columns = (
            df.columns.str.strip()
            .str.replace("\ufeff", "", regex=False)
            .str.replace("\n", "", regex=False)
            .str.upper()
        )

        # Add date column
        df["DATE"] = file_date

        df_list.append(df)

    df_list = [df for df in df_list if not df.empty and not df.isna().all().all()]
    return pd.concat(df_list, ignore_index=True)

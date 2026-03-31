"""
Data cleaning utilities for the geothermal wells dataset.

Objectives:
  - Remove columns that are entirely empty or zero-filled.
  - Drop rows that are missing Lat83 or Long83 coordinates.
  - Produce a cleaned DataFrame ready for downstream processing or export.
"""

import pandas as pd
from pathlib import Path


OUTPUT_CSV = Path(__file__).parent.parent / "Data" / "Processed" / "geothermal_wells_cleaned.csv"


def drop_empty_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove columns that contain no useful data.

    A column is considered empty if every value is either NaN or zero.
    These columns were identified during data inspection (e.g. ReleaseDate,
    OperatorStatus, FieldCode, AreaCode, AreaName, Directional, Redrill,
    SpudDate, ABDdate, CompDate).

    Args:
        df (pd.DataFrame): The raw DataFrame to clean.

    Returns:
        pd.DataFrame: DataFrame with empty/zero-filled columns removed.
    """
    empty_columns = df.columns[
        df.apply(lambda col: col.isna().all() or (col.fillna(0) == 0).all())
    ].tolist()

    df = df.drop(columns=empty_columns)
    print(f"Dropped {len(empty_columns)} empty columns: {empty_columns}")
    return df


def drop_missing_coordinates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove rows that are missing a Lat83 or Long83 coordinate.

    Wells without both coordinates cannot be plotted on a map and are
    not useful for geospatial analysis.

    Args:
        df (pd.DataFrame): The DataFrame to filter.

    Returns:
        pd.DataFrame: DataFrame with rows missing either coordinate removed.
    """
    before = len(df)
    df = df.dropna(subset=["Lat83", "Long83"])
    dropped = before - len(df)
    print(f"Dropped {dropped} rows missing Lat83 or Long83 coordinates")
    return df


def clean_data(df: pd.DataFrame, output_csv: Path | str = OUTPUT_CSV) -> pd.DataFrame:
    """
    Run all cleaning steps on the raw geothermal wells DataFrame and save
    the result to a CSV file.

    Steps performed:
      1. Drop columns that are entirely empty or zero-filled.
      2. Drop rows missing a Lat83 or Long83 coordinate.
      3. Write the cleaned DataFrame to `output_csv`.

    Args:
        df (pd.DataFrame): The raw DataFrame loaded from the CSV file.
        output_csv (str): Path where the cleaned CSV will be saved.
                          Defaults to OUTPUT_CSV.

    Returns:
        pd.DataFrame: The cleaned DataFrame.
    """
    print(f"Starting shape — Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    df = drop_empty_columns(df)
    df = drop_missing_coordinates(df)

    print(f"Final shape   — Rows: {df.shape[0]}, Columns: {df.shape[1]}")

    out_path = Path(output_csv)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Saved cleaned data to {out_path}")

    return df


if __name__ == "__main__":
    # Resolve the raw CSV relative to this script's location so the module
    # can be run directly from any working directory.
    csv_path = Path(__file__).parent.parent / "Data" / "Raw" / "geothermal_wells_ca.csv"

    df_raw = pd.read_csv(csv_path)
    df_clean = clean_data(df_raw)
    print(df_clean.head())
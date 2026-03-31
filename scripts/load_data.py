"""
Utilities for loading raw data files into pandas DataFrames.
"""

import pandas as pd


def load_raw_data(path, **kwargs):
    """
    Load a CSV file into a pandas DataFrame.

    Args:
        path (str): File path to the CSV file to load.
        **kwargs: Any additional keyword arguments are forwarded directly
                  to pandas.read_csv (e.g. dtype, usecols, skiprows).

    Returns:
        pandas.DataFrame: The contents of the CSV file.
    """
    return pd.read_csv(path, **kwargs)
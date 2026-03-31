import pandas as pd


def load_raw_data(path, **kwargs):
    return pd.read_csv(path, **kwargs)
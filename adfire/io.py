import pandas as pd


def read_record(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    return df


def write_record(df, path):
    df.to_csv(path, index=False)


def read_checksum(path) -> pd.Series:
    ser = pd.read_pickle(path)
    return ser


def write_checksum(hash, path):
    hash.to_pickle(path)

import pandas as pd


def read_record(path) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str)
    return df


def write_record(df, path):
    df.to_csv(path)

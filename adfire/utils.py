import numpy as np
import pandas as pd


def is_id_transaction_match(a: pd.Series, b: pd.Series) -> bool:
    return True

def is_checksum_subset(a: pd.Series, b: pd.Series) -> bool:
    return np.isin(a, b).all()

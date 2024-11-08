import numpy as np
import pandas as pd


def is_id_transaction_match(a: pd.Series, b: pd.Series) -> bool:
    df = pd.DataFrame({'a': a, 'b': b})
    return (pd.concat([
        df.groupby(['a', 'b']).size().groupby('a').size(),
        df.groupby(['b', 'a']).size().groupby('b').size()
    ]) == 1).all()

def is_checksum_subset(a: pd.Series, b: pd.Series) -> bool:
    return np.isin(a, b).all()

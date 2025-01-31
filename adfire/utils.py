import numpy as np
import pandas as pd
from pandera.typing import DataFrame

from adfire.schema import MergedInputEntrySchema


def get_worths(df: DataFrame[MergedInputEntrySchema]) -> pd.Series:
    return np.where(df['account_type'].isin(['credit', 'loan']), -df['amount'], df['amount'])

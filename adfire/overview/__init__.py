import numpy as np
import pandas as pd
import pandera as pa

from adfire.io import write_json
from adfire.lint import BaseInputSchema


class OverviewTransactionSchema(BaseInputSchema):
    date: str
    account_name: str
    name: str
    amount: float
    symbol: str
    category: str = pa.Field(nullable=True)

    class Config:
        strict = 'filter'


def report(df: pd.DataFrame):
    df = OverviewTransactionSchema(df)
    df = df.replace({np.nan: None})
    categorized_df = df[df['category'].notna()]
    categorized_df = categorized_df.sort_values('date')
    obj = categorized_df.to_dict('records')
    write_json(obj, '.reports/index.json')

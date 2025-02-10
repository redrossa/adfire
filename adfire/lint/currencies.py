import pandas as pd

from adfire.lint.base import BaseInputSchema


class CurrencySchema(BaseInputSchema):
    amount: float
    symbol: str
    rate: float


def lint(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    df: pd.DataFrame = CurrencySchema(df)
    df['worth'] = df['amount'] * df['rate']
    return df
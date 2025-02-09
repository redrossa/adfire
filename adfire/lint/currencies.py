import pandas as pd

from adfire.lint.base import BaseLinter, BaseInputSchema


class CurrencySchema(BaseInputSchema):
    amount: float
    symbol: str
    rate: float


class CurrencyLinter(BaseLinter):
    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        df: pd.DataFrame = CurrencySchema(df)
        df['worth'] = df['amount'] * df['rate']
        return df


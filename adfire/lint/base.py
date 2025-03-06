import pandas as pd
import pandera as pa
from pandera.typing import Index, Date


class BaseInputSchema(pa.DataFrameModel):
    path: Index[str]
    entry_id: Index[int]

    class Config:
        strict = False
        coerce = True


class OutputSchema(BaseInputSchema):
    date: Date
    account: str
    mask: str = pa.Field(nullable=True)
    name: str
    amount: float
    symbol: str
    rate: str
    category: str = pa.Field(nullable=True)

    class Config:
        strict = 'filter'
        coerce = True


def lint(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    import adfire.lint.accounts as accounts
    return accounts.lint(df, **kwargs)
import pandas as pd
import pandera as pa
from pandera.typing import Index

class BaseInputSchema(pa.DataFrameModel):
    path: Index[str]
    entry_id: Index[int]

    class Config:
        strict = False
        coerce = True


def lint(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    import adfire.lint.accounts as accounts
    return accounts.lint(df, **kwargs)
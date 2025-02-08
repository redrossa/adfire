import pandas as pd
import pandera as pa

from adfire.lint.base import BaseInputSchema, BaseLinter


class CoreSchema(BaseInputSchema):
    date: pa.DateTime
    amount: float
    account: str
    name: str


class CoreLinter(BaseLinter):
    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        return CoreSchema(df)
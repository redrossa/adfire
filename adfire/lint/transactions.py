import numpy as np
import pandas as pd

from adfire.lint.base import BaseLinter
from adfire.lint.core import CoreSchema
from adfire.lint.exceptions import LintError


class TransactionSchema(CoreSchema):
    worth: float
    account: str
    name: str


class TransactionLinter(BaseLinter):
    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        df = TransactionSchema(df)
        sum_by_transaction = df.groupby('name')['worth'].sum()
        non_zero_sums = ~np.isclose(sum_by_transaction, 0)
        if any(non_zero_sums):
            non_zero_txs = sum_by_transaction.index[non_zero_sums]
            invalid_entries = df[df['name'].isin(non_zero_txs)]
            raise LintError("Transactions have non-zero input and output difference", invalid_entries)
        return df

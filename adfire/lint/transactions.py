import numpy as np
import pandas as pd

import adfire.lint.currencies as currencies
from adfire.lint.base import BaseInputSchema
from adfire.lint.exceptions import LintError
from adfire.lint.utils import filter_df_by_schema


class TransactionSchema(BaseInputSchema):
    worth: float
    name: str


def lint(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    df = currencies.lint(df, **kwargs)
    df: pd.DataFrame = TransactionSchema(df)

    # find transactions whose input and output difference is not zero
    sum_by_transaction = df.groupby('name')['worth'].sum()
    non_zero_sums = ~np.isclose(sum_by_transaction, 0)

    if any(non_zero_sums):
        non_zero_txs = sum_by_transaction.index[non_zero_sums]
        invalid_df = df[df['name'].isin(non_zero_txs)]
        filtered_df = filter_df_by_schema(invalid_df, TransactionSchema.to_schema())
        raise LintError("Transactions have non-zero input and output difference", filtered_df)

    return df
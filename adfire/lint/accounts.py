import pandas as pd
import pandera as pa

import adfire.lint.transactions as transactions
from adfire.lint import BaseInputSchema
from adfire.lint.exceptions import LintError
from adfire.lint.utils import filter_df_by_schema


class AccountSchema(BaseInputSchema):
    account_name: str
    account_mask: str = pa.Field(nullable=True)


def parse_config(config) -> pd.DataFrame:
    accounts = config.accounts

    rows = []
    for i, account in enumerate(accounts):
        masks = getattr(account, 'masks', [None])
        names = getattr(account, 'names')
        rows += [(x, y, i) for x in names for y in masks]

    df = pd.DataFrame(rows, columns=['account_name', 'account_mask', 'account_id'])
    df = df.set_index(['account_name', 'account_mask'])
    return df


def lint(df: pd.DataFrame, **kwargs) -> pd.DataFrame:
    df = transactions.lint(df, **kwargs)
    df: pd.DataFrame = AccountSchema(df)

    config = kwargs['config']
    mapping_df = parse_config(config)

    # identify entries with invalid (account_mask, account_name) pairs
    has_account_mask_df = df[df['account_mask'].notna()]
    reindexed_df = has_account_mask_df.reset_index().set_index(mapping_df.index.names)
    valid_masked_indexes = reindexed_df.index.intersection(mapping_df.index)
    invalid_df = reindexed_df.loc[reindexed_df.index.difference(valid_masked_indexes)].reset_index().set_index(has_account_mask_df.index.names)

    if not invalid_df.empty:
        invalid_df = df.loc[invalid_df.index]
        filtered_df = filter_df_by_schema(invalid_df, AccountSchema.to_schema())
        raise LintError('Entries have unregistered account name and mask pairs', filtered_df)

    return df
from collections import defaultdict
from types import SimpleNamespace

import pandas as pd
import pandera as pa

from adfire.lint.base import BaseInputSchema, BaseLinter
from adfire.lint.exceptions import LintError
from adfire.lint.transactions import TransactionLinter
from adfire.lint.utils import filter_df_by_schema


class AccountSchema(BaseInputSchema):
    account_name: str
    account_mask: str = pa.Field(nullable=True)


class AccountLinter(TransactionLinter):
    @staticmethod
    def get_name_mask_pairs(config):
        pairs = []
        for account in config:
            masks = getattr(account, 'masks', [None])
            names = getattr(account, 'names')
            pairs += [(x, y) for x in names for y in masks]
        return pairs


    def __init__(self, **kwargs):
        accounts_config = kwargs['config'].accounts
        if not accounts_config:
            return
        self.name_mask_pairs = AccountLinter.get_name_mask_pairs(accounts_config)

    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().lint(df)
        df: pd.DataFrame = AccountSchema(df)

        # create a dataframe of (account_mask, account_name) pairs
        valid_df = pd.DataFrame(self.name_mask_pairs, columns=['account_name', 'account_mask'])

        # identify entries with invalid (account_mask, account_name) pairs
        has_account_mask_df = df[df['account_mask'].notna()].reset_index()
        merged_df = has_account_mask_df.merge(valid_df, on=['account_name', 'account_mask'], how='left', indicator=True)
        merged_df = merged_df.set_index(['path', 'entry_id'])
        invalid_df = merged_df[merged_df['_merge'] == 'left_only']

        if not invalid_df.empty:
            invalid_df = df.loc[invalid_df.index]
            filtered_df = filter_df_by_schema(invalid_df, AccountSchema.to_schema())
            raise LintError('Entries have unregistered account name and mask pairs', filtered_df)

        return df


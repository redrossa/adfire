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
    def get_user_names(config):
        user_names = {}
        for account in config:
            account_name = getattr(account, 'names', [])
            for user in getattr(account, 'users', []):
                user_name = getattr(user, 'name')
                for mask in getattr(user, 'masks', [None]):  # None to capture account users without masks
                    for name in account_name:
                        key = (mask, name)
                        user_names[key] = user_name if key not in user_names else {}
        return user_names


    def __init__(self, **kwargs):
        accounts_config = kwargs['config'].accounts
        if not accounts_config:
            return
        self.user_names = AccountLinter.get_user_names(accounts_config)

    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        df = super().lint(df)
        df: pd.DataFrame = AccountSchema(df)

        # create a dataframe of (account_mask, account_name) pairs
        valid_df = pd.DataFrame(self.user_names.keys(), columns=['account_mask', 'account_name'])

        # identify entries with invalid (account_mask, account_name) pairs
        has_account_mask_df = df[df['account_mask'].notna()].reset_index()
        merged_df = has_account_mask_df.merge(valid_df, on=['account_mask', 'account_name'], how='left', indicator=True)
        merged_df = merged_df.set_index(['path', 'entry_id'])
        invalid_df = merged_df[merged_df['_merge'] == 'left_only']

        if not invalid_df.empty:
            invalid_df = df.loc[invalid_df.index]
            filtered_df = filter_df_by_schema(invalid_df, AccountSchema.to_schema())
            raise LintError('Entries have unregistered account name and mask pairs', filtered_df)

        return df


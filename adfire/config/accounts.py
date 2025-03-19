import pandas as pd
from pandera import Field

from adfire.config.base import BaseConfig, BaseInputSchema


class AccountSchema(BaseInputSchema):
    account: str
    mask: str = Field(nullable=True)


class AccountsConfig(BaseConfig):
    def __init__(self, accounts=None, **kwargs):
        super(AccountsConfig, self).__init__(**kwargs)
        if accounts is None:
            accounts = []
            
        rows = []
        for i, account in enumerate(accounts):
            masks = account.get('masks', [None])
            names = account.get('names')
            rows += [(x, y, i) for x in names for y in masks]

        df = pd.DataFrame(rows, columns=['name', 'mask', 'id'])
        df = df.set_index(['name', 'mask'])

        self._df = df

    def assign_ids(self, entries_df):
        entries_df = AccountSchema(entries_df)
        entries_df = pd.merge(
            entries_df,
            self._df[['id']],
            left_on=['account', 'mask'],
            right_index=True,
            how='left'
        )
        entries_df = entries_df.rename(columns={'id': 'account_id', 'account': 'account_name', 'mask': 'account_mask'})
        return entries_df

    def apply_entries(self, entries_df):
        entries_df = self.assign_ids(entries_df)
        return entries_df

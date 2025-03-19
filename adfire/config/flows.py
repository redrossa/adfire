import pandas as pd

from adfire.config.base import BaseConfig, BaseInputSchema


class ChangeSchema(BaseInputSchema):
    change: float


class FlowsConfig(BaseConfig):
    def apply_entries(self, entries_df):
        entries_df: pd.DataFrame = ChangeSchema(entries_df)
        entries_df.loc[entries_df['change'] < 0, 'flow'] = 'credit'
        entries_df.loc[entries_df['change'] >= 0, 'flow'] = 'debit'
        return entries_df
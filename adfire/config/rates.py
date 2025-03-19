from typing import Optional

import pandas as pd
from pandera import Field

from adfire.config.flows import FlowsConfig, ChangeSchema


class TransactionSchema(ChangeSchema):
    name: str
    symbol: str
    rate: float = Field(nullable=True)

    class Config:
        add_missing_columns = True


class RatesConfig(FlowsConfig):
    def __init__(self, default_asset='USD', **kwargs):
        super(RatesConfig, self).__init__(**kwargs)
        self._default_currency = default_asset

    def assign_default_asset_rates(self, entries_df):
        entries_df: pd.DataFrame = TransactionSchema(entries_df)
        for tx_name, tx_df in entries_df.groupby('name'):
            mask_default_asset = tx_df['symbol'] == self._default_currency
            mask_nan_rate = tx_df['rate'].isna()
            default_curr_entries_df = tx_df[mask_default_asset & mask_nan_rate]
            entries_df.loc[default_curr_entries_df.index, 'rate'] = 1
        return entries_df

    def assign_asset_conversion_rates(self, entries_df):
        entries_df: pd.DataFrame = TransactionSchema(entries_df)
        for tx_name, tx_df in entries_df.groupby('name'):
            non_default_curr_entries_df = tx_df[tx_df['symbol'] != self._default_currency]
            if len(non_default_curr_entries_df) == 0:
                continue

            asset_change_sum = abs(tx_df.groupby('symbol')['change'].sum())

        return entries_df

    def apply_entries(self, entries_df):
        entries_df = super(RatesConfig, self).apply_entries(entries_df)
        entries_df = self.assign_default_asset_rates(entries_df)
        entries_df = self.assign_asset_conversion_rates(entries_df)
        return entries_df

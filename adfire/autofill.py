import uuid

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
from pandera.typing import DataFrame

from adfire.schema import MergedInputEntrySchema, HashableEntrySchema


def sort_entries(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    """Sort entries by ascending date. TODO: add future sorting specifications here."""
    return df.sort_values(by=['date', 'entry_id'], ascending=[True, True])


def fill_current_balances(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    # calculate current balances
    for account, group in df.groupby('account_name'):
        mask_posted = group['status'] == 'posted'
        posted_curr_bal = group[mask_posted]['amount'].cumsum()
        group.loc[posted_curr_bal.index, '_balance_current'] = posted_curr_bal
        group['_balance_current'] = group['_balance_current'].ffill().fillna(0)
        df.loc[group['_balance_current'].index, '_balance_current'] = group['_balance_current']

    # manage offsets per account
    _df = df[['account_name', 'balance_current', '_balance_current']]
    for account, group in _df.groupby('account_name'):
        mask_balance_filled = group['balance_current'].notna()
        filled_input_bal = group.loc[mask_balance_filled, 'balance_current']
        filled_computed_bal = group.loc[mask_balance_filled, '_balance_current']
        computed_offsets = (filled_input_bal - filled_computed_bal).round(2)

        if not computed_offsets.empty:
            # verify all offsets are equal
            offset = computed_offsets.iloc[0]
            expected_offsets = pd.Series(
                data=offset,
                index=computed_offsets.index,
                dtype=computed_offsets.dtype
            )
            assert_series_equal(computed_offsets, expected_offsets, check_names=False)

            # offset computed bal
            group['_balance_current'] = group['_balance_current'] + offset

        # verify input balance match computed
        filled_offset_computed_bal = group.loc[mask_balance_filled, '_balance_current']
        assert_series_equal(filled_offset_computed_bal, filled_input_bal, check_names=False)

        # replace input current balance column with computed
        df.loc[group['_balance_current'].index, 'balance_current'] = group['_balance_current']

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df


def fill_total_balances(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    grouped_by_account = df.groupby('account_name')
    first = grouped_by_account.first()
    initial_balance = first['balance_current'] - first['amount']
    df['balance_total'] = grouped_by_account['amount'].cumsum() + df['account_name'].map(initial_balance)
    df = MergedInputEntrySchema.validate(df)
    return df


def fill_available_balances(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    grouped_by_account = df.groupby('account_name')

    # calculate temporary cumsum of amounts
    df['_balance_cumsum'] = grouped_by_account['amount'].cumsum()

    # calculate available balances for credit types
    mask_is_credit = df['account_type'] == 'credit'
    credit_masked = df[mask_is_credit]
    df.loc[mask_is_credit, '_balance_available'] = (
            credit_masked['balance_limit'] - credit_masked['_balance_cumsum'])

    # calculate available balances for depository types
    mask_is_depository = df['account_type'] == 'depository'
    depository_masked = df[mask_is_depository]
    df.loc[mask_is_depository, '_balance_available'] = depository_masked['_balance_cumsum']

    # calculate offsets for each account. IMPORTANT: assumes current balances are correctly filled
    first_entries = grouped_by_account.first()
    mask_is_posted = first_entries['status'] == 'posted'
    offsets = first_entries['balance_current'] - np.where(mask_is_posted, first_entries['amount'], 0)
    offsets.name = '_balance_offset'
    df = df.join(offsets, on='account_name')

    # apply offsets to available balances
    df.loc[mask_is_credit, '_balance_available'] = df['_balance_available'] - df['_balance_offset']
    df.loc[mask_is_depository, '_balance_available'] = df['_balance_available'] + df['_balance_offset']

    # verify input available balance match computed
    input_bal = df['balance_available']
    mask_filled = input_bal.notnull()
    filled_input_bal = input_bal[mask_filled]
    filled_computed_bal = df.loc[mask_filled, '_balance_available']
    assert_series_equal(filled_computed_bal, filled_input_bal, check_names=False)

    # replace input available balance column with computed
    df['balance_available'] = df['_balance_available']
    df = df.drop(['_balance_cumsum', '_balance_available'], axis=1)

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df


def assign_transactions(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    # assign universal index to each entry regardless of path
    indexed_df = df.reset_index()
    indexed_df['_id'] = indexed_df.index

    # fill transaction ID's for NaN entries
    mask_is_nan = indexed_df['transaction_id'].isna()
    indexed_df.loc[mask_is_nan, 'transaction_id'] = [str(uuid.uuid4()) for _ in range(mask_is_nan.sum())]

    # add helper columns
    help_df = indexed_df[indexed_df['entity'].isin(indexed_df['account_name'])].copy()
    help_df['_worth'] = np.where(help_df['account_type'] == 'credit', -help_df['amount'], help_df['amount'])
    help_df['_worth_absolute'] = help_df['_worth'].abs()
    help_df['_from'] = np.where(help_df['_worth'] < 0, help_df['account_name'], help_df['entity'])
    help_df['_to'] = np.where(help_df['_worth'] < 0, help_df['entity'], help_df['account_name'])
    help_df['_is_source'] = help_df['_worth'] < 0

    # pair possible transaction entries into one row
    paired_df = help_df.merge(
        help_df,
        how='inner',
        on=['_worth_absolute', '_from', '_to'],
        suffixes=('_open', '_close')
    )

    # filter out pair rows that are not possibly transactions
    mask_self_match = paired_df['_id_open'] == paired_df['_id_close']
    mask_reverse = paired_df['_id_open'] > paired_df['_id_close']
    mask_equal_source = paired_df['_is_source_open'] == paired_df['_is_source_close']
    mask_within_a_week = (paired_df['date_close'] - paired_df['date_open']) < pd.Timedelta(days=7)
    mask_zero_sum_worth = paired_df['_worth_open'] + paired_df['_worth_close'] == 0
    paired_df = paired_df[
        ~mask_self_match &
        ~mask_reverse &
        ~mask_equal_source &
        mask_within_a_week &
        mask_zero_sum_worth]
    paired_df = paired_df.drop_duplicates(subset=['_id_open'])
    paired_df = paired_df.drop_duplicates(subset=['_id_close'])

    # assign equal transaction IDs for entry pairs (choose first if possible, else one already hashed)
    paired_df['transaction_id'] = np.where(
        paired_df['hash_open'].isna() & paired_df['hash_close'].notna(),
        paired_df['transaction_id_close'],
        paired_df['transaction_id_open']
    )

    # split entry pair row into separate rows for original helper index
    transaction_ids = pd.melt(
        paired_df,
        id_vars=['transaction_id'],
        value_vars=['_id_open', '_id_close'],
        var_name='source',
        value_name='index'
    )
    transaction_ids = transaction_ids.set_index('index')
    transaction_ids = indexed_df.join(transaction_ids, lsuffix='_unpaired', rsuffix='_paired')

    # aggregate all transaction ID values to remove NaNs
    transaction_ids['transaction_id'] = transaction_ids[[
        'transaction_id_paired',
        'transaction_id_unpaired'
    ]].bfill(axis=1).iloc[:, 0]

    # verify that the pattern of manual IDs match computed
    unique_pairs = transaction_ids.groupby(['transaction_id', 'transaction_id_paired']).size()
    counts = unique_pairs.groupby('transaction_id').size()
    assert (counts == 1).all(), "Manual ID pattern doesn't match computed"

    # finally, assign IDs
    indexed_df['transaction_id'] = transaction_ids['transaction_id']
    indexed_df = indexed_df.set_index(df.index)
    df['transaction_id'] = indexed_df['transaction_id']

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df


def hash_entries(df: DataFrame[MergedInputEntrySchema], forced_hash = False) -> DataFrame[MergedInputEntrySchema]:
    # filter out columns not required for hashing
    hashable_df = HashableEntrySchema.validate(df, lazy=True)

    # compute hashes
    hashable_df['hash'] = pd.util.hash_pandas_object(hashable_df, index=False).astype(str)  # without astype it's uint

    # verify input hashed entries have equal computed hashes
    if not forced_hash:
        old_hashes_df = df[df['hash'].notna()].reset_index()
        assert all(old_hashes_df['hash'].isin(hashable_df['hash']))

    # set hashes to original df
    df['hash'] = hashable_df['hash']

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df

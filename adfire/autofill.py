import uuid
from typing import Callable

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
from pandera.typing import DataFrame

from adfire.schema import MergedInputEntrySchema, HashableEntrySchema


def sort_entries(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    """Sort entries by ascending date. TODO: add future sorting specifications here."""
    return df.sort_values(by='date', ascending=True)


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
        _df.loc[group['_balance_current'].index, 'balance_current'] = group['_balance_current']

    # clean up
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
    df['_balance_available'] = df['_balance_available'] - df['_balance_offset']

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


def assign_transactions(
        df: DataFrame[MergedInputEntrySchema],
        id_gen_func: Callable = uuid.uuid4
) -> DataFrame[MergedInputEntrySchema]:
    # assign universal index to each entry regardless of path
    indexed_df = df.reset_index()
    indexed_df['_id'] = indexed_df.index

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

    # create unique transaction IDs for each entry pair row
    paired_df['transaction_id'] = [str(id_gen_func()) for _ in range(len(paired_df.index))]

    # split entry pair row into separate rows for original helper index
    transaction_ids = pd.melt(
        paired_df,
        id_vars=['transaction_id'],
        value_vars=['_id_open', '_id_close'],
        var_name='source',
        value_name='index'
    )
    transaction_ids = transaction_ids.set_index('index')
    transaction_ids = indexed_df.join(transaction_ids, lsuffix='_manual', rsuffix='_autofill')
    mask_is_na = transaction_ids['transaction_id_autofill'].isna()
    transaction_ids['transaction_id_autofill'] = transaction_ids['transaction_id_autofill'].astype(str)
    transaction_ids.loc[mask_is_na, 'transaction_id_autofill'] = [str(id_gen_func()) for _ in range(mask_is_na.sum())]

    # fill NaN IDs
    transaction_ids['id'] = transaction_ids.index
    filled = transaction_ids.merge(transaction_ids, on='transaction_id_autofill', how='left')
    filled_no_dupes = filled.drop_duplicates('id_x', keep=False)
    filled_dupes_no_self = filled[filled['id_x'] != filled['id_y']]
    filled = pd.concat([filled_no_dupes, filled_dupes_no_self])
    filled = filled.sort_values('id_x', ignore_index=True)

    # verify that manual IDs don't conflict computed pairs. TODO: not sure we still need this
    # mask_x_notna = filled['transaction_id_manual_x'].notna()
    # mask_y_notna = filled['transaction_id_manual_y'].notna()
    # filled = filled[mask_x_notna & mask_y_notna]
    # assert (filled['transaction_id_manual_x'] == filled['transaction_id_manual_y']).all(), \
    #     "Manual ID's conflict with computed"

    # aggregate all transaction IDs values to remove NaNs
    filled['transaction_id'] = filled[[
        'transaction_id_manual_x',
        'transaction_id_manual_y',
        'transaction_id_autofill'
    ]].bfill(axis=1).iloc[:, 0]

    # verify that the pattern of manual IDs match computed
    unique_pairs = filled.groupby(['transaction_id', 'transaction_id_autofill']).size()
    counts = unique_pairs.groupby('transaction_id').size()
    assert (counts == 1).all(), "Manual ID pattern doesn't match computed"

    # finally, assign IDs
    indexed_df['transaction_id'] = filled['transaction_id']
    indexed_df = indexed_df.set_index(df.index)
    df['transaction_id'] = indexed_df['transaction_id']

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df


def hash_transactions(df: DataFrame[MergedInputEntrySchema]) -> DataFrame[MergedInputEntrySchema]:
    # filter out columns not required for hashing
    hashable_df = df.reset_index()
    hashable_df = HashableEntrySchema.validate(hashable_df, lazy=True)

    # compute hashes
    hashable_df['hash'] = pd.util.hash_pandas_object(hashable_df)

    # set hashes to original df
    df = df.reset_index()
    df = df.set_index('transaction_id')
    hashable_df = hashable_df.set_index('transaction_id')
    df['hash'] = hashable_df['hash']
    df = df.reset_index()
    df = df.set_index(list(MergedInputEntrySchema.to_schema().index.columns))

    # clean up
    df = MergedInputEntrySchema.validate(df)

    return df

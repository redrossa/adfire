import uuid

import numpy as np
import pandas as pd
import pandera as pa
from pandas._testing import assert_series_equal

schema = pa.DataFrameSchema({
    'id.transaction': pa.Column(str, nullable=True),
    'date': pa.Column(pa.DateTime),
    'entity': pa.Column(str),
    'description': pa.Column(str, nullable=True),
    'category': pa.Column(str),
    'amount': pa.Column(float),
    'worth': pa.Column(float, nullable=True),
    'status': pa.Column(str),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str),
    'balances.current': pa.Column(float, nullable=True),
    'balances.available': pa.Column(float, nullable=True), # ideally nullable only if limit is null
    'balances.limit': pa.Column(float, nullable=True),
}, coerce=True, strict='filter', add_missing_columns=True)


def add_col_worth(record: pd.DataFrame) -> pd.DataFrame:
    record['worth'] = np.where(record['type'] == 'credit', -record['amount'], record['amount'])
    return record


def _format_types(record: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(record)


def _sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record = add_col_worth(record)
    return record.sort_values(by=['date', 'worth'], ascending=[True, False], ignore_index=True)


def _fill_current_balances(record: pd.DataFrame) -> pd.DataFrame:
    # calculate current balances
    for account, group in record.groupby('account'):
        mask_posted = group['status'] == 'posted'
        posted_curr_bal = group[mask_posted]['amount'].cumsum()
        group.loc[posted_curr_bal.index, '_balances.current'] = posted_curr_bal
        group['_balances.current'] = group['_balances.current'].ffill().fillna(0)
        record.loc[group['_balances.current'].index, '_balances.current'] = group['_balances.current']

    # manage offsets per account
    df = record[['account', 'balances.current', '_balances.current']]
    for account, group in df.groupby('account'):
        mask_balance_filled = group['balances.current'].notna()
        filled_input_bal = group.loc[mask_balance_filled, 'balances.current']
        filled_computed_bal = group.loc[mask_balance_filled, '_balances.current']
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
            group['_balances.current'] = group['_balances.current'] + offset

        # verify input balance match computed
        filled_offset_computed_bal = group.loc[mask_balance_filled, '_balances.current']
        assert_series_equal(filled_offset_computed_bal, filled_input_bal, check_names=False)

        # replace input current balance column with computed
        record.loc[group['_balances.current'].index, 'balances.current'] = group['_balances.current']

    # remove temporary column
    record = record.drop('_balances.current', axis=1)

    return record


def _fill_available_balances(record: pd.DataFrame) -> pd.DataFrame:
    grouped_by_account = record.groupby('account')

    # calculate temporary cumsum of amounts
    record['_balances.cumsum'] = grouped_by_account['amount'].cumsum()

    # calculate available balances for credit types
    mask_is_credit = record['type'] == 'credit'
    credit_masked = record[mask_is_credit]
    record.loc[mask_is_credit, '_balances.available'] = (
        credit_masked['balances.limit'] - credit_masked['_balances.cumsum'])

    # calculate available balances for depository types
    mask_is_depository = record['type'] == 'depository'
    depository_masked = record[mask_is_depository]
    record.loc[mask_is_depository, '_balances.available'] = depository_masked['_balances.cumsum']

    # calculate offsets for each account. IMPORTANT: assumes current balances are correctly filled
    first_entries = grouped_by_account.first()
    mask_is_posted = first_entries['status'] == 'posted'
    offsets = first_entries['balances.current'] - np.where(mask_is_posted, first_entries['amount'], 0)
    offsets.name = '_balances.offset'
    record = record.join(offsets, on='account')

    # apply offsets to available balances
    record['_balances.available'] = record['_balances.available'] - record['_balances.offset']

    # verify input available balance match computed
    input_bal = record['balances.available']
    mask_filled = input_bal.notnull()
    filled_input_bal = input_bal[mask_filled]
    filled_computed_bal = record.loc[mask_filled, '_balances.available']
    assert_series_equal(filled_computed_bal, filled_input_bal, check_names=False)

    # replace input available balance column with computed
    record['balances.available'] = record['_balances.available']
    record = record.drop(['_balances.cumsum', '_balances.available'], axis=1)

    return record


def _identify_transfers(record: pd.DataFrame) -> pd.DataFrame:
    # add helper columns
    mask_entity_is_in_account = record['entity'].isin(record['account'])
    potential_transfers = record[mask_entity_is_in_account]
    potential_transfers['_worth.absolute'] = potential_transfers['worth'].abs()
    potential_transfers['_from'] = np.where(potential_transfers['worth'] < 0, potential_transfers['account'], potential_transfers['entity'])
    potential_transfers['_to'] = np.where(potential_transfers['worth'] < 0, potential_transfers['entity'], potential_transfers['account'])
    potential_transfers['_is_source'] = potential_transfers['worth'] < 0
    potential_transfers['_id'] = potential_transfers.index

    # merge the dataframe with itself to pair rows
    paired = potential_transfers.merge(
        potential_transfers,
        how='inner',
        on=['_worth.absolute', '_from', '_to'],
        suffixes=('_open', '_close')
    )

    # filter unwanted
    mask_self_match = paired['_id_open'] == paired['_id_close']
    mask_reverse = paired['_id_open'] > paired['_id_close']
    mask_equal_source = paired['_is_source_open'] == paired['_is_source_close']
    mask_within_a_week = (paired['date_close'] - paired['date_open']) < pd.Timedelta(days=7)
    mask_zero_sum_worth = paired['worth_open'] + paired['worth_close'] == 0
    paired = paired[~mask_self_match & ~mask_reverse & ~mask_equal_source & mask_within_a_week & mask_zero_sum_worth]
    paired = paired.drop_duplicates(subset=['_id_open'])
    paired = paired.drop_duplicates(subset=['_id_close'])

    # create unique transaction IDs for the paired transfers
    paired['id.transaction'] = [str(uuid.uuid4()) for _ in range(len(paired.index))]

    # transform shape
    transaction_ids = pd.melt(paired, id_vars=['id.transaction'], value_vars=['_id_open', '_id_close'], var_name='source', value_name='index')
    transaction_ids = transaction_ids.set_index('index')
    transaction_ids = record.join(transaction_ids, lsuffix='_manual', rsuffix='_format')
    mask_is_na = transaction_ids['id.transaction_format'].isna()
    transaction_ids.loc[mask_is_na, 'id.transaction_format'] = [str(uuid.uuid4()) for _ in range(mask_is_na.sum())]

    # fill NaN IDs
    transaction_ids['id'] = transaction_ids.index
    assigned = transaction_ids.merge(transaction_ids, on='id.transaction_format', how='left')
    assigned_no_dupes = assigned.drop_duplicates('id_x', keep=False)
    assigned_dupes_no_self = assigned[assigned['id_x'] != assigned['id_y']]
    assigned = pd.concat([assigned_no_dupes, assigned_dupes_no_self]).sort_values('id_x', ignore_index=True)
    assigned['id.transaction'] = assigned[['id.transaction_manual_x', 'id.transaction_manual_y', 'id.transaction_format']].bfill(axis=1).iloc[:, 0]

    # verify that the pattern of manual IDs match computed
    unique_pairs = assigned.groupby(['id.transaction', 'id.transaction_format']).size()
    counts = unique_pairs.groupby('id.transaction').size()
    assert (counts == 1).all(), "Manual ID pattern doesn't match computed"

    # assign IDs to record
    record['id.transaction'] = assigned['id.transaction']

    return record


def compile_records(records: list[pd.DataFrame]) -> pd.DataFrame:
    return pd.concat(records, ignore_index=True)


def format_record(record: pd.DataFrame, decimals = 2) -> pd.DataFrame:
    return (
        record.pipe(_format_types)
        .pipe(add_col_worth)
        .pipe(_sort_record)
        .pipe(_fill_current_balances)
        .pipe(_fill_available_balances)
        .pipe(_identify_transfers)
        .pipe(_format_types)
        .round(decimals)
    )


def hash_record(record: pd.DataFrame) -> pd.Series:
    filtered = record[[
        'id.transaction',
        'date',
        'entity',
        'amount',
        'worth',
        'status',
        'account',
        'mask',
        'type',
        'subtype',
        'balances.limit',
    ]]
    mask_is_posted = filtered['status'] == 'posted'
    filtered = filtered[mask_is_posted]
    filtered = filtered.set_index('id.transaction')
    hashed = pd.util.hash_pandas_object(filtered)
    return hashed


def is_checksum_subset(a: pd.Series, b: pd.Series) -> bool:
    return np.isin(a, b).all()

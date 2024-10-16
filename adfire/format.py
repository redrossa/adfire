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

    # verify input current balance match computed
    input_bal = record['balances.current']
    mask_filled = input_bal.notnull()
    filled_input_bal = input_bal[mask_filled]
    filled_computed_bal = record.loc[mask_filled, '_balances.current']
    assert_series_equal(filled_computed_bal, filled_input_bal, check_names=False)

    # replace input current balance column with computed
    record['balances.current'] = record['_balances.current']
    record = record.drop('_balances.current', axis=1)

    return record


def _fill_available_balances(record: pd.DataFrame) -> pd.DataFrame:
    # calculate temporary cumsum of amounts
    record['_balances.cumsum'] = record.groupby('account')['amount'].cumsum()

    # calculate available balances for credit types
    mask_is_credit = record['type'] == 'credit'
    credit_masked = record[mask_is_credit]
    record.loc[mask_is_credit, '_balances.available'] = (
        credit_masked['balances.limit'] - credit_masked['_balances.cumsum'])

    # calculate available balances for depository types
    mask_is_depository = record['type'] == 'depository'
    depository_masked = record[mask_is_depository]
    record.loc[mask_is_depository, '_balances.available'] = depository_masked['_balances.cumsum']

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
    paired = paired[~mask_self_match & ~mask_reverse & ~mask_equal_source & mask_within_a_week]
    paired = paired.drop_duplicates(subset=['_id_open'])
    paired = paired.drop_duplicates(subset=['_id_close'])

    # create unique transaction IDs for the paired transfers
    paired['id.transaction'] = [str(uuid.uuid4()) for _ in range(len(paired.index))]

    # transform shape
    transaction_ids = pd.melt(paired, id_vars=['id.transaction'], value_vars=['_id_open', '_id_close'], var_name='source', value_name='index')
    transaction_ids = transaction_ids.set_index('index')

    # assign transaction IDs to original record if nan
    mask_ids_overridden = record['id.transaction'].isna()
    record.loc[mask_ids_overridden, 'id.transaction'] = transaction_ids['id.transaction']

    # verify transactions worth sum to 0
    worth_sums = record.groupby('id.transaction')['worth'].sum()
    expected = worth_sums.copy()
    worth_sums[:] = 0
    assert_series_equal(worth_sums, expected)

    return record


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    return (
        record.pipe(_format_types)
        .pipe(add_col_worth)
        .pipe(_sort_record)
        .pipe(_fill_current_balances)
        .pipe(_fill_available_balances)
        .pipe(_identify_transfers)
        .pipe(_format_types)
    )

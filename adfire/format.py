import numpy as np
import pandas as pd
import pandera as pa
from pandas._testing import assert_series_equal

schema = pa.DataFrameSchema({
    'date': pa.Column(pa.DateTime),
    'description': pa.Column(str),
    'category': pa.Column(str),
    'amount': pa.Column(float),
    'status': pa.Column(str),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str),
    'balances.current': pa.Column(float, nullable=True),
    'balances.available': pa.Column(float, nullable=True), # ideally nullable only if limit is null
    'balances.limit': pa.Column(float, nullable=True),
}, coerce=True, strict='filter', add_missing_columns=True)


def _format_types(record: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(record)


def _sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record['amount.asset'] = np.where(record['type'] == 'depository', record['amount'], -record['amount'])
    return record.sort_values(by=['date', 'amount.asset'], ascending=[True, False], ignore_index=True)


def _fill_current_balances(record: pd.DataFrame) -> pd.DataFrame:
    # get computed current balances
    for account, group in record.groupby('account'):
        mask_posted = group['status'] == 'posted'
        posted_curr_bal = group[mask_posted]['amount'].cumsum()
        group.loc[posted_curr_bal.index, 'balances.current'] = posted_curr_bal
        group['balances.current'] = group['balances.current'].ffill().fillna(0)
        record.loc[group['balances.current'].index, '_balances.current'] = group['balances.current']

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
    # get computed available balances
    record['_balances.cumsum'] = record.groupby('account')['amount'].cumsum()

    mask_is_credit = record['type'] == 'credit'
    credit_masked = record[mask_is_credit]
    record.loc[mask_is_credit, '_balances.available'] = (
        credit_masked['balances.limit'] - credit_masked['_balances.cumsum'])

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


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    typed = _format_types(record)
    sorted = _sort_record(typed)
    filled_current = _fill_current_balances(sorted)
    filled_available = _fill_available_balances(filled_current)
    df = _format_types(filled_available)
    return df

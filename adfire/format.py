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


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    typed = _format_types(record)
    filled_worth = add_col_worth(typed)
    sorted = _sort_record(filled_worth)
    filled_current = _fill_current_balances(sorted)
    filled_available = _fill_available_balances(filled_current)
    df = _format_types(filled_available)
    return df

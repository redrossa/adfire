import json
import os
import uuid

import numpy as np
import pandas as pd
import pandera as pa
import pytest
from pandas._testing import assert_frame_equal, assert_series_equal
from pandera.errors import SchemaError

from adfire.format import format_record, schema, add_col_worth
from adfire.io import read_record


pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class Case:
    cases_path = os.path.join('cases', 'format')

    @classmethod
    def find_cases(cls):
        return [os.path.join(Case.cases_path, x) for x in os.listdir(cls.cases_path)]

    @classmethod
    def load_cases(cls):
        return [Case(x) for x in Case.find_cases()]

    @classmethod
    def read_expected_record(cls, path) -> pd.DataFrame:
        df = pd.read_csv(path, dtype=str)
        expected_schema = pa.DataFrameSchema(schema.columns, coerce=True, strict=True)
        df = expected_schema.validate(df)
        return df

    @classmethod
    def match_error(cls, error_str):
        match error_str:
            case 'SchemaError':
                return SchemaError
            case 'AssertionError':
                return AssertionError
            case _:
                raise ValueError

    def __init__(self, path):
        with open(os.path.join(path, 'metadata.json'), 'r') as f:
            metadata = json.load(f)

        self.name = metadata['name']
        self.description = metadata['description']
        self.input = read_record(os.path.join(path, metadata['input']))

        self.expected = None
        self.error = None
        if 'expected' in metadata:
            self.expected = Case.read_expected_record(os.path.join(path, metadata['expected']))
        else:
            self.error = self.match_error(metadata['error'])


all_cases = Case.load_cases()


@pytest.fixture(params=all_cases, ids=lambda x: x.name)
def case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if not x.error], ids=lambda x: x.name)
def positive_case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if x.error], ids=lambda x: x.name)
def negative_case(request):
    return request.param


def test_format(case, monkeypatch):
    if case.error:
        with pytest.raises(case.error):
            format_record(case.input)
    else:
        # test id.transaction separately
        actual = format_record(case.input).drop('id.transaction', axis=1)
        expected = case.expected.drop('id.transaction', axis=1)
        assert_frame_equal(actual, expected)


def test_types(positive_case):
    actual = format_record(positive_case.input)
    mock_schema = pa.DataFrameSchema(schema.columns, strict=True)
    mock_schema.validate(actual)


def test_date_is_ascending(positive_case):
    actual = format_record(positive_case.input)
    computed = actual['date']
    assert computed.is_monotonic_increasing


def test_amount_is_descending_for_equal_dates(positive_case):
    actual = format_record(positive_case.input)
    actual = add_col_worth(actual)
    for date, group in actual.groupby('date'):
        assert group['worth'].is_monotonic_decreasing


def test_all_current_balances_filled(positive_case):
    actual = format_record(positive_case.input)
    assert actual['balances.current'].notna().all()


def test_available_balances_are_filled_if_limit_filled(positive_case):
    actual = format_record(positive_case.input)
    assert actual.loc[actual['balances.limit'].notna(), 'balances.available'].notna().all()


def test_limit_equals_cumsum_plus_available(positive_case):
    actual = format_record(positive_case.input)
    mask_has_limit = actual['balances.limit'].notna()
    mask_has_available = actual['balances.available'].notna()
    filtered = actual[mask_has_limit & mask_has_available]
    filtered['_balances.cumsum'] = filtered.groupby('account')['amount'].cumsum()
    first_entries = filtered.groupby('account').first()
    mask_is_posted = first_entries['status'] == 'posted'
    offsets = first_entries['balances.current'] - np.where(mask_is_posted, first_entries['amount'], 0)
    offsets.name = '_balances.offset'
    filtered = filtered.join(offsets, on='account')
    assert_series_equal(
        filtered['_balances.cumsum'] + filtered['_balances.offset'] + filtered['balances.available'],
        filtered['balances.limit'],
        check_names=False,
    )


def test_current_is_posted_amount_cumsum(positive_case):
    actual = format_record(positive_case.input)
    for account, group in actual.groupby('account'):
        mask_posted = group['status'] == 'posted'
        posted_curr_bal = group[mask_posted]['amount'].cumsum()
        group.loc[posted_curr_bal.index, 'balances.current'] = posted_curr_bal
        group['balances.current'] = group['balances.current'].ffill().fillna(0)
        offset = actual.iloc[group.index][mask_posted]['balances.current'].iloc[0] - group[mask_posted]['balances.current'].iloc[0]
        group.loc[mask_posted, 'balances.current'] = group.loc[mask_posted, 'balances.current'] + offset
        assert_series_equal(
            group['balances.current'],
            actual.iloc[group.index]['balances.current'],
            check_names=False
        )


def test_transfer_entries_worth_sum_zero(positive_case):
    actual = format_record(positive_case.input)
    grouped = actual.groupby('id.transaction')
    sizes = grouped.size()
    mask_is_transfer = sizes[sizes > 1]
    filtered = actual[actual['id.transaction'].isin(mask_is_transfer)]
    sums = filtered['worth'].sum()
    assert (sums == 0).all()


def test_entries_paired_as_transfer(positive_case):
    actual = format_record(positive_case.input)
    joined = actual.join(positive_case.expected, how='inner', lsuffix='_actual', rsuffix='_expected')
    filtered = joined[joined['id.transaction_expected'].notna()]
    unique = filtered.groupby('id.transaction_expected')['id.transaction_actual'].nunique()
    assert (unique == 1).all()

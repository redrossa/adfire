import json
import os

import numpy as np
import pandas as pd
import pandera as pa
import pytest
from pandas._testing import assert_frame_equal
from pandera.errors import SchemaError

from adfire.format import format_record, schema
from adfire.io import read_record


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
        df = schema.validate(df)
        return df

    @classmethod
    def match_error(cls, error_str):
        match error_str:
            case 'SchemaError':
                return SchemaError
            case _:
                raise ValueError

    def __init__(self, path):
        with open(os.path.join(path, 'metadata.json'), 'r') as f:
            metadata = json.load(f)

        self.name = metadata['name']
        self.description = metadata['description']
        self.input = read_record(os.path.join(path, metadata['input']))
        if 'expected' in metadata:
            self.expected = Case.read_expected_record(os.path.join(path, metadata['expected']))
            self.error = None
        else:
            self.error = self.match_error(metadata['error'])
            self.expected = None


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


def test_format(case):
    if case.error:
        with pytest.raises(case.error):
            format_record(case.input)
    else:
        actual = format_record(case.input)
        assert_frame_equal(actual, case.expected)


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
    actual['amount.asset'] = np.where(actual['type'] == 'depository', actual['amount'], -actual['amount'])
    for date, group in actual.groupby('date'):
        assert group['amount.asset'].is_monotonic_decreasing


def test_all_current_balances_filled(positive_case):
    actual = format_record(positive_case.input)
    assert actual['balances.current'].notna().all()


def test_available_balances_are_filled_if_limit_filled(positive_case):
    actual = format_record(positive_case.input)
    assert actual.loc[actual['balances.limit'].notna(), 'balances.available'].notna().all()


def test_row_wise_balances(positive_case):
    actual = format_record(positive_case.input)
    assert np.isclose(
        actual['balances.current'] + actual['balances.available'],
        actual['balances.limit'],
        equal_nan=True
    ).all()


def test_col_wise_current_balances(positive_case):
    actual = format_record(positive_case.input)
    grouped = actual.groupby('account')
    assert np.isclose(
        grouped['balances.current'].shift(1, fill_value=0) + grouped['amount'].shift(0),
        actual['balances.current'],
        equal_nan=True
    ).all()

import os

import pandas as pd
import pytest

from adfire.format import sort_record


class RecordCase:
    def __init__(self, dir):
        self.name = os.path.basename(dir)
        self.input_record = RecordCase.read_record(os.path.join(dir, 'input.csv'))
        self.expected_record = RecordCase.read_record(os.path.join(dir, 'expect.csv'))

    @classmethod
    def read_record(cls, path):
        return pd.read_csv(path).astype({'date': 'datetime64[ns]', 'mask': 'object'})

    @classmethod
    def find_cases(cls):
        test_path = 'cases/sort_record'
        dirs = [os.path.join(test_path, dir) for dir in os.listdir(test_path)
                if os.path.isdir(os.path.join(test_path, dir))]
        return dirs


@pytest.mark.parametrize('case_dir', RecordCase.find_cases(), ids=lambda dir: os.path.basename(dir))
def test_sort_record(case_dir):
    case = RecordCase(case_dir)
    actual_record = sort_record(case.input_record)
    assert actual_record.equals(case.expected_record)

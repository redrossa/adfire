import os

import pandas as pd
import pytest

from adfire.format import format_types


class FormatCase:
    def __init__(self, dir):
        self.name = os.path.basename(dir)
        self.input_record = pd.read_csv(os.path.join(dir, 'input.csv'), dtype=str)

        self.expected_exception = None
        expect_raise = os.path.join(dir, 'raise.txt')
        if os.path.isfile(expect_raise):
            with open(expect_raise, 'r') as fp:
                exception_name = fp.read()
                match exception_name:
                    case 'KeyError':
                        self.expected_exception = KeyError
                    case 'ValueError':
                        self.expected_exception = ValueError

        self.expected_dtypes = None
        expect_path = os.path.join(dir, 'expect.csv')
        if os.path.isfile(expect_path):
            expected_dtypes_df = pd.read_csv(expect_path)
            self.expected_dtypes = pd.Series(expected_dtypes_df['dtype'].values, index=expected_dtypes_df['column'])

        if self.expected_exception is None and self.expected_dtypes is None:
            raise FileNotFoundError(f'Missing expectation: {self.name}')

    @classmethod
    def find_cases(cls):
        test_path = 'cases/format_types'
        dirs = [os.path.join(test_path, dir) for dir in os.listdir(test_path)
                if os.path.isdir(os.path.join(test_path, dir))]
        return dirs


@pytest.mark.parametrize('case_dir', FormatCase.find_cases(), ids=lambda dir: os.path.basename(dir))
def test_format_types(case_dir):
    case = FormatCase(case_dir)
    if case.expected_exception:
        with pytest.raises(case.expected_exception):
            format_types(case.input_record)
    if case.expected_dtypes is not None:
        formatted = format_types(case.input_record)
        actual_dtypes = formatted.dtypes.astype(str)
        assert actual_dtypes.equals(case.expected_dtypes)

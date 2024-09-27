import glob
import os

import pandas as pd
import pandera as pa
import pytest

from adfire.format import format_types


class FormatTypesCase:
    schema = pa.DataFrameSchema({
        'date': pa.Column(pa.DateTime),
        'description': pa.Column(str),
        'amount': pa.Column(float),
        'account': pa.Column(str),
        'mask': pa.Column(str),
        'type': pa.Column(str),
        'subtype': pa.Column(str)
    })

    def __init__(self, path):
        folders = path.split(os.path.sep)
        root, _ = os.path.splitext(folders[-1])
        self.name = f'{folders[-2]}.{root}'
        self.is_valid = folders[-2] == 'valid'
        self.input_record = pd.read_csv(path, dtype=str)

    @classmethod
    def find_cases(cls):
        test_path = 'cases/format_types'
        valid_path = os.path.join(test_path, 'valid', '*.csv')
        invalid_path = os.path.join(test_path, 'invalid', '*.csv')
        paths = glob.glob(valid_path)
        paths += glob.glob(invalid_path)
        return paths

    @classmethod
    def load_cases(cls):
        return [FormatTypesCase(x) for x in FormatTypesCase.find_cases()]


@pytest.mark.parametrize('case', FormatTypesCase.load_cases(), ids=lambda x: x.name)
def test_format_types(case):
    if not case.is_valid:
        with pytest.raises(pa.errors.SchemaError):
            format_types(case.input_record)
    else:
        formatted = format_types(case.input_record)
        assert FormatTypesCase.schema.validate(formatted) is not None

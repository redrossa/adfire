import glob
import os

import pandas as pd
import pytest


class FillBalancesCase:
    def __init__(self, path):
        folders = path.split(os.path.sep)
        root, _ = os.path.splitext(folders[-1])
        self.name = f'{folders[-2]}.{root}'
        self.is_valid = folders[-2] == 'valid'
        self.input_record = pd.read_csv(path, dtype=str)

    @classmethod
    def find_cases(cls):
        test_path = 'cases/fill_balances'
        valid_path = os.path.join(test_path, 'valid', '*.csv')
        invalid_path = os.path.join(test_path, 'invalid', '*.csv')
        paths = glob.glob(valid_path)
        paths += glob.glob(invalid_path)
        return paths

    @classmethod
    def load_cases(cls):
        return [FillBalancesCase(x) for x in FillBalancesCase.find_cases()]


@pytest.mark.parametrize('case', FillBalancesCase.load_cases(), ids=lambda x: x.name)
def test_fill_balances(case):
    # TODO implement
    pass

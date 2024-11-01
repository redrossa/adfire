import json
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from pandas import testing as tm
import pytest

from adfire.adfire import Adfire
from adfire.errors import ChecksumError
from tests.utils import open_or_none

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class Case:
    cases_path = Path('cases/adfire')

    @classmethod
    def find_cases(cls):
        return [cls.cases_path/x for x in os.listdir(cls.cases_path) if os.path.isfile(cls.cases_path/x/'metadata.json')]

    @classmethod
    def load_cases(cls):
        return [Case(x) for x in cls.find_cases()]

    def __init__(self, path):
        self.path = path
        self.input_path = path/'input.csv'

        checksum_path = path/'checksum.pkl'
        self.checksum_path = checksum_path if checksum_path.is_file() else None

        with open_or_none(path/'metadata.json', 'r') as (f, err):
            self.metadata = json.load(f, object_hook=lambda d: SimpleNamespace(**d)) if not err else None

        with open_or_none(path/'output.csv', 'r') as (f, err):
            self.expected_output = None if err else pd.read_csv(f)

        with open_or_none(path/'output.pkl', 'rb') as (f, err):
            self.expected_checksum = None if err else pd.read_pickle(f)


all_cases = Case.load_cases()


@pytest.fixture(params=all_cases, ids=lambda x: x.metadata.name)
def case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if not x.metadata.fails], ids=lambda x: x.metadata.name)
def positive_case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if x.metadata.fails], ids=lambda x: x.metadata.name)
def negative_case(request):
    return request.param


def test_init(positive_case):
    Adfire(positive_case.input_path, positive_case.checksum_path)


def test_init_fails(negative_case):
    with pytest.raises(ChecksumError) as e:
        Adfire(negative_case.input_path, negative_case.checksum_path)


def test_format(positive_case, tmp_path):
    adfire = Adfire(positive_case.input_path, positive_case.checksum_path)
    out_path = tmp_path/'output.csv'
    adfire.format(out_path)
    actual_output = pd.read_csv(out_path)
    tm.assert_frame_equal(actual_output, positive_case.expected_output)
    out_pkl_path = tmp_path/'output.pkl'
    actual_checksum = pd.read_pickle(out_pkl_path)
    tm.assert_series_equal(actual_checksum, positive_case.expected_checksum)

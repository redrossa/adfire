import json
import os
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
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
        self.checksum_path = path/'checksum.pkl'

        with open_or_none(path/'metadata.json', 'r') as (f, err):
            self.metadata = json.load(f, object_hook=lambda d: SimpleNamespace(**d)) if not err else None


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


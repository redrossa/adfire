import os
import shutil
from pathlib import Path

import pandas as pd
import pytest

from adfire import Adfire
from adfire.format import schema
from adfire.io import read_record
from utils import BaseCase, assert_record_equal, assert_record_format

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class Case(BaseCase):
    pass


all_cases = Case.load_cases(Path(__file__).parent/'cases/adfire')


@pytest.fixture(params=all_cases, ids=lambda x: x.metadata.name)
def case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if not x.metadata.fails], ids=lambda x: x.metadata.name)
def positive_case(request):
    return request.param


@pytest.fixture(params=[x for x in all_cases if x.metadata.fails], ids=lambda x: x.metadata.name)
def negative_case(request):
    return request.param


def test_init(positive_case, tmp_path):
    test_path = tmp_path / f'{positive_case.metadata.name}'
    shutil.copytree(positive_case.path, test_path)
    os.chdir(test_path)

    Adfire(*(test_path/x for x in positive_case.metadata.inputs))


def test_init_fails(negative_case, tmp_path):
    test_path = tmp_path / f'{negative_case.metadata.name}'
    shutil.copytree(negative_case.path, test_path)
    os.chdir(test_path)

    with pytest.raises(Exception, match=negative_case.metadata.fails):
        Adfire(*(test_path/x for x in negative_case.metadata.inputs))


def test_format(positive_case, tmp_path):
    test_path = tmp_path / f'{positive_case.metadata.name}'
    shutil.copytree(positive_case.path, test_path)
    os.chdir(test_path)

    adfire = Adfire(*(test_path / x for x in positive_case.metadata.inputs))
    adfire.format()

    actual = [schema(read_record(test_path/x)) for x in positive_case.metadata.inputs]
    expected = [schema(read_record(Path(test_path/x).with_suffix('.out.csv'))) for x in positive_case.metadata.inputs]

    for (a, e) in zip(actual, expected):
        assert_record_format(a)
        assert_record_format(e)
        assert_record_equal(a, e)

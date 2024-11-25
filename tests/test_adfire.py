import os
import shutil
from pathlib import Path

import pandas as pd
import pytest
from pandas import testing as tm

from adfire import Adfire
from adfire.format import schema
from adfire.io import read_record
from utils import Case, dynamic_import

pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)


class AdfireCase(Case):
    def __init__(self, path):
        super().__init__(path)
        if hasattr(self.metadata, 'mod'):
            mod_path = path / self.metadata.mod
            self.mod = dynamic_import(mod_path)


all_cases = AdfireCase.load_cases(Path(__file__).parent / 'cases/adfire')


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

    Adfire(*(test_path / x for x in positive_case.metadata.inputs))


def test_init_fails(negative_case, tmp_path):
    test_path = tmp_path / f'{negative_case.metadata.name}'
    shutil.copytree(negative_case.path, test_path)
    os.chdir(test_path)

    with pytest.raises(Exception, match=negative_case.metadata.fails):
        Adfire(*(test_path / x for x in negative_case.metadata.inputs))


def test_format(positive_case, tmp_path, id_generator):
    test_path = tmp_path / f'{positive_case.metadata.name}'
    shutil.copytree(positive_case.path, test_path)
    os.chdir(test_path)

    input_paths = [test_path / x for x in positive_case.metadata.inputs]
    expected_output_paths = [x.with_suffix('.out.csv') for x in input_paths]

    adfire = Adfire(*input_paths, generate_id=id_generator)
    adfire.format()

    actual = [schema(read_record(x)) for x in input_paths]  # actual paths are the same as input paths
    expected = [schema(read_record(x)) for x in expected_output_paths]

    for (a, e) in zip(actual, expected):
        tm.assert_frame_equal(a, e)

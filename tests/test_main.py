import json
import os
import shlex
import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest

from adfire.__main__ import main, parse_args
from adfire.format import _format_types
from adfire.io import read_record
from utils import open_or_none, assert_record_equal


class Case:
    cases_path = Path(__file__).parent / 'cases/main'

    @classmethod
    def find_cases(cls):
        return [cls.cases_path / x for x in os.listdir(cls.cases_path) if
                os.path.isfile(cls.cases_path / x / 'metadata.json')]

    @classmethod
    def load_cases(cls):
        return [Case(x) for x in cls.find_cases()]

    def __init__(self, path):
        self.path = path

        with open_or_none(path / 'metadata.json', 'r') as (f, err):
            self.metadata = json.load(f, object_hook=lambda d: SimpleNamespace(**d)) if not err else None

        with open_or_none(path / 'out.csv', 'r') as (f, err):
            self.out_record = _format_types(read_record(f.name)) if not err else None


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


def test_positive(positive_case, tmp_path):
    test_path = tmp_path / f'{positive_case.metadata.name}'
    shutil.copytree(positive_case.path, test_path)
    os.chdir(test_path)
    args = shlex.split(positive_case.metadata.args)[1:]
    parsed = parse_args(args)
    main(args)

    for path in parsed.paths:
        actual_record = _format_types(read_record(path))
        assert_record_equal(actual_record, positive_case.out_record)

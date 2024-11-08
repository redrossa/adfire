import json
import os
from contextlib import contextmanager
from types import SimpleNamespace

import pandas as pd
from pandas import testing as tm

from adfire.utils import is_id_transaction_match


class BaseCase:
    @classmethod
    def find_cases(cls, base_path):
        return [base_path / x for x in os.listdir(base_path) if os.path.isfile(base_path / x / 'metadata.json')]

    @classmethod
    def load_cases(cls, base_path):
        return [cls(x) for x in cls.find_cases(base_path)]

    def __init__(self, path):
        self.path = path

        with open_or_none(path/'metadata.json', 'r') as (f, err):
            self.metadata = json.load(f, object_hook=lambda d: SimpleNamespace(**d)) if not err else None



@contextmanager
def open_or_none(filename, mode='r'):
    try:
        f = open(filename, mode)
    except IOError as e:
        yield None, e
    else:
        try:
            yield f, None
        finally:
            f.close()


def assert_record_equal(a: pd.DataFrame, b: pd.DataFrame):
    assert is_id_transaction_match(a['id.transaction'], b['id.transaction'])
    a = a.drop('id.transaction', axis=1)
    b = b.drop('id.transaction', axis=1)
    tm.assert_frame_equal(a, b)
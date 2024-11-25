import importlib
import json
import os
from contextlib import contextmanager
from types import SimpleNamespace

import pandas as pd
import pandera as pa
from pandas import testing as tm

from adfire.schema import SCHEMA_COLUMNS
from adfire.utils import is_id_transaction_match


class Case:
    @classmethod
    def find_cases(cls, base_path):
        return [base_path / x for x in os.listdir(base_path) if os.path.isfile(base_path / x / 'metadata.json')]

    @classmethod
    def load_cases(cls, base_path):
        return [cls(x) for x in cls.find_cases(base_path)]

    def __init__(self, path):
        self.path = path

        with open_or_none(path / 'metadata.json', 'r') as (f, err):
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


assertion_schema = pa.DataFrameSchema(SCHEMA_COLUMNS)


def assert_record_format(df: pd.DataFrame):
    mask_is_posted = df['status'] == 'posted'
    assert df[mask_is_posted]['id.transaction'].notna().all()
    assert df[mask_is_posted]['hash'].notna().all()
    assertion_schema.validate(df)


def assert_record_equal(a: pd.DataFrame, b: pd.DataFrame):
    assert is_id_transaction_match(a['id.transaction'], b['id.transaction'])
    a = a.drop(['id.transaction', 'source', 'hash'], axis=1)
    b = b.drop(['id.transaction', 'source', 'hash'], axis=1)
    tm.assert_frame_equal(a, b)


def dynamic_import(script_path):
    # Create a module specification from the script path
    spec = importlib.util.spec_from_file_location("dynamic_module", script_path)
    # Create a module object from the specification
    module = importlib.util.module_from_spec(spec)
    # Execute the module
    spec.loader.exec_module(module)
    return module

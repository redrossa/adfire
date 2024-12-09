import json
import os
import shutil
import uuid
from pathlib import Path
from types import SimpleNamespace
from typing import Callable

import pandas as pd
from pandera.typing import DataFrame

from adfire.autofill import assign_transactions, hash_transactions, sort_entries, fill_current_balances, \
    fill_available_balances
from adfire.config import RESOURCES_PATH
from adfire.io import read_record
from adfire.schema import MergedInputEntrySchema, EntrySchema


def _read_metadata_from_dir(path: Path) -> SimpleNamespace:
    """Reads 'portfolio.json' in a directory"""
    metadata_path = path / 'portfolio.json'
    try:
        f = open(metadata_path, 'r')
    except FileNotFoundError as e:
        raise FileNotFoundError(f"'{metadata_path}' does not exist") from e
    with f:
        metadata = json.load(f, object_hook=lambda d: SimpleNamespace(**d))
        return metadata

def _read_entry_files_from_dir(path: Path) -> DataFrame[MergedInputEntrySchema]:
    """Reads all entry files in a directory and merge them into a dataframe"""
    records = []
    for item in path.rglob('*.csv'):
        # Skip files and directories that are hidden (start with a '.')
        if any(part.startswith('.') for part in item.parts):
            continue
        item = item.resolve()
        if item.is_file():
            df = read_record(item)
            records.append((item, df))

    if records:
        df = pd.concat([df for name, df in records], keys=[name for name, df in records], names=['path', 'entry_id'])
        df = MergedInputEntrySchema.validate(df)
    else:
        df = None

    return df


class Portfolio:
    def __init__(self, path: os.PathLike, id_gen_func: Callable = uuid.uuid4):
        """Creates a portfolio object from a directory."""
        path = Path(path)

        self._metadata = _read_metadata_from_dir(path)
        self._merged_entry_dfs = _read_entry_files_from_dir(path)
        self.id_gen_func = id_gen_func

    @classmethod
    def from_new(cls, path: os.PathLike, id_gen_func: Callable = uuid.uuid4):
        """
        Defines directory as a portfolio. If 'portfolio.json' exists, raises an error.
        Otherwise, if directory is empty, populate with sample portfolio; if not empty,
        create 'portfolio.json' from sample.
        """
        path = Path(path)
        metadata_path = path / 'portfolio.json'
        metadata_file_exists = metadata_path.is_file()

        if metadata_file_exists:
            raise FileExistsError(f"'{metadata_path}' already exists")

        dir_is_empty = path.exists() and path.is_dir() and not any(path.iterdir())

        if not dir_is_empty:
            sample_file_path = RESOURCES_PATH / 'sample/portfolio.json'
            shutil.copyfile(sample_file_path, metadata_path)
        else:
            sample_path = RESOURCES_PATH / 'sample'
            shutil.copytree(
                sample_path,
                path,
                dirs_exist_ok=True  # because we already know it's empty
            )

        return cls(path, id_gen_func)

    def lint(self):
        """
        Validates entries in this portfolio. If there are invalid entries,
        raises an error.
        """
        # following computations require df to be sorted already
        df = sort_entries(self._merged_entry_dfs)

        # autofill balances
        df = fill_current_balances(df)
        df = fill_available_balances(df)

        # assign ids (include pairing)
        df = assign_transactions(df, id_gen_func=self.id_gen_func)

        # assign hashes (depends on order of entries in the account)
        df = hash_transactions(df)

        # validate with final schema
        df = EntrySchema.validate(df)
        df = df[EntrySchema.to_schema().columns.keys()]

        return df

    def format(self):
        """
        Lints portfolio and modifies entry files with standard formatting and
        implied values.
        """
        pass

import importlib.util
import json
import os
import runpy
import shutil
import sys
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from pandera.typing import DataFrame

from adfire.config import RESOURCES_PATH
from adfire.io import read_record, write_record
from adfire.schema import EntrySchema, BaseInputSchema, CoreSchema


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


def _read_entry_files_from_dir(path: Path) -> DataFrame[BaseInputSchema]:
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
        df = BaseInputSchema.validate(df)
    else:
        df = None

    return df


class Portfolio:
    def __init__(self, path: os.PathLike):
        """Creates a portfolio object from a directory."""
        self.path = Path(path)

        self._metadata = _read_metadata_from_dir(self.path)
        self._merged_entry_dfs = _read_entry_files_from_dir(self.path)
        self._linted = None
        self._forced_hash = False

    @property
    def linted(self) -> DataFrame[BaseInputSchema]:
        if self._linted is None:
            self._linted = self.lint()
        return self._linted

    @property
    def forced_hash(self) -> bool:
        return self._forced_hash

    @forced_hash.setter
    def forced_hash(self, value: bool):
        self._forced_hash = value
        self._linted = None

    @classmethod
    def from_new(cls, path: os.PathLike) -> 'Portfolio':
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

        return cls(path)

    def lint(self) -> DataFrame[CoreSchema]:
        """
        Validates entries in this portfolio. If there are invalid entries,
        raises an error.
        """
        df = CoreSchema.validate(self._merged_entry_dfs)

        return df

    def format(self):
        """
        Lints portfolio and modifies entry files with standard formatting and
        implied values.
        """
        df = self.linted
        groups = df.groupby('path')
        for path, group_df in groups:
            group_df = EntrySchema.validate(group_df)
            group_df = group_df[EntrySchema.to_schema().columns.keys()]
            write_record(group_df, path)

    def view(self, module: str, *args):
        report_path = f'.reports/{module.removeprefix("adfire.")}'
        old_argv = sys.argv
        spec = importlib.util.find_spec(module)
        if spec:
            os.makedirs(report_path, exist_ok=True)
            os.chdir(report_path)
            sys.argv = [module, *args]
        runpy.run_module(module, init_globals={'portfolio': self}, run_name="__main__")
        sys.argv = old_argv
        os.chdir(self.path.resolve())

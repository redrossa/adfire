import json
import os
import shutil
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from pandera.typing import DataFrame

from adfire.autofill import assign_transactions, hash_entries, sort_entries, fill_current_balances, \
    fill_available_balances
from adfire.config import RESOURCES_PATH
from adfire.io import read_record, write_record
from adfire.logger import get_logger
from adfire.schema import MergedInputEntrySchema, EntrySchema, SortedDiffEntrySchema, BalancesDiffEntrySchema

logger = get_logger(__name__)


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
    def __init__(self, path: os.PathLike):
        """Creates a portfolio object from a directory."""
        path = Path(path)

        self._path = path.resolve()
        self._metadata = _read_metadata_from_dir(path)
        self._merged_entry_dfs = _read_entry_files_from_dir(path)

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

    def lint(self) -> DataFrame[MergedInputEntrySchema]:
        """
        Validates entries in this portfolio. If there are invalid entries,
        raises an error.
        """
        # following computations require df to be sorted already
        df = sort_entries(self._merged_entry_dfs)

        # log sorted changes
        old_grouped = self._merged_entry_dfs.groupby('path')
        new_grouped = df.groupby('path')
        for path, old_group_df in old_grouped:
            new_group_df = new_grouped.get_group(path)
            mask_sorted = new_group_df.index != old_group_df.index
            if any(mask_sorted):
                old_df = old_group_df[mask_sorted].reset_index()
                old_df['change'] = '-'
                new_df = new_group_df[mask_sorted].reset_index()
                new_df['change'] = '+'

                diff_df = pd.concat([old_df, new_df]).sort_index()
                diff_df = SortedDiffEntrySchema.validate(diff_df)
                diff_df = diff_df[SortedDiffEntrySchema.to_schema().columns.keys()]

                diff_str = diff_df.to_string(index=False)
                lines = diff_str.splitlines()

                path = path.replace(str(self._path), str(self._path.name))
                logger.info(f'Sorted entries in {path}')
                for s in lines:
                    logger.info(s)

        # autofill balances
        pre_df = df.copy()
        df = fill_current_balances(df)
        df = fill_available_balances(df)

        # log autofill balances
        diff_df = pre_df.compare(df, result_names=('---', '+++'))
        diff_df.columns = diff_df.columns.to_flat_index().str.join('_')
        diff_df = diff_df.reset_index()
        diff_df['path'] = diff_df['path'].str.replace(str(self._path), str(self._path.name))

        if not diff_df.empty:
            for path, group_df in diff_df.groupby('path'):
                group_df = BalancesDiffEntrySchema.validate(group_df)
                group_str = group_df.to_string(index=False)
                lines = group_str.splitlines()
                logger.info(f'Autofill balances in {path}')
                for s in lines:
                    logger.info(s)

        # assign ids (include pairing)
        df = assign_transactions(df)

        # assign hashes (depends on order of entries in the account)
        df = hash_entries(df)

        # round numbers to cents
        df = df.round(2)
        df = df.replace(-0.0, 0.0)

        # validate with final schema
        df = MergedInputEntrySchema.validate(df)
        df = df[MergedInputEntrySchema.to_schema().columns.keys()]

        return df

    def format(self):
        """
        Lints portfolio and modifies entry files with standard formatting and
        implied values.
        """
        df = self.lint()
        groups = df.groupby('path')
        for path, group_df in groups:
            group_df = EntrySchema.validate(group_df)
            group_df = group_df[EntrySchema.to_schema().columns.keys()]
            write_record(group_df, path)

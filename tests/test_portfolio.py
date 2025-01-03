import os
import shutil

import pytest

from adfire.portfolio import Portfolio


class TestPortfolio:
    class TestInit:
        def test_on_empty_dir(self, tmp_path):
            with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
                Portfolio(tmp_path)

        def test_on_missing_metadata_but_non_empty_dir(self, tmp_path, sample_path):
            shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
            os.remove(tmp_path / 'portfolio.json')
            with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
                Portfolio(tmp_path)

        def test_on_no_entry_files(self, tmp_path, sample_path):
            shutil.copyfile(sample_path / 'portfolio.json', tmp_path / 'portfolio.json')
            p = Portfolio(tmp_path)
            assert p._metadata
            assert p._merged_entry_dfs is None

    class TestFromNew:
        def test_on_empty_dir(self, tmp_path):
            p = Portfolio.from_new(tmp_path)
            assert p._metadata
            assert p._merged_entry_dfs is not None

        def test_on_existing_portfolio(self, tmp_path, sample_path):
            shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
            with pytest.raises(FileExistsError, match=f"'{tmp_path}/portfolio.json' already exist"):
                Portfolio.from_new(tmp_path)

        def test_on_missing_metadata_but_non_empty_dir(self, tmp_path, sample_path):
            shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
            os.remove(tmp_path / 'portfolio.json')
            p = Portfolio.from_new(tmp_path)
            assert p._metadata
            assert p._merged_entry_dfs is not None

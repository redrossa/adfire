import os
import shutil
import sys
from pathlib import Path

import pytest

from adfire.__main__ import main
from tests.utils import dir_is_equal


@pytest.mark.parametrize('option', [
    '-h', '--help',
    '-v', '--version',
])
def test_option_exists(option):
    sys.argv = ['adfire', option]
    with pytest.raises(SystemExit, match='0'):
        main()


class TestInitMode:
    def test_on_empty_dir(self, tmp_path, sample_path):
        sys.argv = ['adfire', 'init', '--path', str(tmp_path)]
        main()
        assert dir_is_equal(sample_path, tmp_path)

    def test_on_uninitialized_portfolio(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
        os.remove(tmp_path / 'portfolio.json')

        sys.argv = ['adfire', 'init', '--path', str(tmp_path)]
        main()
        assert dir_is_equal(sample_path, tmp_path)

    def test_on_initialized_portfolio(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)

        sys.argv = ['adfire', 'init', '--path', str(tmp_path)]
        with pytest.raises(FileExistsError, match=f"'{tmp_path}/portfolio.json' already exists"):
            main()

    def test_default_path(self, tmp_path, sample_path):
        os.chdir(tmp_path)

        sys.argv = ['adfire', 'init']
        main()
        assert dir_is_equal(sample_path, Path.cwd())


class TestLintMode:
    def test_on_empty_dir(self, tmp_path):
        sys.argv = ['adfire', 'lint', '--path', str(tmp_path)]
        with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
            main()

    def test_on_uninitialized_portfolio(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
        os.remove(tmp_path / 'portfolio.json')

        sys.argv = ['adfire', 'lint', '--path', str(tmp_path)]
        with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
            main()

    def test_on_initialized_portfolio(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)

        sys.argv = ['adfire', 'lint', '--path', str(tmp_path)]
        main()

    def test_default_path(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
        os.chdir(tmp_path)

        sys.argv = ['adfire', 'lint']
        main()


class TestFormat:
    def test_on_empty_dir(self, tmp_path):
        sys.argv = ['adfire', 'format', '--path', str(tmp_path)]
        with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
            main()

    def test_on_uninitialized_portfolio(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
        os.remove(tmp_path / 'portfolio.json')

        sys.argv = ['adfire', 'format', '--path', str(tmp_path)]
        with pytest.raises(FileNotFoundError, match=f"'{tmp_path}/portfolio.json' does not exist"):
            main()

    @pytest.mark.freeze_uuids(
        side_effect='auto_increment',
        values=['00000000-0000-0000-0000-000000000000',]
    )
    def test_on_initialized_portfolio(self, tmp_path, sample_path, sample_formatted_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)

        sys.argv = ['adfire', 'format', '--path', str(tmp_path)]
        main()
        assert dir_is_equal(tmp_path, sample_formatted_path)

    def test_default_path(self, tmp_path, sample_path):
        shutil.copytree(sample_path, tmp_path, dirs_exist_ok=True)
        os.chdir(tmp_path)

        sys.argv = ['adfire', 'format']
        main()

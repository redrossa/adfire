import shutil
from pathlib import Path

from adfire.config import RESOURCES_PATH


class Portfolio:
    def __init__(self, path: str):
        self.path = Path(path)

    def create(self):
        """
        Defines this directory as a portfolio by creating a 'portfolio.json'
        file if it doesn't exist. If it exists, it raises an error.
        """
        portfolio_json = self.path / 'portfolio.json'
        portfolio_exists = portfolio_json.is_file()
        is_empty = self.path.exists() and self.path.is_dir() and not any(self.path.iterdir())

        if portfolio_exists:
            raise FileExistsError('portfolio.json already exists')

        if not is_empty:
            sample_portfolio_json = RESOURCES_PATH / 'sample/portfolio.json'
            shutil.copy(sample_portfolio_json, portfolio_json)
        else:
            sample_path = RESOURCES_PATH / 'sample'
            shutil.copytree(sample_path, self.path, dirs_exist_ok=True)  # dirs_exist_ok because we already know it's empty


    def lint(self):
        print('lint: validates the portfolio content')

    def format(self):
        print('format: lint + commits changes by overwriting to input files')

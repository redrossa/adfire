import pandas as pd

from adfire.lint.accounts import AccountLinter


class Linter(AccountLinter):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        return super().lint(df)

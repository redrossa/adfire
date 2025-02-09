import pandas as pd


class LintError(Exception):
    def __init__(self, message, df: pd.DataFrame):
        super().__init__(message)
        self.message = message
        self.df = df

    def __str__(self):
        return f'{self.message}\n{self.df.to_string()}'
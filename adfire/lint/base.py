from abc import ABC, abstractmethod

import pandas as pd
import pandera as pa
from pandera.typing import Index


class BaseInputSchema(pa.DataFrameModel):
    path: Index[str]
    entry_id: Index[int]

    class Config:
        strict = False
        coerce = True


class BaseLinter(ABC):
    @abstractmethod
    def lint(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Processes the DataFrame and returns a modified DataFrame.
        """
        pass
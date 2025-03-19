from abc import ABC, abstractmethod

from pandera import DataFrameModel
from pandera.typing import Index


class BaseInputSchema(DataFrameModel):
    path: Index[str]
    entry_id: Index[int]

    class Config:
        strict = False
        coerce = True


class BaseConfig(ABC):
    def __init__(self, name=None, **kwargs):
        self._name = name

    @abstractmethod
    def apply_entries(self, entries_df):
        pass

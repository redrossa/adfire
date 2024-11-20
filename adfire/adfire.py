import uuid
from typing import Callable

import pandas as pd

from adfire.format import format_record
from adfire.io import read_record, write_record


class Adfire:
    def __init__(self, *paths: str, generate_id: Callable = lambda: str(uuid.uuid4())):
        records = [read_record(p) for p in paths]
        compiled = pd.concat(records, ignore_index=True)
        self.formatted = format_record(compiled, generate_id)

    def format(self):
        for source, group in self.formatted.groupby('source'):
            group = group.drop('source', axis=1)
            write_record(group, source)
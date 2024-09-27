import numpy as np
import pandas as pd
import pandera as pa


schema = pa.DataFrameSchema({
    'date': pa.Column(pa.DateTime),
    'description': pa.Column(str),
    'amount': pa.Column(float),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str)
}, coerce=True)


def format_types(record: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(record)


def sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record['amount.asset'] = np.where(record['type'] == 'depository', record['amount'], -record['amount'])
    return record.sort_values(by=['date', 'amount.asset'], ascending=[True, False], ignore_index=True)


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    typed = format_types(record)
    sorted = sort_record(typed)
    return sorted

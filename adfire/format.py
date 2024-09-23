import numpy as np
import pandas as pd


SCHEMA = {
    'date': 'datetime64[ns]',
    'description': 'object',
    'amount': 'float64',
    'account': 'object',
    'mask': 'object',
    'type': 'object',
    'subtype': 'object'
}


def format_types(record: pd.DataFrame) -> pd.DataFrame:
    return record.astype(SCHEMA)



def sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record['amount.asset'] = np.where(record['type'] == 'depository', record['amount'], -record['amount'])
    return record.sort_values(by=['date', 'amount.asset'], ascending=[True, False])


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    typed = format_types(record)
    sorted = sort_record(typed)
    return sorted

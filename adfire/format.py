import numpy as np
import pandas as pd
import pandera as pa


schema = pa.DataFrameSchema({
    'date': pa.Column(pa.DateTime),
    'description': pa.Column(str),
    'category': pa.Column(str),
    'amount': pa.Column(float),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str),
    'balances.current': pa.Column(float, nullable=True),
    'balances.available': pa.Column(float, nullable=True), # ideally nullable only if limit is null
    'balances.limit': pa.Column(float, nullable=True),
}, coerce=True, strict='filter')


def _format_types(record: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(record)


def _sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record['amount.asset'] = np.where(record['type'] == 'depository', record['amount'], -record['amount'])
    return record.sort_values(by=['date', 'amount.asset'], ascending=[True, False], ignore_index=True)


def _fill_balances(record: pd.DataFrame) -> pd.DataFrame:
    record['balances.current'] = record.groupby('account')['amount'].cumsum()
    record['balances.available'] = record['balances.limit'] - record['balances.current']
    return record


def format_record(record: pd.DataFrame) -> pd.DataFrame:
    typed = _format_types(record)
    sorted = _sort_record(typed)
    filled = _fill_balances(sorted)
    df = _format_types(filled)
    return df

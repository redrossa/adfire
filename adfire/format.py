import numpy as np
import pandas as pd
import pandera as pa


schema = pa.DataFrameSchema({
    'id.transaction': pa.Column(str, nullable=True),
    'date': pa.Column(pa.DateTime),
    'entity': pa.Column(str),
    'description': pa.Column(str, nullable=True),
    'category': pa.Column(str),
    'amount': pa.Column(float),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str),
    'balances.current': pa.Column(float, nullable=True),
    'balances.available': pa.Column(float, nullable=True), # ideally nullable only if limit is null
    'balances.limit': pa.Column(float, nullable=True),
}, coerce=True, strict='filter', add_missing_columns=True)


def add_col_worth(record: pd.DataFrame) -> pd.DataFrame:
    record['worth'] = np.where(record['type'] == 'depository', record['amount'], -record['amount'])
    return record


def _format_types(record: pd.DataFrame) -> pd.DataFrame:
    return schema.validate(record)


def _sort_record(record: pd.DataFrame) -> pd.DataFrame:
    record = add_col_worth(record)
    return record.sort_values(by=['date', 'worth'], ascending=[True, False], ignore_index=True)


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

import pandas as pd
import pandera as pa

from adfire.io import write_json
from adfire.lint import BaseInputSchema
from adfire.lint.accounts import parse_config
from adfire.utils import namespace_to_dict


class OverviewTransactionSchema(BaseInputSchema):
    date: str
    account: str
    mask: str = pa.Field(nullable=True)
    name: str
    amount: float
    symbol: str
    rate: float
    category: str = pa.Field(nullable=True)
    worth: float

    class Config:
        strict = 'filter'


def round_to_whole(v: float):
    v = abs(v)
    i = int(v)
    return i if v - i == 0 else v


def is_purchase(df: pd.DataFrame):
    if len(df) != 2:
        return []

    from_entry_df = df[df['worth'] < 0]
    to_entry_df = df[df['worth'] >= 0]
    if len(from_entry_df) != len(to_entry_df):
        return []

    from_entry = from_entry_df.iloc[0]
    to_entry = to_entry_df.iloc[0]
    if from_entry['account_id'] < 0 or to_entry['account_id'] >= 0:
        return []
    if from_entry['symbol'] != to_entry['symbol']:
        return []

    return [{
        'date': to_entry['date'],
        'title': to_entry['account'],
        'amount': from_entry['worth'],
        'category': to_entry['category'],
        'account': from_entry['account'],
        'type': 'purchase'
    }]


def is_income(df: pd.DataFrame):
    from_entries_df = df[df['worth'] < 0]
    if any(from_entries_df['account_id'] > 0):
        return []

    to_entries_df = df[df['worth'] >= 0]
    to_categories = to_entries_df['category'].fillna('')

    mask_is_owned = to_entries_df['account_id'] > 0
    mask_is_income = to_categories.str.startswith('income')
    mask_not_income_pretax = ~to_categories.str.startswith('income.pretax')
    income_entries_df = to_entries_df[mask_is_owned & mask_is_income & mask_not_income_pretax]
    if len(income_entries_df) == 0:
        return []

    txs = []
    for index, row in income_entries_df.iterrows():
        txs.append({
            'date': row['date'],
            'title': row['name'],
            'amount': row['worth'],
            'category': row['category'],
            'account': row['account'],
            'type': 'income'
        })
    return txs


def is_close(df: pd.DataFrame):
    pnl_df = df[df['account'] == 'P/L']
    if len(pnl_df) != 1:
        return []

    pnl_entry = pnl_df.iloc[0]
    pnl_excl_df = df[~df.index.isin(pnl_df.index)]

    from_entries_df = pnl_excl_df[pnl_excl_df['worth'] < 0]
    if any(from_entries_df['account_id'] < 0):
        return []

    to_entries_df = pnl_excl_df[pnl_excl_df['worth'] >= 0]
    to_owned_entries_df = to_entries_df[to_entries_df['account_id'] >= 0]
    if len(to_owned_entries_df) != 1:
        return []

    to_entry = to_owned_entries_df.iloc[0]
    positions = from_entries_df.groupby('symbol')['amount'].sum()
    positions = positions[positions.index != to_entry['symbol']]

    txs = []
    for symbol, amount in positions.items():
        return [{
            'date': pnl_entry['date'],
            'title': f'Close {round_to_whole(amount)} {symbol}',
            'amount': -pnl_entry['worth'],
            'account': to_entry['account'],
            'type': 'close'
        }]


def is_open(df: pd.DataFrame):
    from_entries_df = df[df['worth'] < 0]
    from_owned_entries_df = from_entries_df[from_entries_df['account_id'] >= 0]
    if len(from_owned_entries_df) != 1:
        return []

    from_entry = from_owned_entries_df.iloc[0]

    to_entries_df = df[df['worth'] >= 0]
    to_owned_entries_df = to_entries_df[to_entries_df['account_id'] >= 0]
    if len(to_owned_entries_df) == 0:
        return []

    txs = []
    for index, row in to_owned_entries_df.iterrows():
        if row['symbol'] == from_entry['symbol']:
            continue
        txs.append({
            'date': row['date'],
            'title': f'Open {round_to_whole(row["amount"])} {row["symbol"]}',
            'amount': from_entry['amount'],
            'account': from_entry['account'],
            'type': 'open'
        })
    return txs


def map_accounts(df: pd.DataFrame, config) -> pd.DataFrame:
    mapping_df = parse_config(config)
    mapped_df = df.merge(mapping_df, left_on=['account', 'mask'], right_on=['account', 'mask'], how='left')
    mask_unowned = mapped_df['account_id'].isna()
    mapping_df = {x: -i for i, x in enumerate(mapped_df.loc[mask_unowned, 'account'].unique(), 1)}
    mapped_df.loc[mask_unowned, 'account_id'] = mapped_df.loc[mask_unowned, 'account'].map(mapping_df)
    mapped_df['account_id'] = mapped_df['account_id'].astype(int)
    return mapped_df


def report_highlights(df: pd.DataFrame, config):
    df: pd.DataFrame = OverviewTransactionSchema(df)
    df = map_accounts(df, config)
    grouped = df.groupby('name')
    txs = []
    for name, group in grouped:
        if closes := is_close(group):
            txs += closes
        elif opens := is_open(group):
            txs += opens
        elif purchases := is_purchase(group):
            txs += purchases
        elif incomes := is_income(group):
            txs += incomes
    txs = sorted(txs, key=lambda x: x['date'], reverse=True)
    write_json(txs, '.reports/index.json')


def report_config(config):
    obj = namespace_to_dict(config)
    write_json(obj, '.reports/config.json')


def report(df: pd.DataFrame, config):
    report_highlights(df, config)
    report_config(config)

import numpy as np
import pandas as pd
import pytest

from adfire.config import RESOURCES_PATH


@pytest.fixture
def sample_path():
    return RESOURCES_PATH / 'sample'


@pytest.fixture
def sample_formatted_path():
    return RESOURCES_PATH / 'sample_formatted'


@pytest.fixture
def unsorted_entries():
    df = pd.DataFrame({
        'path': ['', '', '', '', '', '', ''],
        'entry_id': [0, 0, 1, 0, 0, 0, 0],
        'date': ['2057-06-26', '2073-04-03', '2049-09-24', '2033-05-26', '2088-10-18', '2049-09-24', '2035-09-12'],
        'status': ['', '', '', '', '', '', ''],
        'amount': ['', '', '', '', '', '', ''],
        'balance_current': ['', '', '', '', '', '', ''],
        'balance_available': ['', '', '', '', '', '', ''],
        'balance_limit': ['', '', '', '', '', '', ''],
        'entity': ['', '', '', '', '', '', ''],
        'account_name': ['', '', '', '', '', '', ''],
        'account_mask': ['', '', '', '', '', '', ''],
        'account_type': ['', '', '', '', '', '', ''],
        'account_subtype': ['', '', '', '', '', '', ''],
        'description': ['', '', '', '', '', '', ''],
        'category': ['', '', '', '', '', '', ''],
        'transaction_id': ['', '', '', '', '', '', ''],
        'hash': ['', '', '', '', '', '', ''],
    })
    df = df.set_index(['path', 'entry_id'])
    return df


@pytest.fixture
def unfilled_current_balances():
    df = pd.DataFrame({
        'path': ['a', 'b', 'c', 'd', 'e', 'c', 'f'],
        'entry_id': [0, 0, 1, 0, 0, 0, 0],
        'date': ['2057-06-26', '2073-04-03', '2049-09-24', '2033-05-26', '2088-10-18', '2049-09-24', '2035-09-12'],
        'status': ['', '', '', '', '', '', ''],
        'amount': [13.73, 50.46, 46.88, 12.41, 25.57, 51.14, 21.14],
        'balance_current': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        'balance_available': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        'balance_limit': [np.nan, np.nan, np.nan, np.nan, np.nan, np.nan, np.nan],
        'entity': ['', '', '', '', '', '', ''],
        'account_name': ['', '', '', '', '', '', ''],
        'account_mask': ['', '', '', '', '', '', ''],
        'account_type': ['', '', '', '', '', '', ''],
        'account_subtype': ['', '', '', '', '', '', ''],
        'description': ['', '', '', '', '', '', ''],
        'category': ['', '', '', '', '', '', ''],
        'transaction_id': ['', '', '', '', '', '', ''],
        'hash': ['', '', '', '', '', '', ''],
    })
    df = df.set_index(['path', 'entry_id'])
    return df


@pytest.fixture
def posted_repeat_entry():
    return pd.Series({
        'path': 'a',
        'entry_id': 0,
        'date': pd.to_datetime('2024-08-16').date(),
        'status': 'posted',
        'repeat': 'RRULE:FREQ=MONTHLY',
        'amount': 8.65,
        'balance_current': np.nan,
        'balance_total': np.nan,
        'balance_available': np.nan,
        'balance_limit': 700.0,
        'entity': 'Spotify',
        'account_name': 'Chase Freedom Unlimited',
        'account_mask': 'xxxx',
        'account_type': 'credit',
        'account_subtype': 'credit card',
        'description': np.nan,
        'category': np.nan,
        'transaction_id': np.nan,
        'hash': np.nan,
    })

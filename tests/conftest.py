from pathlib import Path

import numpy as np
import pandas as pd
import pytest


@pytest.fixture
def resources_path():
    return Path(__file__).parent.parent / 'resources'


@pytest.fixture
def sample_path(resources_path):
    return resources_path / 'sample'


@pytest.fixture
def sample_formatted_path(resources_path):
    return resources_path / 'sample_formatted'


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

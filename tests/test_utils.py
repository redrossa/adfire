from pathlib import Path

import pandas as pd
import pytest

from adfire.io import read_checksum, read_record
from adfire.utils import is_checksum_subset, is_id_transaction_match
from utils import Case, open_or_none


class IsIdTransactionMatchCase(Case):
    def __init__(self, path):
        super().__init__(path)

        with open_or_none(path/'a.csv', 'r') as (f, err):
            self.a = None if err else pd.read_csv(f, skip_blank_lines=False, dtype=str)['id.transaction']

        with open_or_none(path/'b.csv', 'r') as (f, err):
            self.b = None if err else pd.read_csv(f, skip_blank_lines=False, dtype=str)['id.transaction']


all_is_id_transaction_match_cases = IsIdTransactionMatchCase.load_cases(Path(__file__).parent/'cases/utils/is_id_transaction_match')


@pytest.fixture(params=all_is_id_transaction_match_cases, ids=lambda x: x.metadata.name)
def is_id_transaction_match_case(request):
    return request.param


def test_is_id_transaction_match(is_id_transaction_match_case):
    assert is_id_transaction_match(is_id_transaction_match_case.a, is_id_transaction_match_case.b) != is_id_transaction_match_case.metadata.fails


class IsChecksumSubsetCase(Case):
    def __init__(self, path):
        super().__init__(path)

        with open_or_none(path/'a.pkl', 'rb') as (f, err):
            self.a = None if err else read_checksum(f)

        with open_or_none(path/'b.pkl', 'rb') as (f, err):
            self.b = None if err else read_checksum(f)


all_is_checksum_subset_cases = IsChecksumSubsetCase.load_cases(Path(__file__).parent/'cases/utils/is_checksum_subset')


@pytest.fixture(params=all_is_checksum_subset_cases, ids=lambda x: x.metadata.name)
def is_checksum_subset_case(request):
    return request.param


def test_is_checksum_subset(is_checksum_subset_case):
    assert is_checksum_subset(is_checksum_subset_case.a, is_checksum_subset_case.b) != is_checksum_subset_case.metadata.fails

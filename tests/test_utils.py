from pathlib import Path

import pytest

from adfire.io import read_checksum
from adfire.utils import is_checksum_subset
from utils import BaseCase, open_or_none


def test_is_id_transaction_match():
    pass


class IsChecksumSubsetCase(BaseCase):
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

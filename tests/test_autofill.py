from pandas.testing import assert_series_equal

from adfire.autofill import sort_entries, fill_current_balances, hash_entries, assign_transactions
from adfire.io import read_record
from adfire.schema import MergedInputEntrySchema


class TestSortEntries:
    def test_should_sort_by_date_then_entries(self, unsorted_entries):
        df = sort_entries(unsorted_entries)
        df = df.reset_index()
        actual = list(df[['date', 'entry_id']].itertuples(index=False, name=None))
        expected = sorted(actual, key=lambda i: f'{i[0]} {i[1]}')
        assert actual == expected


class TestFillCurrentBalances:
    def test_should_assume_account_starting_balance_is_0_if_all_balances_null(self, unfilled_current_balances):
        df = fill_current_balances(unfilled_current_balances)
        actual = df['balance_current']
        expected = unfilled_current_balances['balance_current'].cumsum()
        assert_series_equal(actual, expected)


class TestAssignTransactions:
    def test_should_keep_assigned_transactions_unchanged(self, sample_formatted_path):
        path = sample_formatted_path / 'accounts/chase freedom student.csv'
        df = read_record(path)
        df['path'] = path
        df['entry_id'] = df.index
        df = df.set_index(['path', 'entry_id'])
        df = MergedInputEntrySchema.validate(df)
        actual = assign_transactions(df)['transaction_id']
        expected = df['transaction_id']
        assert_series_equal(actual, expected)


class TestHashTransactions:
    def test_should_keep_hashed_entries_unchanged(self, sample_formatted_path):
        path = sample_formatted_path / 'accounts/chase freedom student.csv'
        df = read_record(path)
        df['path'] = path
        df['entry_id'] = df.index
        df = df.set_index(['path', 'entry_id'])
        df = MergedInputEntrySchema.validate(df)
        actual = hash_entries(df)['hash']
        expected = df['hash']
        assert_series_equal(actual, expected)

import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal

from adfire.autofill import sort_entries, fill_current_balances, hash_entries, assign_transactions, \
    post_repeat_entries
from adfire.io import read_record
from adfire.schema import MergedInputEntrySchema


class TestExplodeRepeatEntries:
    class TestShouldPost:
        def test_when_repeat_entry_becomes_posted(self, posted_repeat_entry):
            df = posted_repeat_entry.to_frame().T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)
            assert len(df) == 2

            r1 = posted_repeat_entry.copy()
            r1['repeat'] = np.nan
            assert_series_equal(df.reset_index().iloc[0], r1, check_names=False)

            r2 = posted_repeat_entry.copy()
            r2['entry_id'] = 1
            r2['date'] = pd.to_datetime('2024-09-16').date()
            r2['status'] = 'pending'
            assert_series_equal(df.reset_index().iloc[1], r2, check_names=False)

        def test_when_posted_entry_with_same_date_added(self, posted_repeat_entry):
            posted_entry = posted_repeat_entry.copy()
            posted_entry['entry_id'] = 1
            posted_entry['repeat'] = np.nan

            df = pd.concat([posted_repeat_entry, posted_entry], axis=1).T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)
            assert len(df) == 3

            r1 = posted_repeat_entry.copy()
            r1['repeat'] = np.nan
            assert_series_equal(df.reset_index().iloc[0], r1, check_names=False)

            assert_series_equal(df.reset_index().iloc[1], posted_entry, check_names=False)

            r2 = posted_repeat_entry.copy()
            r2['entry_id'] = 2
            r2['date'] = pd.to_datetime('2024-09-16').date()
            r2['status'] = 'pending'
            assert_series_equal(df.reset_index().iloc[2], r2, check_names=False)

        def test_posted_entry_with_later_date_added(self, posted_repeat_entry):
            unposted_repeat_entry = posted_repeat_entry.copy()
            unposted_repeat_entry['status'] = 'pending'

            posted_entry = posted_repeat_entry.copy()
            posted_entry['entry_id'] = 1
            posted_entry['date'] = pd.to_datetime('2024-08-20').date()
            posted_entry['repeat'] = np.nan

            df = pd.concat([unposted_repeat_entry, posted_entry], axis=1).T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)
            assert len(df) == 3

            r1 = posted_repeat_entry.copy()
            r1['repeat'] = np.nan
            assert_series_equal(df.reset_index().iloc[0], r1, check_names=False)

            assert_series_equal(df.reset_index().iloc[1], posted_entry, check_names=False)

            r3 = posted_repeat_entry.copy()
            r3['entry_id'] = 2
            r3['date'] = pd.to_datetime('2024-09-16').date()
            r3['status'] = 'pending'
            assert_series_equal(df.reset_index().iloc[2], r3, check_names=False)

        def test_when_posted_entry_with_much_later_date_added(self, posted_repeat_entry):
            unposted_repeat_entry = posted_repeat_entry.copy()
            unposted_repeat_entry['status'] = 'pending'

            posted_entry1 = posted_repeat_entry.copy()
            posted_entry1['entry_id'] = 1
            posted_entry1['date'] = pd.to_datetime('2024-08-20').date()
            posted_entry1['repeat'] = np.nan

            posted_entry2 = posted_repeat_entry.copy()
            posted_entry2['entry_id'] = 2
            posted_entry2['date'] = pd.to_datetime('2024-11-20').date()
            posted_entry2['repeat'] = np.nan

            df = pd.concat([unposted_repeat_entry, posted_entry1, posted_entry2], axis=1).T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)
            assert len(df) == 7

            r1 = posted_repeat_entry.copy()
            r1['repeat'] = np.nan
            assert_series_equal(df.reset_index().iloc[0], r1, check_names=False)

            assert_series_equal(df.reset_index().iloc[1], posted_entry1, check_names=False)

    class TestShouldNotPost:
        def test_when_only_pending(self, posted_repeat_entry):
            unposted_repeat_entry = posted_repeat_entry.copy()
            unposted_repeat_entry['status'] = 'pending'

            df = unposted_repeat_entry.to_frame().T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)
            assert len(df) == 1

            assert_series_equal(df.reset_index().iloc[0], unposted_repeat_entry, check_names=False)

        def test_when_no_later_posted(self, posted_repeat_entry):
            posted_entry = posted_repeat_entry.copy()
            posted_entry['entry_id'] = 0
            posted_entry['date'] = pd.to_datetime('2024-08-01').date()
            posted_entry['repeat'] = np.nan

            unposted_repeat_entry = posted_repeat_entry.copy()
            unposted_repeat_entry['entry_id'] = 1
            unposted_repeat_entry['status'] = 'pending'

            df = pd.concat([posted_entry, unposted_repeat_entry], axis=1).T
            df = df.set_index(['path', 'entry_id'])
            df = post_repeat_entries(df)

            assert_series_equal(df.reset_index().iloc[0], posted_entry, check_names=False)
            assert_series_equal(df.reset_index().iloc[1], unposted_repeat_entry, check_names=False)


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

import datetime

import numpy as np
import pandas as pd
import pandas.testing as tm
import pytest
from pandera.errors import SchemaError

from adfire.schema import InputEntrySchema


class TestInputEntrySchema:
    def test_on_empty(self):
        df = pd.DataFrame()
        with pytest.raises(SchemaError):
            InputEntrySchema.validate(df)

    def test_on_minimum_user_input(self):
        expected = pd.DataFrame(
            {
                'transaction_id': [None],
                'date': [datetime.date(2024, 8, 24)],
                'description': [None],
                'entity': ['Fogo de Chao'],
                'category': [None],
                'amount': [70.0],
                'status': ['posted'],
                'account_name': ['Amex Gold'],
                'account_mask': ['00000'],
                'account_type': ['credit'],
                'account_subtype': ['credit card'],
                'balance_current': [np.nan],
                'balance_available': [np.nan],
                'balance_limit': [np.nan],
                'hash': [None]
            }
        )

        df = pd.DataFrame(
            {
                'date': ['2024-08-24'],
                'entity': ['Fogo de Chao'],
                'amount': ['70'],
                'status': ['posted'],
                'account_name': ['Amex Gold'],
                'account_mask': ['00000'],
                'account_type': ['credit'],
                'account_subtype': ['credit card'],
            }
        )
        actual = InputEntrySchema.validate(df)

        tm.assert_frame_equal(expected, actual)

    def test_missing_required_col(self):
        df = pd.DataFrame(
            {
                'date': ['2024-08-24'],
                'entity': ['Fogo de Chao'],
                'status': ['posted'],
                'account_name': ['Amex Gold'],
                'account_mask': ['00000'],
                'account_type': ['credit'],
                'account_subtype': ['credit card'],
            }
        )

        with pytest.raises(SchemaError, match="column 'amount'"):
            InputEntrySchema.validate(df)

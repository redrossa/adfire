import pandera as pa
from pandera.typing import Index


class EntrySchema(pa.DataFrameModel):
    date: pa.Date
    status: str
    amount: float
    balance_current: float
    balance_available: float = pa.Field(nullable=True)
    balance_limit: float = pa.Field(nullable=True)
    entity: str
    account_name: str
    account_mask: str
    account_type: str
    account_subtype: str
    description: str = pa.Field(nullable=True)
    category: str = pa.Field(nullable=True)
    transaction_id: str = pa.Field(nullable=True)
    hash: str = pa.Field(nullable=True)


class InputEntrySchema(EntrySchema):
    balance_current: float = pa.Field(nullable=True)
    hash: str = pa.Field(nullable=True)

    class Config:
        add_missing_columns = True
        coerce = True
        strict = 'filter'


class MergedInputEntrySchema(InputEntrySchema):
    path: Index[str]
    entry_id: Index[int]


class HashableEntrySchema(MergedInputEntrySchema):
    status: str = pa.Field(eq='posted')
    balance_current: float = pa.Field(nullable=False)
    transaction_id: str = pa.Field(nullable=False)

    @classmethod
    def to_schema(cls) -> pa.DataFrameSchema:
        schema = super().to_schema()
        return schema.remove_columns(['description', 'category', 'hash'])

    class Config:
        drop_invalid_rows = True


class AccountBalancesSchema(pa.DataFrameModel):
    account_name: Index[str]
    balance_current: float

    class Config:
        strict = 'filter'

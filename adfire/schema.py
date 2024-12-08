import pandera as pa


class EntrySchema(pa.DataFrameModel):
    transaction_id: str = pa.Field(nullable=True)
    date: pa.Date
    description: str = pa.Field(nullable=True)
    entity: str
    category: str = pa.Field(nullable=True)
    amount: float
    status: str
    account_name: str
    account_mask: str
    account_type: str
    account_subtype: str
    balance_current: float
    balance_available: float = pa.Field(nullable=True)
    balance_limit: float = pa.Field(nullable=True)
    hash: str


class InputEntrySchema(EntrySchema):
    balance_current: float = pa.Field(nullable=True)
    hash: str = pa.Field(nullable=True)

    class Config:
        add_missing_columns = True
        coerce = True
        strict = 'filter'

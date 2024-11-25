import pandera as pa

SCHEMA_COLUMNS = {
    'id.transaction': pa.Column(str, nullable=True),
    'date': pa.Column(pa.DateTime),
    'entity': pa.Column(str),
    'description': pa.Column(str, nullable=True),
    'category': pa.Column(str),
    'amount': pa.Column(float),
    'worth': pa.Column(float, nullable=True),
    'status': pa.Column(str),
    'account': pa.Column(str),
    'mask': pa.Column(str),
    'type': pa.Column(str),
    'subtype': pa.Column(str),
    'balances.current': pa.Column(float, nullable=True),
    'balances.available': pa.Column(float, nullable=True), # ideally nullable only if limit is null
    'balances.limit': pa.Column(float, nullable=True),
    'hash': pa.Column(str, nullable=True),
}
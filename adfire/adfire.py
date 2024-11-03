from pathlib import Path

from adfire.errors import ChecksumError
from adfire.format import format_record, hash_record, is_checksum_subset, compile_records
from adfire.io import read_record, write_record, read_checksum, write_checksum


class Adfire:
    def __init__(self, *paths: str, checksum_path: str = None):
        records = [read_record(p) for p in paths]
        compiled = compile_records(records)
        self.record = format_record(compiled)
        self.checksum = hash_record(self.record)
        if checksum_path:
            input_checksum = read_checksum(checksum_path)
            try:
                assert is_checksum_subset(input_checksum, self.checksum), 'Record failed integrity check'
            except AssertionError as e:
                raise ChecksumError(e)

    def format(self, out_path):
        file_path = Path(out_path)
        checksum_path = file_path.with_suffix('.pkl')
        write_record(self.record, out_path)
        write_checksum(self.checksum, checksum_path)
from pathlib import Path

from adfire.errors import ChecksumError
from adfire.format import format_record, hash_record, is_checksum_subset
from adfire.io import read_record, write_record, read_checksum, write_checksum


class Adfire:
    def __init__(self, path, checksum_path = None):
        self.path = path
        self.checksum_path = checksum_path
        self.record = read_record(path)
        self.record = format_record(self.record)
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
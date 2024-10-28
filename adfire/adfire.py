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
            assert is_checksum_subset(input_checksum, self.checksum), 'Record failed integrity check'

    def format(self, path = None, checksum_path = None):
        self.record = self.record.round(2)
        write_record(self.record, self.path if not path else path)
        write_checksum(self.checksum, self.checksum_path if not checksum_path else checksum_path)
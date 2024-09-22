from adfire.io import read_record, write_record
from adfire.format import format_record


class Adfire:
    def __init__(self, path):
        self.path = path
        self.record = read_record(path)
        self.record = format_record(self.record)

    def format(self, path = None):
        self.record = self.record.round(2)
        write_record(self.record, self.path if not path else path)
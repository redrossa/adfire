import argparse

from adfire.adfire import Adfire

parser = argparse.ArgumentParser(description='Adfire CLI')
parser.add_argument('path', nargs='+', help='the input record path')
parser.add_argument('-k', '--checksum_path', help='the input checksum file')
parser.add_argument('-o', '--out', help='the formatted record output path')

args = parser.parse_args()

if __name__ == '__main__':
    adfire = Adfire(*args.path, checksum_path=args.checksum_path)
    adfire.format(args.out)
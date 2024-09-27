import argparse

from adfire.adfire import Adfire

parser = argparse.ArgumentParser(description='Adfire CLI')
parser.add_argument('path', help='the record path')
parser.add_argument('-o', '--out', help='the format record output path')

args = parser.parse_args()

if __name__ == '__main__':
    adfire = Adfire(args.path)
    adfire.format(args.out)
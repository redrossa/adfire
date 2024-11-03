import argparse

from adfire.adfire import Adfire


def main():
    parser = argparse.ArgumentParser(description='Adfire CLI')
    parser.add_argument('paths', nargs='+', help='list of input record paths, space separated')
    parser.add_argument('-k', '--checksum_path', help='the input checksum file')
    parser.add_argument('-o', '--out', help='the formatted record output path')

    args = parser.parse_args()

    adfire = Adfire(*args.paths, checksum_path=args.checksum_path)
    adfire.format(args.out)


if __name__ == '__main__':
    main()
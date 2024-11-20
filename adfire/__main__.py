import argparse
import importlib

from adfire.adfire import Adfire


def parse_args(args: list[str] | None = None):
    parser = argparse.ArgumentParser(description='Adfire CLI')
    parser.add_argument(
        'paths',
        nargs='+',
        help='list of input record files, space separated'
    )
    parser.add_argument(
        '--version',
        action='version',
        version=f'%(prog)s {importlib.metadata.version("adfire")}',
    )

    return parser.parse_args(args)


def main(args: list[str] | None = None):
    args = parse_args(args)
    adfire = Adfire(*args.paths)
    adfire.format()


if __name__ == '__main__':
    main()
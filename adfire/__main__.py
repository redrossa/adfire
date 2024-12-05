import argparse
import importlib

from adfire.portfolio import Portfolio


def main():
    parser = argparse.ArgumentParser(description='Adfire CLI')
    parser.add_argument(
        'mode',
        help='command modes',
        choices=['init', 'lint', 'format']
    )
    parser.add_argument(
        'path',
        help='portfolio path, default to current directory',
        default='.',
        nargs='?'
    )
    parser.add_argument(
        '-v', '--version',
        action='version',
        version=f'%(prog)s {importlib.metadata.version("adfire")}'
    )

    args = parser.parse_args()

    portfolio = Portfolio(args.path)
    if args.mode == 'init':
        portfolio.create()
    if args.mode == 'lint':
        portfolio.lint()
    elif args.mode == 'format':
        portfolio.format()


if __name__ == '__main__':
    main()
import argparse
import importlib

from adfire.portfolio import Portfolio


def main():
    parser = argparse.ArgumentParser(description='Adfire CLI')
    parser.add_argument(
        'mode',
        help='command modes',
        choices=['init', 'lint', 'format', 'view']
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

    portfolio = Portfolio.from_new(args.path) if args.mode == 'init' else Portfolio(args.path)
    if args.mode == 'lint':
        portfolio.lint()
    elif args.mode == 'format':
        portfolio.format()
    elif args.mode == 'view':
        portfolio.view()


if __name__ == '__main__':
    main()
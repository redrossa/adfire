import argparse
import importlib
import sys

from adfire.portfolio import Portfolio


def main():
    parser = argparse.ArgumentParser(description='Adfire CLI')
    parser.add_argument(
        'mode',
        help='command modes',
        choices=['init', 'lint', 'format', 'view'])
    if 'view' in sys.argv:
        parser.add_argument(
            'module',
            help='view module')
        parser.add_argument('args', nargs='*')
    parser.add_argument(
        '-p', '--path',
        help='portfolio path, default to current directory',
        default='.',
        nargs='?')
    parser.add_argument(
        '-f', '--force',
        help='force format of hashed entries',
        action=argparse.BooleanOptionalAction)
    parser.add_argument(
        '-v', '--version',
        action='version',
        version=f'%(prog)s {importlib.metadata.version("adfire")}')

    args = parser.parse_args()

    portfolio = Portfolio.from_new(args.path) if args.mode == 'init' else Portfolio(args.path)
    portfolio.forced_hash = args.force
    if args.mode == 'lint':
        portfolio.lint()
    elif args.mode == 'format':
        portfolio.format()
    elif args.mode == 'view':
        try:
            module_name = f'adfire.{args.module}'  # try with default modules first
            portfolio.view(module_name, *args.args)
        except ImportError:
            portfolio.view(args.module, *args.args)


if __name__ == '__main__':
    main()

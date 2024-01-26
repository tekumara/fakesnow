import argparse
import runpy
import sys
from collections.abc import Sequence

import fakesnow

USAGE = "Usage: fakesnow <path> | -m <module> [<arg>]..."


def arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="""eg: fakesnow script.py OR fakesnow -m pytest""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--db_path",
        help="databases path. Use existing database files from this path or create them here if they don't already "
        "exist. If None databases are in-memory.",
    )
    parser.add_argument("-m", "--module", help="module")
    parser.add_argument("path", type=str, nargs="?", help="path")
    parser.add_argument("args", nargs="*", help="args")
    return parser


def main(args: Sequence[str] = sys.argv[1:]) -> int:
    parser = arg_parser()
    pargs, remainder = parser.parse_known_args(args)

    with fakesnow.patch(db_path=pargs.db_path):
        if module := pargs.module:
            if pargs.path:
                sys.argv = [module, pargs.path, *pargs.args, *remainder]
            else:
                sys.argv = [module]

            # add current directory to path to mimic python -m
            sys.path.insert(0, "")
            runpy.run_module(module, run_name="__main__", alter_sys=True)
        elif path := pargs.path:
            sys.argv = [path, *pargs.args, *remainder]

            runpy.run_path(path, run_name="__main__")
        else:
            parser.print_usage()
            return 42

    return 0

import argparse
import runpy
import signal
import sys
import threading
from collections.abc import Sequence

import fakesnow


def arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="""eg: fakesnow script.py OR fakesnow -m pytest OR fakesnow -s""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-d",
        "--db_path",
        help="databases path. Use existing database files from this path or create them here if they don't already "
        "exist. If None databases are in-memory.",
    )
    parser.add_argument("-m", "--module", help="target module")
    parser.add_argument("-s", "--server", action="store_true", help="Run as HTTP server")
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        # suppress default so we can customise how it appears in the help message
        default=argparse.SUPPRESS,
        help="Port to run the HTTP server on (default: random available port)",
    )
    parser.add_argument("path", type=str, nargs="?", help="target path")
    parser.add_argument("targs", nargs="*", help="target args")
    return parser


def split(args: Sequence[str]) -> tuple[Sequence[str], Sequence[str]]:
    # split the arguments into two lists either:
    # 1) after the first -m flag, or
    # 2) after the first positional arg
    in_flag = False
    i = 0
    for i in range(len(args)):
        a = args[i]
        if a in ["-m", "--module"]:
            i = min(i + 1, len(args) - 1)
            break
        elif a.startswith("-"):
            in_flag = True
        elif not in_flag:
            break
        else:
            in_flag = False

    return args[: i + 1], args[i + 1 :]


def main(args: Sequence[str] = sys.argv[1:]) -> int:
    parser = arg_parser()
    # split args so the fakesnow cli doesn't consume from the target's args (eg: -m and -d)
    fsargs, targs = split(args)
    pargs = parser.parse_args(fsargs)

    if pargs.server:
        stop = threading.Event()

        def signal_handler(_signum: int, _frame: object) -> None:
            stop.set()

        signal.signal(signal.SIGINT, signal_handler)

        try:
            with fakesnow.server(port=getattr(pargs, "port", None)):
                # wait for SIGINT
                stop.wait()
        except RuntimeError:
            # catch to avoid showing traceback since uvicorn will log the error
            return 1

        return 0

    with fakesnow.patch(db_path=pargs.db_path):
        if module := pargs.module:
            # NB: pargs.path and pargs.args are consumed by targs
            sys.argv = [module, *targs]

            # add current directory to path to mimic python -m
            sys.path.insert(0, "")
            runpy.run_module(module, run_name="__main__", alter_sys=True)
        elif path := pargs.path:
            # NB: pargs.args is consumed by targs
            sys.argv = [path, *targs]

            runpy.run_path(path, run_name="__main__")
        else:
            parser.print_usage()
            return 42

    return 0

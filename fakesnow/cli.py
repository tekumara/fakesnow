import runpy
import sys
from collections.abc import Sequence

import fakesnow

USAGE = "Usage: fakesnow <path> | -m <module> [<arg>]..."


def main(args: Sequence[str] = sys.argv) -> int:
    if len(args) < 2 or (len(args) == 2 and args[1] == "-m"):
        print(USAGE, file=sys.stderr)
        return 42

    with fakesnow.patch():
        if args[1] == "-m":
            module = args[2]
            sys.argv = args[2:]

            # add current directory to path to mimic python -m
            sys.path.insert(0, "")
            runpy.run_module(module, run_name="__main__", alter_sys=True)
        else:
            path = args[1]
            sys.argv = args[1:]
            runpy.run_path(path, run_name="__main__")

    return 0

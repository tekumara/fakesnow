import sys
from typing import List


def main(args: List[str] = sys.argv[1:]) -> None:
    print(f"hello {args[0]}!")


if __name__ == "__main__":
    main()

from __future__ import annotations

import os
import sys
from collections.abc import Sequence
from typing import Any


def log_sql(sql: str, params: Sequence[Any] | dict[Any, Any] | None = None) -> None:
    if (fs_debug := os.environ.get("FAKESNOW_DEBUG")) and fs_debug != "snowflake":
        print(f"{sql};{params=}" if params else f"{sql};", file=sys.stderr)

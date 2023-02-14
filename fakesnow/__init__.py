from __future__ import annotations

import builtins
from dataclasses import dataclass
import types
from contextlib import contextmanager
from typing import (
    Callable,
    Iterator,
    Mapping,
    Sequence,
)
from unittest.mock import create_autospec, patch

import duckdb
import snowflake.connector
import snowflake.connector.pandas_tools
from duckdb import DuckDBPyConnection

import fakesnow.fakes as fakes


class FakeFunctions:
    def __init__(
        self,
        connect: Callable[..., fakes.FakeSnowflakeConnection],
        write_pandas: Callable[..., fakes.WritePandasResult],
    ):
        self.connect = connect
        self.write_pandas = write_pandas


@contextmanager
def mock() -> Iterator[FakeFunctions]:
    """Patch imports with fake functions.

    Yields:
        Iterator[FakeFunctions]: Useful if you need to manually patch things.
    """
    duck_conn = duckdb.connect(database=":memory:")

    fake_fns = FakeFunctions(
        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting, see https://duckdb.org/docs/api/python/overview.html#startup--shutdown
        connect=lambda **kwargs: fakes.FakeSnowflakeConnection(duck_conn.cursor(), **kwargs),
        write_pandas=fakes.write_pandas,
    )

    with patch("snowflake.connector.connect", side_effect=fake_fns.connect), patch(
        "snowflake.connector.pandas_tools.write_pandas", side_effect=fake_fns.write_pandas
    ):

        yield fake_fns

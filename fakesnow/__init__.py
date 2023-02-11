from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import create_autospec, patch

import duckdb
import snowflake.connector
import snowflake.connector.pandas_tools
from duckdb import DuckDBPyConnection

import fakesnow.fakes as fakes


@contextmanager
def mock() -> Iterator[None]:
    with patch.object(snowflake.connector, "connect") as connect, patch.object(
        snowflake.connector.pandas_tools, "write_pandas"
    ) as write_pandas:
        duck_conn = duckdb.connect(database=":memory:")

        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting, see https://duckdb.org/docs/api/python/overview.html#startup--shutdown
        connect.side_effect = lambda **kwargs: fakes.FakeSnowflakeConnection(duck_conn.cursor(), **kwargs)

        write_pandas.side_effect = fakes.write_pandas

        yield

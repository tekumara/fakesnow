from __future__ import annotations
from typing import Any, Sequence
from typing_extensions import Self
from unittest.mock import MagicMock

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector import SnowflakeConnection
import duckdb
from duckdb import DuckDBPyConnection


class FakeSnowflakeCursor:
    def __init__(
        self,
        connection: DuckDBPyConnection,
        use_dict_result: bool = False,
    ) -> None:
        self._connection = connection
        super().__init__()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(
        self, command: str, params: Sequence[Any] | dict[Any, Any] | None = None, *args, **kwargs
    ) -> FakeSnowflakeCursor:
        print(command)
        self._connection.execute(command)
        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        return self._connection.fetchall()


class FakeSnowflakeConnection(SnowflakeConnection):
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback):
        pass

    def connect(self, **kwargs) -> None:
        return

    def cursor(self, cursor_class: type[SnowflakeCursor] = FakeSnowflakeCursor) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(self)

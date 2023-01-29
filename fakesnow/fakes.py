from __future__ import annotations

from types import TracebackType
from typing import Any, Optional, Sequence, Type

import duckdb
import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import SnowflakeCursor
from sqlglot import parse_one
from typing_extensions import Self

import fakesnow.transforms as transforms


class FakeSnowflakeCursor:
    def __init__(
        self,
        _duck_conn: DuckDBPyConnection,
        use_dict_result: bool = False,
    ) -> None:
        self.duck_conn = _duck_conn

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = ...,
        exc_value: Optional[BaseException] = ...,
        traceback: Optional[TracebackType] = ...,
    ) -> bool:
        return False

    def execute(
        self, command: str, params: Sequence[Any] | dict[Any, Any] | None = None, *args: Any, **kwargs: Any
    ) -> FakeSnowflakeCursor:
        parsed = parse_one(command, read="snowflake")
        transformed = transforms.database_as_schema(parsed).sql()

        try:
            self.duck_conn.execute(transformed)
        except duckdb.CatalogException as e:
            raise snowflake.connector.errors.ProgrammingError(e.args[0]) from e

        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        return self.duck_conn.fetchall()


class FakeSnowflakeConnection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        if schema:
            self._schema = f"{database}_{schema}" if database else schema
            duck_conn.execute(f"set schema = '{self._schema}'")

        self._duck_conn = duck_conn

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = ...,
        exc_value: Optional[BaseException] = ...,
        traceback: Optional[TracebackType] = ...,
    ) -> bool:
        return False

    def cursor(self, cursor_class: type[SnowflakeCursor] = FakeSnowflakeCursor) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(_duck_conn=self._duck_conn)

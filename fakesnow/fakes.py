from __future__ import annotations

from types import TracebackType
from typing import Any, Optional, Sequence, Type
from sqlglot import parse_one, exp

import duckdb
import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor
from typing_extensions import Self
from sqlglot.dialects.snowflake import Snowflake
import fakesnow.transforms as transforms


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
        transformed = transforms.database_prefix(parsed).sql()

        try:
            self._connection.execute(transformed)
        except duckdb.CatalogException as e:
            raise snowflake.connector.errors.ProgrammingError(e.args[0]) from e

        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        return self._connection.fetchall()


class FakeSnowflakeConnection(SnowflakeConnection):
    def __init__(self, *args: Any, **kwargs: Any):
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]] = ...,
        exc_value: Optional[BaseException] = ...,
        traceback: Optional[TracebackType] = ...,
    ) -> bool:
        return False

    def connect(self, **kwargs: Any) -> None:
        return

    def cursor(self, cursor_class: type[SnowflakeCursor] = FakeSnowflakeCursor) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(self)

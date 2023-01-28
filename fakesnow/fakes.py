from __future__ import annotations
from typing import Any, Sequence
from typing_extensions import Self

from snowflake.connector.cursor import SnowflakeCursor
from snowflake.connector import SnowflakeConnection
import duckdb

class FakeSnowflakeCursor(SnowflakeCursor):

    def __init__(
        self,
        connection: SnowflakeConnection,
        use_dict_result: bool = False,
    ) -> None:
        self._connection = connection

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback):
        pass

    def execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args
    ):
        pass

    def fetchall(self) -> list[tuple] | list[dict]:
        return []

    # def __getattr__(self, attr):
    #     raise NotImplementedError(f"{attr} not implemented on {self.__class__}")

class FakeSnowflakeConnection(SnowflakeConnection):

    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, type, value, traceback):
        pass

    def connect(self, **kwargs) -> None:
        return

    def cursor(
        self, cursor_class: type[SnowflakeCursor] = FakeSnowflakeCursor
    ) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(self)

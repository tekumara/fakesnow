from __future__ import annotations

from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Optional, Sequence, Type, Union, cast

import duckdb
import pyarrow.lib

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from snowflake.connector.result_batch import ResultBatch
from sqlglot import parse_one
from typing_extensions import Self

import fakesnow.transforms as transforms

if TYPE_CHECKING:
    import pandas

class FakeSnowflakeCursor:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        use_dict_result: bool = False,
    ) -> None:
        """Create a fake snowflake cursor backed by DuckDB.

        Args:
            duck_conn (DuckDBPyConnection): DuckDB connection.
            use_dict_result (bool, optional): If true rows are returned as dicts otherwise they
                are returned as tuples. Defaults to False.
        """
        self._duck_conn = duck_conn
        self._use_dict_result = use_dict_result

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
        expression = parse_one(command, read="snowflake")

        for t in [transforms.database_as_schema, transforms.set_schema]:
            expression = t(expression)

        transformed = expression.sql()

        try:
            self._duck_conn.execute(transformed)
        except duckdb.CatalogException as e:
            raise snowflake.connector.errors.ProgrammingError(e.args[0]) from e

        self._arrow_table = None
        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        if self._use_dict_result:
            return self._duck_conn.fetch_arrow_table().to_pylist()
        else:
            return self._duck_conn.fetchall()

    def fetchone(self) -> dict | tuple | None:
        if not self._use_dict_result:
            return cast(Union[tuple,None], self._duck_conn.fetchone())

        if not self._arrow_table:
            self._arrow_table = self._duck_conn.fetch_arrow_table()
            self._arrow_table_fetch_one_index = -1

        self._arrow_table_fetch_one_index += 1

        try:
            return self._arrow_table.take([self._arrow_table_fetch_one_index]).to_pylist()
        except pyarrow.lib.ArrowIndexError:
            return None

    def get_result_batches(self) -> list[ResultBatch] | None:
        # chunk_size is multiple of 1024
        # see https://github.com/duckdb/duckdb/issues/4755
        reader = self._duck_conn.fetch_record_batch(chunk_size=1024)

        batches = []
        while True:
            try:
                batches.append(DuckResultBatch(self._use_dict_result, reader.read_next_batch()))
            except StopIteration:
                break

        return batches


class DuckResultBatch(ResultBatch):

    def __init__(self, use_dict_result: bool, batch: pyarrow.RecordBatch):
        self._use_dict_result = use_dict_result
        self._batch = batch

    def create_iter(
        self, **kwargs
    ) -> (
        Iterator[dict | Exception]
        | Iterator[tuple | Exception]
        | Iterator[pyarrow.Table]
        | Iterator[pandas.DataFrame]
    ):
        if self._use_dict_result:
            return iter(self._batch.to_pylist())

        return iter(tuple(d.values()) for d in self._batch.to_pylist())

    def to_pandas(self) -> pandas.DataFrame:
        raise NotImplementedError()

    def to_arrow(self) -> pyarrow.Table:
        raise NotImplementedError()

class FakeSnowflakeConnection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        # TODO handle if database only supplied
        if schema:
            self._schema = f"{database}_{schema}" if database else schema
            duck_conn.execute(f"set schema = '{self._schema}'")

        self._duck_conn = duck_conn

    def __enter__(self) -> Self:
        return self

    def __exit__(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        exc_type: Optional[Type[BaseException]] = ...,
        exc_value: Optional[BaseException] = ...,
        traceback: Optional[TracebackType] = ...,
    ) -> bool:
        return False

    def cursor(  # pyright: ignore[reportIncompatibleMethodOverride]
        self, cursor_class: Type[SnowflakeCursor] = SnowflakeCursor
    ) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(duck_conn=self._duck_conn, use_dict_result=cursor_class == DictCursor)

    def execute_string(  # pyright: ignore[reportIncompatibleMethodOverride]
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: Type[SnowflakeCursor] = SnowflakeCursor,
        **kwargs: dict[str, Any],
    ) -> Iterable[FakeSnowflakeCursor]:
        cursors = [self.cursor(cursor_class).execute(e.sql()) for e in sqlglot.parse(sql_text, read="snowflake") if e]
        return cursors if return_cursors else []


from __future__ import annotations

import re
from types import TracebackType
from typing import TYPE_CHECKING, Any, Iterable, Iterator, Literal, Optional, Sequence, Type, Union, cast

import duckdb
import pyarrow
import pyarrow.lib
import pyarrow.types
import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import DictCursor, ResultMetadata, SnowflakeCursor
from snowflake.connector.result_batch import ResultBatch
from sqlglot import exp, parse_one
from typing_extensions import Self

import fakesnow.expr as expr
import fakesnow.transforms as transforms

if TYPE_CHECKING:
    import pandas as pd

SCHEMA_UNSET = "schema_unset"


class FakeSnowflakeCursor:
    def __init__(
        self,
        conn: FakeSnowflakeConnection,
        duck_conn: DuckDBPyConnection,
        use_dict_result: bool = False,
    ) -> None:
        """Create a fake snowflake cursor backed by DuckDB.

        Args:
            conn (FakeSnowflakeConnection): Used to maintain current database and schema.
            duck_conn (DuckDBPyConnection): DuckDB connection.
            use_dict_result (bool, optional): If true rows are returned as dicts otherwise they
                are returned as tuples. Defaults to False.
        """
        self._conn = conn
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

    def describe(self, command: str, *args: Any, **kwargs: Any) -> list[ResultMetadata]:
        """Return the schema of the result without executing the query.

        Takes the same arguments as execute

        Returns:
            list[ResultMetadata]: _description_
        """

        # fmt: off
        def as_result_metadata(column_name: str, column_type: str, _: str) -> ResultMetadata:
            # see https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes
            # and https://arrow.apache.org/docs/python/api/datatypes.html#type-checking
            # type ignore because of https://github.com/snowflakedb/snowflake-connector-python/issues/1423
            if column_type == "INTEGER":
                return ResultMetadata(
                    name=column_name, type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True                    # type: ignore # noqa: E501
                )
            elif column_type.startswith("DECIMAL"):
                match = re.search(r'\((\d+),(\d+)\)', column_type)
                if match:
                    precision = int(match[1])
                    scale = int(match[2])
                else:
                    precision = scale = None
                return ResultMetadata(
                    name=column_name, type_code=0, display_size=None, internal_size=None, precision=precision, scale=scale, is_nullable=True # type: ignore # noqa: E501
                )
            elif column_type == "VARCHAR":
                return ResultMetadata(
                    name=column_name, type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True   # type: ignore # noqa: E501
                )
            elif column_type == "FLOAT":
                return ResultMetadata(
                    name=column_name, type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True       # type: ignore # noqa: E501
                )
            elif column_type == "TIMESTAMP":
                return ResultMetadata(
                    name=column_name, type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True             # type: ignore # noqa: E501
                )
            else:
                # TODO handle more types
                raise NotImplementedError(f"for column type {column_type}")

        # fmt: on

        describe = transforms.as_describe(parse_one(command, read="snowflake"))
        self.execute(describe, *args, **kwargs)

        meta = [
            as_result_metadata(column_name, column_type, null)
            for (column_name, column_type, null, _, _, _) in self._duck_conn.fetchall()
        ]

        return meta

    def execute(
        self,
        command: str | exp.Expression,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        self._arrow_table = None

        expression = command if isinstance(command, exp.Expression) else parse_one(command, read="snowflake")
        cmd = expr.key_command(expression)

        if cmd == "USE DATABASE" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            self._conn.database = ident.this
            return self

        if cmd == "USE SCHEMA" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            if not self._conn.database:
                raise snowflake.connector.errors.ProgrammingError(
                    msg="SQL compilation error:\nObject does not exist, or operation cannot be performed.",
                    errno=2043,
                    sqlstate="02000",
                )
            self._conn.schema_set = True

        expression = transforms.database_prefix(
            expression, current_database=self._conn.database, schema_set=self._conn.schema_set
        )

        # TODO: move into use schema block
        expression = transforms.set_schema(expression)

        transformed = expression.sql()

        if "unqualified_and_no" in transformed:
            if not self._conn.database:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"Cannot perform {cmd}. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",  # noqa: E501
                    errno=90105,
                    sqlstate="22000",
                ) from None
            elif not self._conn.schema_set:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"Cannot perform {cmd}. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name.",  # noqa: E501
                    errno=90106,
                    sqlstate="22000",
                ) from None
            else:
                raise AssertionError(f"unqualified_and_no_current but {self._conn.database=} {self._conn.schema_set=}")

        try:
            self._duck_conn.execute(transformed)
        except duckdb.CatalogException as e:
            # minimal processing to make it look like a snowflake exception, message content may differ
            msg = cast(str, e.args[0]).split("\n")[0]
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=2003, sqlstate="42S02") from None

        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        if self._use_dict_result:
            return self._duck_conn.fetch_arrow_table().to_pylist()
        else:
            return self._duck_conn.fetchall()

    def fetchone(self) -> dict | tuple | None:
        if not self._use_dict_result:
            return cast(Union[tuple, None], self._duck_conn.fetchone())

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


class FakeSnowflakeConnection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        database: Optional[str] = None,
        schema: Optional[str] = None,
        *args: Any,
        **kwargs: Any,
    ):
        self.database = database
        self.schema = schema
        self.schema_set = False

        if database and schema:
            # TODO: use . like snowflake does
            transformed_schema = f"{database}_{schema}"

            # check schema exists
            if duck_conn.execute(
                f"select * from duckdb_schemas() where schema_name = '{transformed_schema}'"
            ).fetchone():
                duck_conn.execute(f"set schema = '{transformed_schema}'")
                self.schema_set = True

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

    def cursor(self, cursor_class: Type[SnowflakeCursor] = SnowflakeCursor) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(conn=self, duck_conn=self._duck_conn, use_dict_result=cursor_class == DictCursor)

    def execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: Type[SnowflakeCursor] = SnowflakeCursor,
        **kwargs: dict[str, Any],
    ) -> Iterable[FakeSnowflakeCursor]:
        cursors = [self.cursor(cursor_class).execute(e.sql()) for e in sqlglot.parse(sql_text, read="snowflake") if e]
        return cursors if return_cursors else []

    def insert_df(
        self, df: pd.DataFrame, table_name: str, database: str | None = None, schema: str | None = None
    ) -> int:
        self._duck_conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")
        return self._duck_conn.fetchall()[0][0]


class DuckResultBatch(ResultBatch):
    def __init__(self, use_dict_result: bool, batch: pyarrow.RecordBatch):
        self._use_dict_result = use_dict_result
        self._batch = batch

    def create_iter(
        self, **kwargs: dict[str, Any]
    ) -> (Iterator[dict | Exception] | Iterator[tuple | Exception] | Iterator[pyarrow.Table] | Iterator[pd.DataFrame]):
        if self._use_dict_result:
            return iter(self._batch.to_pylist())

        return iter(tuple(d.values()) for d in self._batch.to_pylist())

    @property
    def rowcount(self) -> int:
        return self._batch.num_rows

    def to_pandas(self) -> pd.DataFrame:
        raise NotImplementedError()

    def to_arrow(self) -> pyarrow.Table:
        raise NotImplementedError()


CopyResult = tuple[
    str,
    str,
    int,
    int,
    int,
    int,
    Optional[str],
    Optional[int],
    Optional[int],
    Optional[str],
]

WritePandasResult = tuple[
    bool,
    int,
    int,
    Sequence[CopyResult],
]


def write_pandas(
    conn: FakeSnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
    database: str | None = None,
    schema: str | None = None,
    chunk_size: int | None = None,
    compression: str = "gzip",
    on_error: str = "abort_statement",
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    create_temp_table: bool = False,
    overwrite: bool = False,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
    **kwargs: Any,
) -> WritePandasResult:
    count = conn.insert_df(df, table_name, database, schema)

    # mocks https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
    mock_copy_results = [("fakesnow/file0.txt", "LOADED", count, count, 1, 0, None, None, None, None)]

    # return success
    return (True, len(mock_copy_results), count, mock_copy_results)

from __future__ import annotations

import json
import os
import re
import sys
from collections.abc import Iterable, Iterator, Sequence
from pathlib import Path
from string import Template
from types import TracebackType
from typing import TYPE_CHECKING, Any, Literal, Optional, cast

import duckdb

if TYPE_CHECKING:
    import pandas as pd
    import pyarrow.lib
import pyarrow
import snowflake.connector.converter
import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import DictCursor, ResultMetadata, SnowflakeCursor
from snowflake.connector.result_batch import ResultBatch
from sqlglot import exp, parse_one
from typing_extensions import Self

import fakesnow.checks as checks
import fakesnow.expr as expr
import fakesnow.info_schema as info_schema
import fakesnow.macros as macros
import fakesnow.transforms as transforms
from fakesnow.global_database import create_global_database

SCHEMA_UNSET = "schema_unset"
SQL_SUCCESS = "SELECT 'Statement executed successfully.' as 'status'"
SQL_CREATED_DATABASE = Template("SELECT 'Database ${name} successfully created.' as 'status'")
SQL_CREATED_SCHEMA = Template("SELECT 'Schema ${name} successfully created.' as 'status'")
SQL_CREATED_TABLE = Template("SELECT 'Table ${name} successfully created.' as 'status'")
SQL_DROPPED = Template("SELECT '${name} successfully dropped.' as 'status'")
SQL_INSERTED_ROWS = Template("SELECT ${count} as 'number of rows inserted'")
SQL_UPDATED_ROWS = Template("SELECT ${count} as 'number of rows updated', 0 as 'number of multi-joined rows updated'")
SQL_DELETED_ROWS = Template("SELECT ${count} as 'number of rows deleted'")


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
        self._last_sql = None
        self._last_params = None
        self._sqlstate = None
        self._arraysize = 1
        self._arrow_table = None
        self._arrow_table_fetch_index = None
        self._rowcount = None
        self._converter = snowflake.connector.converter.SnowflakeConverter()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    @property
    def arraysize(self) -> int:
        return self._arraysize

    @arraysize.setter
    def arraysize(self, value: int) -> None:
        self._arraysize = value

    def close(self) -> bool:
        self._last_sql = None
        self._last_params = None
        return True

    def describe(self, command: str, *args: Any, **kwargs: Any) -> list[ResultMetadata]:
        """Return the schema of the result without executing the query.

        Takes the same arguments as execute

        Returns:
            list[ResultMetadata]: _description_
        """

        describe = f"DESCRIBE {command}"
        self.execute(describe, *args, **kwargs)
        return FakeSnowflakeCursor._describe_as_result_metadata(self.fetchall())

    @property
    def description(self) -> list[ResultMetadata]:
        # use a separate cursor to avoid consuming the result set on this cursor
        with self._conn.cursor() as cur:
            cur.execute(f"DESCRIBE {self._last_sql}", self._last_params)
            meta = FakeSnowflakeCursor._describe_as_result_metadata(cur.fetchall())

        return meta

    def execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        try:
            self._sqlstate = None
            return self._execute(command, params, *args, **kwargs)
        except snowflake.connector.errors.ProgrammingError as e:
            self._sqlstate = e.sqlstate
            raise e

    def _execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        self._arrow_table = None
        self._arrow_table_fetch_index = None
        self._rowcount = None

        command, params = self._rewrite_with_params(command, params)
        expression = parse_one(command, read="snowflake")

        cmd = expr.key_command(expression)

        no_database, no_schema = checks.is_unqualified_table_expression(expression)

        if no_database and not self._conn.database_set:
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"Cannot perform {cmd}. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",  # noqa: E501
                errno=90105,
                sqlstate="22000",
            )
        elif no_schema and not self._conn.schema_set:
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"Cannot perform {cmd}. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name.",  # noqa: E501
                errno=90106,
                sqlstate="22000",
            )

        transformed = (
            expression.transform(transforms.upper_case_unquoted_identifiers)
            .transform(transforms.set_schema, current_database=self._conn.database)
            .transform(transforms.create_database, db_path=self._conn.db_path)
            .transform(transforms.extract_comment_on_table)
            .transform(transforms.extract_comment_on_columns)
            .transform(transforms.information_schema_fs_columns_snowflake)
            .transform(transforms.information_schema_fs_tables_ext)
            .transform(transforms.drop_schema_cascade)
            .transform(transforms.tag)
            .transform(transforms.semi_structured_types)
            .transform(transforms.parse_json)
            # indices_to_json_extract must be before regex_substr
            .transform(transforms.indices_to_json_extract)
            .transform(transforms.json_extract_cast_as_varchar)
            .transform(transforms.json_extract_cased_as_varchar)
            .transform(transforms.flatten)
            .transform(transforms.regex_replace)
            .transform(transforms.regex_substr)
            .transform(transforms.values_columns)
            .transform(transforms.to_date)
            .transform(transforms.to_decimal)
            .transform(transforms.to_timestamp_ntz)
            .transform(transforms.to_timestamp)
            .transform(transforms.object_construct)
            .transform(transforms.timestamp_ntz_ns)
            .transform(transforms.float_to_double)
            .transform(transforms.integer_precision)
            .transform(transforms.extract_text_length)
            .transform(transforms.sample)
            .transform(transforms.array_size)
            .transform(transforms.random)
            .transform(transforms.identifier)
            .transform(lambda e: transforms.show_schemas(e, self._conn.database))
            .transform(lambda e: transforms.show_objects_tables(e, self._conn.database))
            .transform(transforms.show_users)
            .transform(transforms.create_user)
        )
        sql = transformed.sql(dialect="duckdb")
        result_sql = None

        if transformed.find(exp.Select) and (seed := transformed.args.get("seed")):
            sql = f"SELECT setseed({seed}); {sql}"

        if fs_debug := os.environ.get("FAKESNOW_DEBUG"):
            debug = command if fs_debug == "snowflake" else sql
            print(f"{debug};{params=}" if params else f"{debug};", file=sys.stderr)

        try:
            self._duck_conn.execute(sql, params)
        except duckdb.BinderException as e:
            msg = e.args[0]
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=2043, sqlstate="02000") from None
        except duckdb.CatalogException as e:
            # minimal processing to make it look like a snowflake exception, message content may differ
            msg = cast(str, e.args[0]).split("\n")[0]
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=2003, sqlstate="42S02") from None
        except duckdb.TransactionException as e:
            if "cannot rollback - no transaction is active" in str(
                e
            ) or "cannot commit - no transaction is active" in str(e):
                # snowflake doesn't error on rollback or commit outside a tx
                result_sql = SQL_SUCCESS
            else:
                raise e
        except duckdb.ConnectionException as e:
            raise snowflake.connector.errors.DatabaseError(msg=e.args[0], errno=250002, sqlstate="08003") from None

        affected_count = None
        if cmd == "USE DATABASE" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            self._conn.database = ident.this.upper()
            self._conn.database_set = True

        elif cmd == "USE SCHEMA" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            self._conn.schema = ident.this.upper()
            self._conn.schema_set = True

        elif create_db_name := transformed.args.get("create_db_name"):
            # we created a new database, so create the info schema extensions
            self._duck_conn.execute(info_schema.creation_sql(create_db_name))
            result_sql = SQL_CREATED_DATABASE.substitute(name=create_db_name)

        elif cmd == "CREATE SCHEMA" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            name = ident.this if ident.quoted else ident.this.upper()
            result_sql = SQL_CREATED_SCHEMA.substitute(name=name)

        elif cmd == "CREATE TABLE" and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            name = ident.this if ident.quoted else ident.this.upper()
            result_sql = SQL_CREATED_TABLE.substitute(name=name)

        elif cmd.startswith("DROP") and (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            name = ident.this if ident.quoted else ident.this.upper()
            result_sql = SQL_DROPPED.substitute(name=name)

            # if dropping the current database/schema then reset conn metadata
            if cmd == "DROP DATABASE" and name == self._conn.database:
                self._conn.database = None
                self._conn.schema = None

            elif cmd == "DROP SCHEMA" and name == self._conn.schema:
                self._conn.schema = None

        elif cmd == "INSERT":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_INSERTED_ROWS.substitute(count=affected_count)

        elif cmd == "UPDATE":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_UPDATED_ROWS.substitute(count=affected_count)

        elif cmd == "DELETE":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_DELETED_ROWS.substitute(count=affected_count)

        elif cmd == "DESCRIBE TABLE":
            # DESCRIBE TABLE has already been run above to detect and error if the table exists
            # We now rerun DESCRIBE TABLE but transformed with columns to match Snowflake
            result_sql = transformed.transform(
                lambda e: transforms.describe_table(e, self._conn.database, self._conn.schema)
            ).sql(dialect="duckdb")

        if table_comment := cast(tuple[exp.Table, str], transformed.args.get("table_comment")):
            # record table comment
            table, comment = table_comment
            catalog = table.catalog or self._conn.database
            schema = table.db or self._conn.schema
            assert catalog and schema
            self._duck_conn.execute(info_schema.insert_table_comment_sql(catalog, schema, table.name, comment))

        if (text_lengths := cast(list[tuple[str, int]], transformed.args.get("text_lengths"))) and (
            table := transformed.find(exp.Table)
        ):
            # record text lengths
            catalog = table.catalog or self._conn.database
            schema = table.db or self._conn.schema
            assert catalog and schema
            self._duck_conn.execute(info_schema.insert_text_lengths_sql(catalog, schema, table.name, text_lengths))

        if result_sql:
            self._duck_conn.execute(result_sql)

        self._arrow_table = self._duck_conn.fetch_arrow_table()
        self._rowcount = affected_count or self._arrow_table.num_rows

        self._last_sql = result_sql or sql
        self._last_params = params

        return self

    def executemany(
        self,
        command: str,
        seqparams: Sequence[Any] | dict[str, Any],
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        if isinstance(seqparams, dict):
            # see https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
            raise NotImplementedError("dict params not supported yet")

        # TODO: support insert optimisations
        # the snowflake connector will optimise inserts into a single query
        # unless num_statements != 1 .. but for simplicity we execute each
        # query one by one, which means the response differs
        for p in seqparams:
            self.execute(command, p)

        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        if self._arrow_table is None:
            # mimic snowflake python connector error type
            raise TypeError("No open result set")
        return self.fetchmany(self._arrow_table.num_rows)

    def fetch_pandas_all(self, **kwargs: dict[str, Any]) -> pd.DataFrame:
        if self._arrow_table is None:
            # mimic snowflake python connector error type
            raise snowflake.connector.NotSupportedError("No open result set")
        return self._arrow_table.to_pandas()

    def fetchone(self) -> dict | tuple | None:
        result = self.fetchmany(1)
        return result[0] if result else None

    def fetchmany(self, size: int | None = None) -> list[tuple] | list[dict]:
        # https://peps.python.org/pep-0249/#fetchmany
        size = size or self._arraysize

        if self._arrow_table is None:
            # mimic snowflake python connector error type
            raise TypeError("No open result set")
        if self._arrow_table_fetch_index is None:
            self._arrow_table_fetch_index = 0
        else:
            self._arrow_table_fetch_index += size

        tslice = self._arrow_table.slice(offset=self._arrow_table_fetch_index, length=size).to_pylist()
        return tslice if self._use_dict_result else [tuple(d.values()) for d in tslice]

    def get_result_batches(self) -> list[ResultBatch] | None:
        if self._arrow_table is None:
            return None
        return [FakeResultBatch(self._use_dict_result, b) for b in self._arrow_table.to_batches(max_chunksize=1000)]

    @property
    def rowcount(self) -> int | None:
        return self._rowcount

    @property
    def sfqid(self) -> str | None:
        return "fakesnow"

    @property
    def sqlstate(self) -> str | None:
        return self._sqlstate

    @staticmethod
    def _describe_as_result_metadata(describe_results: list) -> list[ResultMetadata]:
        # fmt: off
        def as_result_metadata(column_name: str, column_type: str, _: str) -> ResultMetadata:
            # see https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes
            # and https://arrow.apache.org/docs/python/api/datatypes.html#type-checking
            if column_type in {"BIGINT", "INTEGER"}:
                return ResultMetadata(
                    name=column_name, type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True               # noqa: E501
                )
            elif column_type.startswith("DECIMAL"):
                match = re.search(r'\((\d+),(\d+)\)', column_type)
                if match:
                    precision = int(match[1])
                    scale = int(match[2])
                else:
                    precision = scale = None
                return ResultMetadata(
                    name=column_name, type_code=0, display_size=None, internal_size=None, precision=precision, scale=scale, is_nullable=True    # noqa: E501
                )
            elif column_type == "VARCHAR":
                # TODO: fetch internal_size from varchar size
                return ResultMetadata(
                    name=column_name, type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True      # noqa: E501
                )
            elif column_type == "DOUBLE":
                return ResultMetadata(
                    name=column_name, type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True          # noqa: E501
                )
            elif column_type == "BOOLEAN":
                return ResultMetadata(
                    name=column_name, type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True         # noqa: E501
                )
            elif column_type == "DATE":
                return ResultMetadata(
                    name=column_name, type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True          # noqa: E501
                )
            elif column_type in {"TIMESTAMP", "TIMESTAMP_NS"}:
                return ResultMetadata(
                    name=column_name, type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True                # noqa: E501
                )
            elif column_type == "TIMESTAMP WITH TIME ZONE":
                return ResultMetadata(
                    name=column_name, type_code=7, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True                # noqa: E501
                )
            elif column_type == "BLOB":
                return ResultMetadata(
                    name=column_name, type_code=11, display_size=None, internal_size=8388608, precision=None, scale=None, is_nullable=True      # noqa: E501
                )
            elif column_type == "TIME":
                return ResultMetadata(
                    name=column_name, type_code=12, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True               # noqa: E501
                )
            elif column_type == "JSON":
                # TODO: correctly map OBJECT and ARRAY see https://github.com/tekumara/fakesnow/issues/26
                return ResultMetadata(
                    name=column_name, type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True               # noqa: E501
                )
            else:
                # TODO handle more types
                raise NotImplementedError(f"for column type {column_type}")

        # fmt: on

        meta = [
            as_result_metadata(column_name, column_type, null)
            for (column_name, column_type, null, _, _, _) in describe_results
        ]
        return meta

    def _rewrite_with_params(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
    ) -> tuple[str, Sequence[Any] | dict[Any, Any] | None]:
        if params and self._conn._paramstyle in ("pyformat", "format"):  # noqa: SLF001
            # handle client-side in the same manner as the snowflake python connector

            def convert(param: Any) -> Any:  # noqa: ANN401
                return self._converter.quote(self._converter.escape(self._converter.to_snowflake(param)))

            if isinstance(params, dict):
                params = {k: convert(v) for k, v in params.items()}
            else:
                params = tuple(convert(v) for v in params)

            return command % params, None

        return command, params


class FakeSnowflakeConnection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        database: str | None = None,
        schema: str | None = None,
        create_database: bool = True,
        create_schema: bool = True,
        db_path: str | os.PathLike | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        self._duck_conn = duck_conn
        # upper case database and schema like snowflake unquoted identifiers
        # NB: catalog names are not case-sensitive in duckdb but stored as cased in information_schema.schemata
        self.database = database and database.upper()
        self.schema = schema and schema.upper()
        self.database_set = False
        self.schema_set = False
        self.db_path = db_path
        self._paramstyle = snowflake.connector.paramstyle

        create_global_database(duck_conn)

        # create database if needed
        if (
            create_database
            and self.database
            and not duck_conn.execute(
                f"""select * from information_schema.schemata
                where catalog_name = '{self.database}'"""
            ).fetchone()
        ):
            db_file = f"{Path(db_path)/self.database}.db" if db_path else ":memory:"
            duck_conn.execute(f"ATTACH DATABASE '{db_file}' AS {self.database}")
            duck_conn.execute(info_schema.creation_sql(self.database))
            duck_conn.execute(macros.creation_sql(self.database))

        # create schema if needed
        if (
            create_schema
            and self.database
            and self.schema
            and not duck_conn.execute(
                f"""select * from information_schema.schemata
                where catalog_name = '{self.database}' and schema_name = '{self.schema}'"""
            ).fetchone()
        ):
            duck_conn.execute(f"CREATE SCHEMA {self.database}.{self.schema}")

        # set database and schema if both exist
        if (
            self.database
            and self.schema
            and duck_conn.execute(
                f"""select * from information_schema.schemata
                where catalog_name = '{self.database}' and schema_name = '{self.schema}'"""
            ).fetchone()
        ):
            duck_conn.execute(f"SET schema='{self.database}.{self.schema}'")
            self.database_set = True
            self.schema_set = True
        # set database if only that exists
        elif (
            self.database
            and duck_conn.execute(
                f"""select * from information_schema.schemata
                where catalog_name = '{self.database}'"""
            ).fetchone()
        ):
            duck_conn.execute(f"SET schema='{self.database}.main'")
            self.database_set = True

        # use UTC instead of local time zone for consistent testing
        duck_conn.execute("SET GLOBAL TimeZone = 'UTC'")

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        pass

    def close(self, retry: bool = True) -> None:
        self._duck_conn.close()

    def commit(self) -> None:
        self.cursor().execute("COMMIT")

    def cursor(self, cursor_class: type[SnowflakeCursor] = SnowflakeCursor) -> FakeSnowflakeCursor:
        return FakeSnowflakeCursor(conn=self, duck_conn=self._duck_conn, use_dict_result=cursor_class == DictCursor)

    def execute_string(
        self,
        sql_text: str,
        remove_comments: bool = False,
        return_cursors: bool = True,
        cursor_class: type[SnowflakeCursor] = SnowflakeCursor,
        **kwargs: dict[str, Any],
    ) -> Iterable[FakeSnowflakeCursor]:
        cursors = [
            self.cursor(cursor_class).execute(e.sql(dialect="snowflake"))
            for e in sqlglot.parse(sql_text, read="snowflake")
            if e
        ]
        return cursors if return_cursors else []

    def rollback(self) -> None:
        self.cursor().execute("ROLLBACK")

    def _insert_df(
        self, df: pd.DataFrame, table_name: str, database: str | None = None, schema: str | None = None
    ) -> int:
        # Objects in dataframes are written as parquet structs, and snowflake loads parquet structs as json strings.
        # Whereas duckdb analyses a dataframe see https://duckdb.org/docs/api/python/data_ingestion.html#pandas-dataframes--object-columns
        # and converts a object to the most specific type possible, eg: dict -> STRUCT, MAP or varchar, and list -> LIST
        # For dicts see https://github.com/duckdb/duckdb/pull/3985 and https://github.com/duckdb/duckdb/issues/9510
        #
        # When the rows have dicts with different keys there isn't a single STRUCT that can cover them, so the type is
        # varchar and value a string containing a struct representation. In order to support dicts with different keys
        # we first convert the dicts to json strings. A pity we can't do something inside duckdb and avoid the dataframe
        # copy and transform in python.

        df = df.copy()

        # Identify columns of type object
        object_cols = df.select_dtypes(include=["object"]).columns

        # Apply json.dumps to these columns
        for col in object_cols:
            # don't jsonify string
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

        self._duck_conn.execute(f"INSERT INTO {table_name}({','.join(df.columns.to_list())}) SELECT * FROM df")
        return self._duck_conn.fetchall()[0][0]


class FakeResultBatch(ResultBatch):
    def __init__(self, use_dict_result: bool, batch: pyarrow.RecordBatch):
        self._use_dict_result = use_dict_result
        self._batch = batch

    def create_iter(
        self, **kwargs: dict[str, Any]
    ) -> Iterator[dict | Exception] | Iterator[tuple | Exception] | Iterator[pyarrow.Table] | Iterator[pd.DataFrame]:
        if self._use_dict_result:
            return iter(self._batch.to_pylist())

        return iter(tuple(d.values()) for d in self._batch.to_pylist())

    @property
    def rowcount(self) -> int:
        return self._batch.num_rows

    def to_pandas(self) -> pd.DataFrame:
        return self._batch.to_pandas()

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
    count = conn._insert_df(df, table_name, database, schema)  # noqa: SLF001

    # mocks https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
    mock_copy_results = [("fakesnow/file0.txt", "LOADED", count, count, 1, 0, None, None, None, None)]

    # return success
    return (True, len(mock_copy_results), count, mock_copy_results)

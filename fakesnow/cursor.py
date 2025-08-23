from __future__ import annotations

import os
import re
import sys
import uuid
from collections.abc import Callable, Iterator, Sequence
from string import Template
from types import TracebackType
from typing import TYPE_CHECKING, Any, cast

import duckdb
import pyarrow  # needed by fetch_arrow_table()
import snowflake.connector.converter
import snowflake.connector.errors
import sqlglot
import sqlglot.errors
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import ResultMetadata
from snowflake.connector.result_batch import ResultBatch
from sqlglot import exp, parse_one
from typing_extensions import Self

import fakesnow.checks as checks
import fakesnow.expr as expr
import fakesnow.info_schema as info_schema
import fakesnow.transforms as transforms
from fakesnow import logger
from fakesnow.copy_into import copy_into
from fakesnow.params import MutableParams
from fakesnow.rowtype import describe_as_result_metadata
from fakesnow.transforms import stage

if TYPE_CHECKING:
    # don't require pandas at import time
    import pandas as pd
    from snowflake.connector import SnowflakeConnection

    # avoid circular import
    from fakesnow.conn import FakeSnowflakeConnection


SCHEMA_UNSET = "schema_unset"
SQL_SUCCESS = "SELECT 'Statement executed successfully.' as 'status'"
SQL_CREATED_DATABASE = Template("SELECT 'Database ${name} successfully created.' as 'status'")
SQL_CREATED_SEQUENCE = Template("SELECT 'Sequence ${name} successfully created.' as 'status'")
SQL_CREATED_SCHEMA = Template("SELECT 'Schema ${name} successfully created.' as 'status'")
SQL_CREATED_SECRET = Template("SELECT 'Secret ${name} successfully created.' as 'status'")
SQL_CREATED_TABLE = Template("SELECT 'Table ${name} successfully created.' as 'status'")
SQL_CREATED_VIEW = Template("SELECT 'View ${name} successfully created.' as 'status'")
SQL_CREATED_STAGE = Template("SELECT 'Stage area ${name} successfully created.' as status")
SQL_DROPPED = Template("SELECT '${name} successfully dropped.' as 'status'")
SQL_INSERTED_ROWS = Template("SELECT ${count} as 'number of rows inserted'")
SQL_UPDATED_ROWS = Template("SELECT ${count} as 'number of rows updated', 0 as 'number of multi-joined rows updated'")
SQL_DELETED_ROWS = Template("SELECT ${count} as 'number of rows deleted'")
SQL_COPY_ROWS = Template(
    "SELECT '${file}' as file, 'LOADED' as status, ${count} as rows_parsed, "
    "${count} as rows_loaded, 1 as error_limit, 0 as errors_seen, "
    "NULL as first_error, NULL as first_error_line, NULL as first_error_character, "
    "NULL as first_error_column_name"
)


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
        self._last_transformed = None
        self._sqlstate = None
        self._arraysize = 1
        self._arrow_table = None
        self._arrow_table_fetch_index = None
        self._rowcount = None
        self._sfqid = None
        self._converter = snowflake.connector.converter.SnowflakeConverter()
        self._prefetch_hook: Callable[[], None] | None = None

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
        self._last_transformed = None
        return True

    def describe(self, command: str, *args: Any, **kwargs: Any) -> list[ResultMetadata]:
        """Return the schema of the result without executing the query.

        Takes the same arguments as execute

        Returns:
            list[ResultMetadata]: _description_
        """

        describe = f"DESCRIBE {command}"
        self.execute(describe, *args, **kwargs)
        return describe_as_result_metadata(self.fetchall())

    @property
    def description(self) -> list[ResultMetadata]:
        return describe_as_result_metadata(self._describe_last_sql())

    def _describe_last_sql(self) -> list:
        # use a separate cursor to avoid consuming the result set on this cursor
        with self._conn.cursor() as cur:
            # TODO: can we replace with self._duck_conn.description?
            expression = sqlglot.parse_one(f"DESCRIBE {self._last_sql}", read="duckdb")
            cur._execute(expression, self._last_params)  # noqa: SLF001
            return cur.fetchall()

    def execute(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        try:
            self._sqlstate = None

            if os.environ.get("FAKESNOW_DEBUG") == "snowflake":
                p = params or kwargs.get("binding_params")
                print(f"{command};params={p}" if p else f"{command};", file=sys.stderr)

            command = self._inline_variables(command)
            if kwargs.get("binding_params"):
                # params have come via the server
                params = kwargs["binding_params"]
            else:
                command, params = self._rewrite_with_params(command, params)

            # convert tuple to mutable list
            if not isinstance(params, (list, dict)) and params is not None:
                params = list(params)

            if self._conn.nop_regexes and any(re.match(p, command, re.IGNORECASE) for p in self._conn.nop_regexes):
                transformed = transforms.SUCCESS_NOP
                self._execute(transformed, params)
                return self

            expression = parse_one(command, read="snowflake")
            self.check_db_and_schema(expression)

            for exp in self._transform_explode(expression):
                transformed = self._transform(exp, params)
                self._execute(transformed, params)

            if not kwargs.get("server") and (put_stage_data := transformed.args.get("put_stage_data")):  # pyright: ignore[reportPossiblyUnboundVariable]
                self._put_files(put_stage_data)

            return self
        except snowflake.connector.errors.ProgrammingError as e:
            self._sqlstate = e.sqlstate
            raise e
        except sqlglot.errors.ParseError as e:
            self._sqlstate = "42000"
            # strip highlight for better readability, TODO: show pointer to start of error
            msg = str(e).replace("\x1b[4m", "").replace("\x1b[0m", "")
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=1003, sqlstate="42000") from None
        except NotImplementedError as e:
            msg = f"{e} not implemented. Please raise an issue via https://github.com/tekumara/fakesnow/issues/new"
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=9999, sqlstate="99999") from e

    def execute_async(
        self,
        command: str,
        params: Sequence[Any] | dict[Any, Any] | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        return self.execute(command, params, *args, **kwargs)

    def get_results_from_sfqid(self, sfqid: str) -> None:
        def prefetch() -> None:
            value = self._conn.results_cache.get(sfqid)
            if value is None:
                raise snowflake.connector.errors.DatabaseError(
                    "Cannot retrieve data on the status of this query. "
                    "No information returned from server for query '{}'"
                )
            # Restore the cached result data
            self._arrow_table, self._rowcount, self._last_sql, self._last_params, self._last_transformed = value
            self._sfqid = sfqid
            self._arrow_table_fetch_index = None
            self._prefetch_hook = None

        self._prefetch_hook = prefetch

    def _put_files(self, put_stage_data: stage.UploadCommandDict) -> None:
        results = stage.upload_files(put_stage_data)
        _df = pyarrow.Table.from_pylist(results)
        self._duck_conn.execute("select * from _df")
        self._arrow_table = self._duck_conn.fetch_arrow_table()
        self._rowcount = self._arrow_table.num_rows

    def check_db_and_schema(self, expression: exp.Expression) -> None:
        no_database, no_schema = checks.is_unqualified_table_expression(expression)

        if no_database and not self._conn.database_set:
            cmd = expr.key_command(expression)
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"Cannot perform {cmd}. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",  # noqa: E501
                errno=90105,
                sqlstate="22000",
            )
        elif no_schema and not self._conn.schema_set:
            cmd = expr.key_command(expression)
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"Cannot perform {cmd}. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name.",  # noqa: E501
                errno=90106,
                sqlstate="22000",
            )

    def _transform(self, expression: exp.Expression, params: MutableParams | None) -> exp.Expression:
        return (
            expression.transform(lambda e: transforms.identifier(e, params))
            .transform(transforms.upper_case_unquoted_identifiers)
            .transform(transforms.update_variables, variables=self._conn.variables)
            .transform(transforms.set_schema, current_database=self._conn.database)
            .transform(transforms.create_database, db_path=self._conn.db_path)
            .transform(transforms.extract_comment_on_table)
            .transform(transforms.extract_comment_on_columns)
            .transform(transforms.information_schema_fs)
            .transform(transforms.information_schema_databases, current_schema=self._conn.schema)
            .transform(transforms.drop_schema_cascade)
            .transform(transforms.tag)
            .transform(transforms.semi_structured_types)
            .transform(transforms.try_parse_json)
            .transform(transforms.split)
            # NOTE: trim_cast_varchar must be before json_extract_cast_as_varchar
            .transform(transforms.trim_cast_varchar)
            # indices_to_json_extract must be before regex_substr
            .transform(transforms.indices_to_json_extract)
            .transform(transforms.json_extract_cast_as_varchar)
            .transform(transforms.json_extract_cased_as_varchar)
            .transform(transforms.json_extract_precedence)
            .transform(transforms.flatten_value_cast_as_varchar)
            .transform(transforms.flatten)
            .transform(transforms.regex_replace)
            .transform(transforms.regex_substr)
            .transform(transforms.result_scan)
            .transform(transforms.sequence_nextval)
            .transform(transforms.values_columns)
            .transform(transforms.to_date)
            .transform(transforms.to_decimal)
            .transform(transforms.try_to_decimal)
            .transform(transforms.to_timestamp)
            .transform(transforms.object_construct)
            .transform(transforms.timestamp_ntz)
            .transform(transforms.float_to_double)
            .transform(transforms.integer_precision)
            .transform(transforms.extract_text_length)
            .transform(transforms.sample)
            .transform(transforms.array_size)
            .transform(transforms.random)
            .transform(transforms.array_agg_within_group)
            .transform(transforms.array_agg)
            .transform(transforms.array_construct_etc)
            .transform(transforms.dateadd_date_cast)
            .transform(transforms.dateadd_string_literal_timestamp_cast)
            .transform(transforms.datediff_string_literal_timestamp_cast)
            .transform(transforms.show_databases)
            .transform(transforms.show_functions)
            .transform(transforms.show_procedures)
            .transform(transforms.show_warehouses)
            .transform(lambda e: transforms.show_schemas(e, self._conn.database))
            .transform(lambda e: transforms.show_tables_etc(e, self._conn.database, self._conn.schema))
            .transform(lambda e: transforms.show_columns(e, self._conn.database, self._conn.schema))
            .transform(lambda e: transforms.show_stages(e, self._conn.database, self._conn.schema))
            # TODO collapse into a single show_keys function
            .transform(lambda e: transforms.show_keys(e, self._conn.database, kind="PRIMARY"))
            .transform(lambda e: transforms.show_keys(e, self._conn.database, kind="UNIQUE"))
            .transform(lambda e: transforms.show_keys(e, self._conn.database, kind="FOREIGN"))
            .transform(transforms.show_users)
            .transform(transforms.create_user)
            .transform(transforms.sha256)
            .transform(transforms.create_clone)
            .transform(transforms.alias_in_join)
            .transform(transforms.alter_table_strip_cluster_by)
            .transform(lambda e: transforms.create_stage(e, self._conn.database, self._conn.schema))
            .transform(lambda e: transforms.list_stage(e, self._conn.database, self._conn.schema))
            .transform(lambda e: transforms.put_stage(e, self._conn.database, self._conn.schema, params))
            .transform(lambda e: transforms.create_table_as(e, self._duck_conn))
        )

    def _transform_explode(self, expression: exp.Expression) -> list[exp.Expression]:
        # Applies transformations that require splitting the expression into multiple expressions
        # Split transforms have limited support at the moment.

        auto_seq_result = transforms.create_table_autoincrement(expression)
        if len(auto_seq_result) > 1:
            return auto_seq_result

        merge_result = transforms.merge(expression)
        if len(merge_result) > 1:
            return merge_result

        alter_result = transforms.alter_table_add_multiple_columns(expression)
        if len(alter_result) > 1:
            return alter_result

        # Return original expression if no transform applied
        return [expression]

    def _execute(self, transformed: exp.Expression, params: MutableParams | None = None) -> None:
        self._arrow_table = None
        self._arrow_table_fetch_index = None
        self._rowcount = None
        self._sfqid = None

        cmd = expr.key_command(transformed)

        sql = transformed.sql(dialect="duckdb")

        if not sql:
            raise NotImplementedError(transformed.sql(dialect="snowflake"))

        if sfqid := transformed.args.get("result_scan_sfqid"):
            value = self._conn.results_cache.get(sfqid)
            if value is None:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"Statement {sfqid} not found", errno=709, sqlstate="02000"
                )
            # Restore the cached result data
            self._arrow_table, self._rowcount, self._last_sql, self._last_params, self._last_transformed = value
            self._sfqid = sfqid
            self._arrow_table_fetch_index = None
            return

        if transformed.find(exp.Select) and (seed := transformed.args.get("seed")):
            sql = f"SELECT setseed({seed}); {sql}"

        result_sql = None

        try:
            if isinstance(transformed, exp.Copy):
                sql = copy_into(self._duck_conn, self._conn.database, self._conn.schema, transformed, params)
            else:
                logger.log_sql(sql, params)
                self._duck_conn.execute(sql, params)
        except duckdb.BinderException as e:
            msg = e.args[0]
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=2043, sqlstate="02000") from e
        except duckdb.CatalogException as e:
            # minimal processing to make it look like a snowflake exception, message content may differ
            msg = cast(str, e.args[0]).split("\n")[0]
            raise snowflake.connector.errors.ProgrammingError(msg=msg, errno=2003, sqlstate="42S02") from e
        except duckdb.TransactionException as e:
            if "cannot rollback - no transaction is active" in str(
                e
            ) or "cannot commit - no transaction is active" in str(e):
                # snowflake doesn't error on rollback or commit outside a tx
                result_sql = SQL_SUCCESS
            else:
                raise e
        except duckdb.ConnectionException as e:
            raise snowflake.connector.errors.DatabaseError(msg=e.args[0], errno=250002, sqlstate="08003") from e
        except duckdb.ParserException as e:
            raise snowflake.connector.errors.ProgrammingError(msg=e.args[0], errno=1003, sqlstate="42000") from e

        affected_count = None

        if set_database := transformed.args.get("set_database"):
            self._conn.database = set_database
            self._conn.database_set = True
            self._conn.schema_set = False
            result_sql = SQL_SUCCESS

        elif set_schema := transformed.args.get("set_schema"):
            self._conn._schema = set_schema  # noqa: SLF001
            self._conn.schema_set = True
            result_sql = SQL_SUCCESS

        elif create_db_name := transformed.args.get("create_db_name"):
            # we created a new database, so create the info schema extensions
            self._duck_conn.execute(info_schema.per_db_creation_sql(create_db_name))
            result_sql = SQL_CREATED_DATABASE.substitute(name=create_db_name)

        elif stage_name := transformed.args.get("create_stage_name"):
            (affected_count,) = self._duck_conn.fetchall()[0]
            if affected_count == 0:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nObject '{stage_name}' already exists.",
                    errno=2002,
                    sqlstate="42710",
                )
            result_sql = SQL_CREATED_STAGE.substitute(name=stage_name)

        elif stage_name := transformed.args.get("list_stage_name") or transformed.args.get("put_stage_name"):
            if self._duck_conn.fetch_arrow_table().num_rows != 1:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nStage '{stage_name}' does not exist or not authorized.",
                    errno=2003,
                    sqlstate="02000",
                )
            if transformed.args.get("list_stage_name"):
                result_sql = stage.list_stage_files_sql(stage_name)
            elif transformed.args.get("put_stage_name"):
                result_sql = SQL_SUCCESS

        elif cmd == "INSERT":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_INSERTED_ROWS.substitute(count=affected_count)

        elif cmd == "UPDATE":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_UPDATED_ROWS.substitute(count=affected_count)

        elif cmd == "DELETE":
            (affected_count,) = self._duck_conn.fetchall()[0]
            result_sql = SQL_DELETED_ROWS.substitute(count=affected_count)

        elif cmd == "TRUNCATETABLE":
            result_sql = SQL_SUCCESS

        elif cmd in ("DESCRIBE TABLE", "DESCRIBE VIEW"):
            # DESCRIBE TABLE/VIEW has already been run above to detect and error if the table exists
            # We now rerun DESCRIBE TABLE/VIEW but transformed with columns to match Snowflake
            result_sql = transformed.transform(
                lambda e: transforms.describe_table(e, self._conn.database, self._conn.schema)
            ).sql(dialect="duckdb")

        elif eid := transformed.find(exp.Identifier, bfs=False):
            ident = eid.name
            if cmd == "CREATE SCHEMA" and ident:
                result_sql = SQL_CREATED_SCHEMA.substitute(name=ident)

            elif cmd == "CREATE SEQUENCE" and ident:
                result_sql = SQL_CREATED_SEQUENCE.substitute(name=ident)

            elif cmd == "CREATE TABLE" and ident:
                result_sql = SQL_CREATED_TABLE.substitute(name=ident)

            elif cmd.startswith("ALTER") and ident:
                result_sql = SQL_SUCCESS

            elif cmd == "CREATE VIEW" and ident:
                result_sql = SQL_CREATED_VIEW.substitute(name=ident)

            elif cmd.startswith("DROP") and ident:
                result_sql = SQL_DROPPED.substitute(name=ident)

                # if dropping the current database/schema then reset conn metadata
                if cmd == "DROP DATABASE" and ident == self._conn.database:
                    self._conn.database = None
                    self._conn._schema = None  # noqa: SLF001

                elif cmd == "DROP SCHEMA" and ident == self._conn.schema:
                    self._conn._schema = None  # noqa: SLF001
        elif (
            cmd == "CREATE"
            and isinstance(transformed, exp.Command)
            and isinstance(transformed.expression, str)
            and transformed.expression.upper().startswith(" SECRET")
        ):
            match = re.search(r"SECRET\s+(\w+)\s*\(", transformed.expression, re.IGNORECASE)
            secret_name = match[1].upper() if match else "UNKNOWN"
            result_sql = SQL_CREATED_SECRET.substitute(name=secret_name)

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
            logger.log_sql(result_sql)
            self._duck_conn.execute(result_sql)

        self._arrow_table = self._duck_conn.fetch_arrow_table()
        self._rowcount = affected_count or self._arrow_table.num_rows
        self._sfqid = str(uuid.uuid4())

        self._last_sql = result_sql or sql
        self._last_params = None if result_sql else params
        self._last_transformed = transformed

        self._conn.results_cache[self._sfqid] = (
            self._arrow_table,
            self._rowcount,
            self._last_sql,
            self._last_params,
            self._last_transformed,
        )

    def executemany(
        self,
        command: str,
        seqparams: Sequence[Any] | dict[str, Any],
        **kwargs: Any,
    ) -> FakeSnowflakeCursor:
        if isinstance(seqparams, dict):
            # see https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api
            raise NotImplementedError("executemany dict params")

        # TODO: support insert optimisations
        # the snowflake connector will optimise inserts into a single query
        # unless num_statements != 1 .. but for simplicity we execute each
        # query one by one, which means the response differs
        for p in seqparams:
            self.execute(command, p)

        return self

    def fetchall(self) -> list[tuple] | list[dict]:
        if self._prefetch_hook is not None:
            self._prefetch_hook()
        if self._arrow_table is None:
            # mimic snowflake python connector error type
            raise TypeError("No open result set")
        return self.fetchmany(self._arrow_table.num_rows)

    def fetch_pandas_all(self, **kwargs: dict[str, Any]) -> pd.DataFrame:
        if self._prefetch_hook is not None:
            self._prefetch_hook()
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

        if self._prefetch_hook is not None:
            self._prefetch_hook()
        if self._arrow_table is None:
            # mimic snowflake python connector error type
            raise TypeError("No open result set")
        tslice = self._arrow_table.slice(offset=self._arrow_table_fetch_index or 0, length=size).to_pylist()

        if self._arrow_table_fetch_index is None:
            self._arrow_table_fetch_index = size
        else:
            self._arrow_table_fetch_index += size

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
        return self._sfqid

    @property
    def sqlstate(self) -> str | None:
        return self._sqlstate

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

    def _inline_variables(self, sql: str) -> str:
        return self._conn.variables.inline_variables(sql)


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

    def populate_data(self, connection: SnowflakeConnection | None = None, **kwargs: Any) -> Self:
        raise NotImplementedError()

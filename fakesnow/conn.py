from __future__ import annotations

import os
from collections.abc import Iterable
from pathlib import Path
from types import TracebackType
from typing import Any

import snowflake.connector.converter
import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from snowflake.connector.cursor import DictCursor, SnowflakeCursor
from sqlglot import exp
from typing_extensions import Self

import fakesnow.info_schema as info_schema
import fakesnow.macros as macros
from fakesnow.cursor import FakeSnowflakeCursor
from fakesnow.variables import Variables


class FakeSnowflakeConnection:
    def __init__(
        self,
        duck_conn: DuckDBPyConnection,
        database: str | None = None,
        schema: str | None = None,
        create_database: bool = True,
        create_schema: bool = True,
        db_path: str | os.PathLike | None = None,
        nop_regexes: list[str] | None = None,
        *args: Any,
        **kwargs: Any,
    ):
        self._duck_conn = duck_conn
        self._is_closed = False
        # upper case database and schema like snowflake unquoted identifiers
        # so they appear as upper-cased in information_schema
        # catalog and schema names are not actually case-sensitive in duckdb even though
        # they are as cased in information_schema.schemata, so when selecting from
        # information_schema.schemata below we use upper-case to match any existing duckdb
        # catalog or schemas like "information_schema"
        self.database = database and database.upper()
        self._schema = schema and (
            "_FS_INFORMATION_SCHEMA" if schema.upper() == "INFORMATION_SCHEMA" else schema.upper()
        )

        self.database_set = False
        self.schema_set = False
        self.db_path = Path(db_path) if db_path else None
        self.nop_regexes = nop_regexes
        self._paramstyle = kwargs.get("paramstyle", snowflake.connector.paramstyle)
        self.variables = Variables()

        # create database if needed
        if (
            create_database
            and self.database
            and not duck_conn.execute(
                f"""select * from information_schema.schemata
                where upper(catalog_name) = '{self.database}'"""
            ).fetchone()
        ):
            db_file = f"{self.db_path / self.database}.db" if self.db_path else ":memory:"
            duck_conn.execute(f"ATTACH DATABASE '{db_file}' AS {self.database}")
            duck_conn.execute(info_schema.per_db_creation_sql(self.database))
            duck_conn.execute(macros.creation_sql(self.database))

        # create schema if needed
        if (
            create_schema
            and self.database
            and self._schema
            and not duck_conn.execute(
                f"""select * from information_schema.schemata
                where upper(catalog_name) = '{self.database}' and upper(schema_name) = '{self._schema}'"""
            ).fetchone()
        ):
            duck_conn.execute(f"CREATE SCHEMA {self.database}.{self._schema}")

        # set database and schema if both exist
        if (
            self.database
            and self._schema
            and duck_conn.execute(
                f"""select * from information_schema.schemata
                where upper(catalog_name) = '{self.database}' and upper(schema_name) = '{self._schema}'"""
            ).fetchone()
        ):
            duck_conn.execute(f"SET schema='{self.database}.{self._schema}'")
            self.database_set = True
            self.schema_set = True
        # set database if only that exists
        elif (
            self.database
            and duck_conn.execute(
                f"""select * from information_schema.schemata
                where upper(catalog_name) = '{self.database}'"""
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

    def autocommit(self, _mode: bool) -> None:
        # autcommit is always on in duckdb
        pass

    def close(self, retry: bool = True) -> None:
        self._duck_conn.close()
        self._is_closed = True

    def commit(self) -> None:
        self.cursor().execute("COMMIT")

    def cursor(self, cursor_class: type[SnowflakeCursor] = SnowflakeCursor) -> FakeSnowflakeCursor:
        # TODO: use duck_conn cursor for thread-safety
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
            if e and not isinstance(e, exp.Semicolon)  # ignore comments
        ]
        return cursors if return_cursors else []

    def is_closed(self) -> bool:
        return self._is_closed

    def rollback(self) -> None:
        self.cursor().execute("ROLLBACK")

    @property
    def schema(self) -> str | None:
        return "INFORMATION_SCHEMA" if self._schema == "_FS_INFORMATION_SCHEMA" else self._schema

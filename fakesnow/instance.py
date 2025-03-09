from __future__ import annotations

import os
from typing import Any

import duckdb

import fakesnow.fakes as fakes
from fakesnow import info_schema

GLOBAL_DATABASE_NAME = "_fs_global"


class FakeSnow:
    def __init__(
        self,
        create_database_on_connect: bool = True,
        create_schema_on_connect: bool = True,
        db_path: str | os.PathLike | None = None,
        nop_regexes: list[str] | None = None,
    ):
        self.create_database_on_connect = create_database_on_connect
        self.create_schema_on_connect = create_schema_on_connect
        self.db_path = db_path
        self.nop_regexes = nop_regexes

        self.duck_conn = duckdb.connect(database=":memory:")

        # create a "global" database for storing objects which span databases.
        self.duck_conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {GLOBAL_DATABASE_NAME}")
        # create the info schema extensions
        self.duck_conn.execute(info_schema.fs_global_creation_sql(GLOBAL_DATABASE_NAME))

    def connect(
        self, database: str | None = None, schema: str | None = None, **kwargs: Any
    ) -> fakes.FakeSnowflakeConnection:
        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting see
        # https://github.com/duckdb/duckdb/blob/18254ec/tools/pythonpkg/src/pyconnection.cpp#L1440
        # and to make connections thread-safe see
        # https://duckdb.org/docs/api/python/overview.html#using-connections-in-parallel-python-programs
        return fakes.FakeSnowflakeConnection(
            self.duck_conn.cursor(),
            database,
            schema,
            create_database=self.create_database_on_connect,
            create_schema=self.create_schema_on_connect,
            db_path=self.db_path,
            nop_regexes=self.nop_regexes,
            **kwargs,
        )

from __future__ import annotations

import os
from typing import Any

import duckdb

import fakesnow.fakes as fakes

GLOBAL_DATABASE_NAME = "_fs_global"
USERS_TABLE_FQ_NAME = f"{GLOBAL_DATABASE_NAME}._fs_users_ext"

# replicates the output structure of https://docs.snowflake.com/en/sql-reference/sql/show-users
SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT = f"""
create table if not exists {USERS_TABLE_FQ_NAME} (
    name varchar,
    created_on TIMESTAMPTZ,
    login_name varchar,
    display_name varchar,
    first_name varchar,
    last_name varchar,
    email varchar,
    mins_to_unlock varchar,
    days_to_expiry varchar,
    comment varchar,
    disabled varchar,
    must_change_password varchar,
    snowflake_lock varchar,
    default_warehouse varchar,
    default_namespace varchar,
    default_role varchar,
    default_secondary_roles varchar,
    ext_authn_duo varchar,
    ext_authn_uid varchar,
    mins_to_bypass_mfa varchar,
    owner varchar,
    last_success_login TIMESTAMPTZ,
    expires_at_time TIMESTAMPTZ,
    locked_until_time TIMESTAMPTZ,
    has_password varchar,
    has_rsa_public_key varchar,
)
"""


def create_global_database(conn: duckdb.DuckDBPyConnection) -> None:
    """Create a "global" database for storing objects which span databases.

    Including (but not limited to):
     - Users
    """
    conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {GLOBAL_DATABASE_NAME}")
    conn.execute(SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT)


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
        self.duck_conn.execute(SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT)

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

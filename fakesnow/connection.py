from __future__ import annotations

import contextlib
import os
from pathlib import Path

from duckdb import BinderException, DuckDBPyConnection

import fakesnow.info_schema as info_schema
import fakesnow.macros as macros

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


def init_connection(
    conn: DuckDBPyConnection,
    database: str | None = None,
    schema: str | None = None,
    create_database: bool = True,
    create_schema: bool = True,
    db_path: str | os.PathLike | None = None,
) -> tuple[bool, bool]:
    database_set = False
    schema_set = False

    create_global_database(conn)

    # create database if needed
    if (
        create_database
        and database
        and not conn.execute(
            f"""select * from information_schema.schemata
            where catalog_name = '{database}'"""
        ).fetchone()
    ):
        db_file = f"{Path(db_path)/database}.db" if db_path else ":memory:"
        conn.execute(f"ATTACH DATABASE '{db_file}' AS {database}")
        conn.execute(info_schema.creation_sql(database))
        conn.execute(macros.creation_sql(database))

    # create schema if needed
    if (
        create_schema
        and database
        and schema
        and not conn.execute(
            f"""select * from information_schema.schemata
            where catalog_name = '{database}' and schema_name = '{schema}'"""
        ).fetchone()
    ):
        conn.execute(f"CREATE SCHEMA {database}.{schema}")

    # set database and schema if both exist
    if (
        database
        and schema
        and conn.execute(
            f"""select * from information_schema.schemata
            where catalog_name = '{database}' and schema_name = '{schema}'"""
        ).fetchone()
    ):
        conn.execute(f"SET schema='{database}.{schema}'")
        database_set = True
        schema_set = True
    # set database if only that exists
    elif (
        database
        and conn.execute(
            f"""select * from information_schema.schemata
            where catalog_name = '{database}'"""
        ).fetchone()
    ):
        conn.execute(f"SET schema='{database}.main'")
        database_set = True

    # use UTC instead of local time zone for consistent testing
    conn.execute("SET GLOBAL TimeZone = 'UTC'")

    return database_set, schema_set


def create_global_database(conn: DuckDBPyConnection) -> None:
    """Create a "global" database for storing objects which span database.

    Including (but not limited to):
     - Users
    """
    with contextlib.suppress(BinderException):
        conn.execute(f"ATTACH DATABASE ':memory:' AS {GLOBAL_DATABASE_NAME}")

    conn.execute(SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT)

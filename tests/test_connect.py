# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false

from __future__ import annotations

import concurrent.futures
import tempfile

import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools

import fakesnow


def test_close_conn(conn: snowflake.connector.SnowflakeConnection):
    assert not conn.is_closed()

    conn.close()
    with pytest.raises(snowflake.connector.errors.DatabaseError) as excinfo:
        conn.execute_string("select 1")

    # actual snowflake error message is:
    # 250002 (08003): Connection is closed
    assert "250002 (08003)" in str(excinfo.value)

    assert conn.is_closed()


def test_close_cur(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.close() is True


def test_connect_auto_create(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1"):
        # creates db1 and schema1
        pass

    with snowflake.connector.connect(database="db1", schema="schema1"):
        # connects again and reuses db1 and schema1
        pass


def test_connect_different_sessions_use_database(_fakesnow_no_auto_create: None):
    # connect without default database and schema
    with snowflake.connector.connect() as conn1, conn1.cursor() as cur:
        # use the table's fully qualified name
        cur.execute("create database marts")
        cur.execute("create schema marts.jaffles")
        cur.execute("create table marts.jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into marts.jaffles.customers values (1, 'Jenny', 'P')")

        # use database and schema
        cur.execute("use database marts")
        assert cur.fetchall() == [("Statement executed successfully.",)]
        assert cur.description[0].name == "status"

        cur.execute("use schema jaffles")
        assert cur.fetchall() == [("Statement executed successfully.",)]
        assert cur.description[0].name == "status"

        cur.execute("insert into customers values (2, 'Jasper', 'M')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2, conn2.cursor() as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_connect_concurrently(_fakesnow: None) -> None:
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_a = executor.submit(snowflake.connector.connect)
        future_b = executor.submit(snowflake.connector.connect)

        futures = [future_a, future_b]

        for future in concurrent.futures.as_completed(futures):
            # exceptions if any will be raised here. we want to avoid
            # duckdb.duckdb.TransactionException: TransactionContext Error: Catalog write-write conflict
            _ = future.result()


def test_connect_db_path_can_create_database() -> None:
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path, fakesnow.patch(db_path=db_path):
        cursor = snowflake.connector.connect().cursor()
        cursor.execute("CREATE DATABASE db2")


def test_connect_db_path_reuse():
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path:
        with (
            fakesnow.patch(db_path=db_path),
            snowflake.connector.connect(database="db1", schema="schema1") as conn,
            conn.cursor() as cur,
        ):
            # creates db1.schema1.example
            cur.execute("create table example (x int)")
            cur.execute("insert into example values (420)")

        # reconnect
        with (
            fakesnow.patch(db_path=db_path),
            snowflake.connector.connect(database="db1", schema="schema1") as conn,
            conn.cursor() as cur,
        ):
            assert cur.execute("select * from example").fetchall() == [(420,)]


def test_connect_db_path_doesnt_exist():
    with fakesnow.patch(db_path="db-path-foobar"):
        with pytest.raises(NotADirectoryError) as excinfo:
            snowflake.connector.connect(database="db1")

        assert "No such directory: 'db-path-foobar'. Please ensure db_path exists." in str(excinfo.value)


def test_connect_information_schema():
    with fakesnow.patch(create_schema_on_connect=False):
        conn = snowflake.connector.connect(database="db1", schema="information_schema")
        assert conn.schema == "INFORMATION_SCHEMA"
        with conn, conn.cursor() as cur:
            # shouldn't fail
            cur.execute("SELECT * FROM databases")


def test_connect_then_unset_schema(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1") as conn, conn.cursor() as cur:
        # this will unset the schema
        cur.execute("USE DATABASE db1")

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )


def test_connect_without_database(_fakesnow_no_auto_create: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("select * from customers")

        # actual snowflake error message is:
        #
        # 002003 (42S02): SQL compilation error:
        # Object 'CUSTOMERS' does not exist or not authorized.
        # assert (
        #     "002003 (42S02): Catalog Error: Table with name customers does not exist!"
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("select * from jaffles.customers")

        assert (
            "090105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
            in str(excinfo.value)
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create schema jaffles")

        assert (
            "090105 (22000): Cannot perform CREATE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
            in str(excinfo.value)
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("use schema jaffles")

        # assert (
        #     "002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        assert (
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
            in str(excinfo.value)
        )

        cur.execute("create database db1")
        # should succeed even though there is no current database (used by dbeaver)
        cur.execute("show objects in schema db1.information_schema")

        # test description works without database
        assert cur.execute("SELECT 1").fetchall() == [(1,)]
        assert cur.description


def test_connect_without_schema(_fakesnow: None):
    # database will be created but not schema
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        assert not conn.schema

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("select * from customers")

        # actual snowflake error message is:
        #
        # 002003 (42S02): SQL compilation error:
        # Object 'CUSTOMERS' does not exist or not authorized.
        # assert (
        #     "002003 (42S02): Catalog Error: Table with name customers does not exist!"
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )

        # test description works without schema
        assert cur.execute("SELECT 1").fetchall() == [(1,)]
        assert cur.description

        conn.execute_string("CREATE SCHEMA schema1; USE SCHEMA schema1;")
        assert conn.schema == "SCHEMA1"


def test_connect_with_non_existent_db_or_schema(_fakesnow_no_auto_create: None):
    # can connect with db that doesn't exist
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        # but no valid database set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
            in str(excinfo.value)
        )

        # database still present on connection
        assert conn.database == "MARTS"

        cur.execute("CREATE database marts")

    # can connect with schema that doesn't exist
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn, conn.cursor() as cur:
        # but no valid schema set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )

        # schema still present on connection
        assert conn.schema == "JAFFLES"


def test_current_database_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select current_database(), current_schema()")

        assert cur.fetchall() == [
            {"current_database()": "DB1", "current_schema()": "SCHEMA1"},
        ]

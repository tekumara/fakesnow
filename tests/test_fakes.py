import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools


def test_connect_without_database(_fake_snow: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM customers")

        # actual snowflake error message is:
        #
        # 002003 (42S02): SQL compilation error:
        # Object 'CUSTOMERS' does not exist or not authorized.
        # assert (
        #     "002003 (42S02): Catalog Error: Table with name customers does not exist!"
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM jaffles.customers")

        assert (
            "090105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create schema jaffles")

        assert (
            "090105 (22000): Cannot perform CREATE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
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
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )


def test_connect_without_schema(_fake_snow: None):

    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        conn.execute_string(
            "CREATE database marts; USE database marts;"
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM customers")

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
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )


def test_connect_different_sessions_use_database(_fake_snow: None):
    # connect without default database and schema
    with snowflake.connector.connect() as conn1, conn1.cursor() as cur:
        # use the table's fully qualified name
        cur.execute("create database marts")
        cur.execute("create schema marts.jaffles")
        cur.execute("create table marts.jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into marts.jaffles.customers values (1, 'Jenny', 'P')")

        # use database and schema
        cur.execute("use database marts")
        cur.execute("use schema jaffles")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2, conn2.cursor() as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]

def test_connect_with_non_existent_db_or_schema(_fake_snow: None):
    # can connect with db that doesn't exist
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:

        # but no valid database set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        # database still present on connection
        assert conn.database == "marts"

        cur.execute("CREATE database marts")

    # can connect with schema that doesn't exist
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn, conn.cursor() as cur:

        # but no valid schema set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        # schema still present on connection
        assert conn.schema == "jaffles"


def test_current_database_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select current_database(), current_schema()")

        assert cur.fetchall() == [
            {"current_database()": "db1", "current_schema()": "schema1"},
        ]


def test_describe(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute(
            "create table customers (ID int, CNAME varchar, AMOUNT decimal(10,2), PCT real, UPDATE_AT timestamp)"
        )
        metadata = cur.describe("select * from customers")

        # fmt: off
        assert metadata == [
            snowflake.connector.cursor.ResultMetadata(
                name="ID", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True              # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="CNAME", type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True, # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="AMOUNT", type_code=0, display_size=None, internal_size=None, precision=10, scale=2, is_nullable=True,         # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="PCT", type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,       # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name='UPDATE_AT', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True        # type: ignore # noqa: E501
            ),
        ]
        # fmt: on


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    [_, cur2] = conn.execute_string(
        """ create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
            select count(*) customers """
    )
    assert [(1,)] == cur2.fetchall()


def test_fetchall(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_fetchall_dict_cursor(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]


def test_fetchone(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == (1, "Jenny", "P")
        assert cur.fetchone() == (2, "Jasper", "M")
        assert not cur.fetchone()


def test_fetchone_dict_cursor(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
        ]
        assert cur.fetchone() == [
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert not cur.fetchone()


def test_get_result_batches(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [(1, "Jenny", "P"), (2, "Jasper", "M")]
        assert sum(batch.rowcount for batch in batches) == 2


def test_get_result_batches_dict(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert sum(batch.rowcount for batch in batches) == 2


def test_non_existant_table_throws_snowflake_exception(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("select * from this_table_does_not_exist")


def test_schema_create_and_use(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema jaffles")
        cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("use schema jaffles")
        # fully qualified works too
        cur.execute("use schema db1.jaffles")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")


def test_use_invalid_schema(_fake_snow: None):

    with snowflake.connector.connect() as conn:
        conn.execute_string("CREATE DATABASE db1; USE DATABASE db1;")

    with conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("use schema this_does_not_exist")

        # assert (
        #     "002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )


def test_write_pandas(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
                {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "customers")

        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]

import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools
from pandas.testing import assert_frame_equal


def test_connect_auto_create(_fake_snow: None):

    with snowflake.connector.connect(database="db1", schema="schema1"):
        # creates db2 and schema1
        pass

    with snowflake.connector.connect(database="db1", schema="schema1"):
        # connects again and reuses db1 and schema1
        pass


def test_connect_without_database(_fake_snow_no_auto_create: None):
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


def test_connect_without_schema(_fake_snow_no_auto_create: None):

    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        conn.execute_string("CREATE database marts; USE database marts;")

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


def test_connect_different_sessions_use_database(_fake_snow_no_auto_create: None):
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


def test_connect_with_non_existent_db_or_schema(_fake_snow_no_auto_create: None):
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
        assert conn.database == "MARTS"

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
        assert conn.schema == "JAFFLES"


def test_current_database_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select current_database(), current_schema()")

        assert cur.fetchall() == [
            {"current_database()": "DB1", "current_schema()": "SCHEMA1"},
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


def test_fetch_pandas_all(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        expected_df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
                {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
            ]
        )
        assert_frame_equal(cur.fetch_pandas_all(), expected_df, check_dtype=False)


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


def test_regex(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("select regexp_replace('abc123', '\\\\D', '')")
        assert cur.fetchone() == ("123",)


def test_schema_create_and_use(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema jaffles")
        cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("use schema jaffles")
        # fully qualified works too
        cur.execute("use schema db1.jaffles")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")


def test_schema_drop(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema jaffles")
        cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        # dropping schema drops its contents
        cur.execute("drop schema jaffles")


def test_semi_structured_types(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table semi (emails array, name object, notes variant)")
        cur.execute(
            """insert into semi(emails, name, notes) SELECT [1, 2], parse_json('{"k1": "v2"}'), parse_json('["foo"]')"""
        )
        cur.execute(
            """insert into semi(emails, name, notes) VALUES ([3,4], parse_json('{"k2": "v2"}'), parse_json('["bar"]'))"""  # noqa: E501
        )

        cur.execute("select emails[0] from semi")
        # returned as strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)
        assert cur.fetchall() == [("1",), ("3",)]


def test_table_comments(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:

        def read_comment() -> str:
            cur.execute(
                """SELECT COALESCE(COMMENT, '') FROM INFORMATION_SCHEMA.TABLES
                        WHERE TABLE_NAME = 'INGREDIENTS' AND TABLE_SCHEMA = 'SCHEMA1' LIMIT 1"""
            )
            return cur.fetchall()[0][0]

        cur.execute("CREATE TABLE ingredients (id int) COMMENT = 'cheese'")
        assert read_comment() == "cheese"
        cur.execute("COMMENT ON TABLE ingredients IS 'pepperoni'")
        assert read_comment() == "pepperoni"
        cur.execute("COMMENT IF EXISTS ON TABLE schema1.ingredients IS 'mushrooms'")
        assert read_comment() == "mushrooms"


def test_tags_noop(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE table1 (id int)")
        cur.execute("ALTER TABLE table1 SET TAG foo='bar'")
        cur.execute("ALTER TABLE table1 MODIFY COLUMN name1 SET TAG foo='bar'")


def test_unquoted_identifiers_are_upper_cased(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (id int, first_name varchar, last_name varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("select first_name, first_name as fname from customers")

        assert cur.fetchall() == [
            {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
        ]

        cur.execute("select first_name, first_name as fname from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [
            {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
        ]


def test_use_invalid_schema(_fake_snow_no_auto_create: None):

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

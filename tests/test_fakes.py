# ruff: noqa: E501

import datetime
import json

import pandas as pd
import pytest
import pytz
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools
from pandas.testing import assert_frame_equal
from snowflake.connector.cursor import ResultMetadata


def test_alter_table(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table table1 (id int)")
    cur.execute("alter table table1 add column name varchar(20)")
    cur.execute("select name from table1")


def test_binding_default_paramstyle(conn: snowflake.connector.SnowflakeConnection):
    assert conn._paramstyle == "pyformat"  # noqa: SLF001
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (%s, %s, %s)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_binding_qmark(conn: snowflake.connector.SnowflakeConnection):
    conn._paramstyle = "qmark"  # noqa: SLF001
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (?, ?, ?)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_connect_auto_create(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1"):
        # creates db2 and schema1
        pass

    with snowflake.connector.connect(database="db1", schema="schema1"):
        # connects again and reuses db1 and schema1
        pass


def test_connect_without_database(_fakesnow_no_auto_create: None):
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


def test_connect_without_schema(_fakesnow: None):
    # database will be created but not schema
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        assert not conn.schema

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
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )

        conn.execute_string("CREATE SCHEMA schema1; USE SCHEMA schema1;")
        assert conn.schema == "SCHEMA1"


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
        cur.execute("use schema jaffles")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2, conn2.cursor() as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


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


def test_describe(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        create or replace table example (
            XBOOLEAN BOOLEAN, XDOUBLE DOUBLE, XFLOAT FLOAT,
            XNUMBER82 NUMBER(8,2), XNUMBER NUMBER, XDECIMAL DECIMAL, XNUMERIC NUMERIC,
            XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT,
            XVARCHAR20 VARCHAR(20), XVARCHAR VARCHAR, XTEXT TEXT,
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XDATE DATE, XTIME TIME,
            XBINARY BINARY
        )
        """
    )
    # fmt: off
    expected_metadata = [
        ResultMetadata(name='XBOOLEAN', type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XDOUBLE', type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XFLOAT', type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XNUMBER82', type_code=0, display_size=None, internal_size=None, precision=8, scale=2, is_nullable=True),
        ResultMetadata(name='XNUMBER', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XDECIMAL', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XNUMERIC', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XINTEGER', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XBIGINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XSMALLINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XTINYINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XBYTEINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        # TODO: store actual size
        ResultMetadata(name='XVARCHAR20', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XVARCHAR', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XTEXT', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP_NTZ9', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XDATE', type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XTIME', type_code=12, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XBINARY', type_code=11, display_size=None, internal_size=8388608, precision=None, scale=None, is_nullable=True)
    ]
    # fmt: on

    assert cur.describe("select * from example") == expected_metadata
    cur.execute("select * from example")
    assert cur.description == expected_metadata

    # test with params
    assert cur.describe("select * from example where XNUMBER = ?", (1,)) == expected_metadata
    cur.execute("select * from example where XNUMBER = ?", (1,))
    assert cur.description == expected_metadata


def test_executemany(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

    customers = [(1, "Jenny", "P"), (2, "Jasper", "M")]
    cur.executemany("insert into customers (id, first_name, last_name) values (%s,%s,%s)", customers)

    cur.execute("select id, first_name, last_name from customers")
    assert cur.fetchall() == customers


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    [_, cur2] = conn.execute_string(
        """ create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
            select count(*) customers """
    )
    assert [(1,)] == cur2.fetchall()


def test_fetchall(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_fetchone(cur: snowflake.connector.cursor.SnowflakeCursor):
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

        assert cur.fetchone() == {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"}
        assert cur.fetchone() == {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"}
        assert not cur.fetchone()


def test_fetch_pandas_all(cur: snowflake.connector.cursor.SnowflakeCursor):
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
    # integers have dtype int64
    assert_frame_equal(cur.fetch_pandas_all(), expected_df)


def test_floats_are_64bit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (f float, f4 float4, f8 float8, d double, r real)")
    cur.execute("insert into example values (1.23, 1.23, 1.23, 1.23, 1.23)")
    cur.execute("select * from example")
    # 32 bit floats will return 1.2300000190734863 rather than 1.23
    assert cur.fetchall() == [(1.23, 1.23, 1.23, 1.23, 1.23)]


def test_get_result_batches(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_information_schema_columns_numeric(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/data-types-numeric
    cur.execute(
        """
        create or replace table example (
            XBOOLEAN BOOLEAN, XDOUBLE DOUBLE, XFLOAT FLOAT, XNUMBER82 NUMBER(8,2), XNUMBER NUMBER, XDECIMAL DECIMAL, XNUMERIC NUMERIC,
            XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT
        )
        """
    )

    cur.execute(
        """
        select column_name,data_type,numeric_precision,numeric_precision_radix,numeric_scale
        from information_schema.columns where table_name = 'EXAMPLE' order by ordinal_position
        """
    )

    assert cur.fetchall() == [
        ("XBOOLEAN", "BOOLEAN", None, None, None),
        ("XDOUBLE", "FLOAT", None, None, None),
        ("XFLOAT", "FLOAT", None, None, None),
        ("XNUMBER82", "NUMBER", 8, 10, 2),
        ("XNUMBER", "NUMBER", 38, 10, 0),
        ("XDECIMAL", "NUMBER", 38, 10, 0),
        ("XNUMERIC", "NUMBER", 38, 10, 0),
        ("XINT", "NUMBER", 38, 10, 0),
        ("XINTEGER", "NUMBER", 38, 10, 0),
        ("XBIGINT", "NUMBER", 38, 10, 0),
        ("XSMALLINT", "NUMBER", 38, 10, 0),
        ("XTINYINT", "NUMBER", 38, 10, 0),
        ("XBYTEINT", "NUMBER", 38, 10, 0),
    ]


def test_information_schema_columns_other(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/data-types-datetime
    cur.execute(
        """
        create or replace table example (
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XDATE DATE, XTIME TIME,
            XBINARY BINARY
        )
        """
    )

    cur.execute(
        """
        select column_name,data_type
        from information_schema.columns where table_name = 'EXAMPLE' order by ordinal_position
        """
    )

    assert cur.fetchall() == [
        ("XTIMESTAMP", "TIMESTAMP_NTZ"),
        ("XTIMESTAMP_NTZ9", "TIMESTAMP_NTZ"),
        ("XDATE", "DATE"),
        ("XTIME", "TIME"),
        ("XBINARY", "BINARY"),
    ]


def test_information_schema_columns_text(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/data-types-text
    cur.execute(
        """
        create or replace table example (
            XVARCHAR20 VARCHAR(20), XVARCHAR VARCHAR, XTEXT TEXT
        )
        """
    )

    cur.execute(
        """
        select column_name,data_type,character_maximum_length,character_octet_length
        from information_schema.columns where table_name = 'EXAMPLE' order by ordinal_position
        """
    )

    assert cur.fetchall() == [
        ("XVARCHAR20", "TEXT", 20, 80),
        ("XVARCHAR", "TEXT", 16777216, 16777216),
        ("XTEXT", "TEXT", 16777216, 16777216),
    ]


def test_non_existent_table_throws_snowflake_exception(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")


def test_object_construct(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null)")

    # TODO: strip null within duckdb via python UDF
    def strip_none_values(d: dict) -> dict:
        return {k: v for k, v in d.items() if v}

    result = cur.fetchone()
    assert isinstance(result, tuple)
    assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1,\n  "b": "BBBB"\n}')


def test_regex(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select regexp_replace('abc123', '\\\\D', '')")
    assert cur.fetchone() == ("123",)


def test_regex_substr(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/regexp_substr
    string1 = "It was the best of times, it was the worst of times."

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+\\\\w+')")
    assert cur.fetchone() == ("the best",)

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+\\\\w+', 1, 2)")
    assert cur.fetchone() == ("the worst",)

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+(\\\\w+)', 1, 2, 'e', 1)")
    assert cur.fetchone() == ("worst",)


def test_rowcount(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example(id int)")
    cur.execute("insert into example SELECT * FROM (VALUES (1), (2), (3));")
    # TODO: rows inserted ie: 3
    assert cur.rowcount is None


def test_schema_create_and_use(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema jaffles")
    cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    cur.execute("use schema jaffles")
    # fully qualified works too
    cur.execute("use schema db1.jaffles")
    cur.execute("insert into customers values (1, 'Jenny', 'P')")


def test_schema_drop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema jaffles")
    cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    # dropping schema drops its contents
    cur.execute("drop schema jaffles")


def test_semi_structured_types(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table semis (emails array, name object, notes variant)")
    cur.execute(
        """insert into semis(emails, name, notes) SELECT [1, 2], parse_json('{"k": "v1"}'), parse_json('["foo"]')"""
    )
    cur.execute(
        """insert into semis(emails, name, notes) VALUES ([3,4], parse_json('{"k": "v2"}'), parse_json('{"b": "ar"}'))"""
    )

    # results are returned as strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)

    cur.execute("select emails[0] from semis")
    assert cur.fetchall() == [("1",), ("3",)]

    cur.execute("select name['k'] from semis")
    assert cur.fetchall() == [('"v1"',), ('"v2"',)]

    cur.execute("select notes[0] from semis")
    assert cur.fetchall() == [('"foo"',), (None,)]


def test_sqlstate(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select 'hello world'")
    # sqlstate is None on success
    assert cur.sqlstate is None

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")

    assert cur.sqlstate == "42S02"


def test_sfqid(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.sfqid == "fakesnow"


def test_table_comments(cur: snowflake.connector.cursor.SnowflakeCursor):
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
    cur.execute("ALTER TABLE ingredients SET comment = 'pineapple'")
    assert read_comment() == "pineapple"


def test_tags_noop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE table1 (id int)")
    cur.execute("ALTER TABLE table1 SET TAG foo='bar'")
    cur.execute("ALTER TABLE table1 MODIFY COLUMN name1 SET TAG foo='bar'")


def test_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT to_timestamp(0)")
    assert cur.fetchall() == [(datetime.datetime(1970, 1, 1, 0, 0),)]


def test_timestamp_to_date(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT to_date(to_timestamp(0)), to_date(cast(to_timestamp(0) as timestamp(9)))")
    assert cur.fetchall() == [(datetime.date(1970, 1, 1), datetime.date(1970, 1, 1))]


def test_write_pandas_timestamp_ntz(conn: snowflake.connector.SnowflakeConnection):
    # compensate for https://github.com/duckdb/duckdb/issues/7980
    with conn.cursor() as cur:
        cur.execute("create table example (UPDATE_AT_NTZ timestamp_ntz(9))")
        # cur.execute("create table example (UPDATE_AT_NTZ timestamp)")

        now_utc = datetime.datetime.now(pytz.utc)
        df = pd.DataFrame([(now_utc,)], columns=["UPDATE_AT_NTZ"])
        snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE")

        cur.execute("select * from example")

        assert cur.fetchall() == [(now_utc.replace(tzinfo=None),)]


def test_transactions(conn: snowflake.connector.SnowflakeConnection):
    conn.execute_string(
        """CREATE TABLE table1 (i int);
            BEGIN TRANSACTION;
            INSERT INTO table1 (i) VALUES (1);"""
    )
    conn.rollback()
    conn.execute_string(
        """BEGIN TRANSACTION;
            INSERT INTO table1 (i) VALUES (2);"""
    )
    conn.commit()
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM table1")
        assert cur.fetchall() == [(2,)]


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


def test_use_invalid_schema(_fakesnow: None):
    # database will be created but not schema
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("use schema this_does_not_exist")

        # assert (
        #     "002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."
        #     in str(excinfo.value)
        # )

        # invalid schema doesn't get set on the connection
        assert not conn.schema

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )


def test_values(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("SELECT * FROM VALUES ('Amsterdam', 1), ('London', 2)")

        assert cur.fetchall() == [
            {"COLUMN1": "Amsterdam", "COLUMN2": 1},
            {"COLUMN1": "London", "COLUMN2": 2},
        ]

        cur.execute(
            "SELECT column2, column1, parse_json(column3) as pj FROM VALUES ('Amsterdam', 1, '[]'), ('London', 2, '{}')"
        )

        assert cur.fetchall() == [
            {"COLUMN2": 1, "COLUMN1": "Amsterdam", "PJ": "[]"},
            {"COLUMN2": 2, "COLUMN1": "London", "PJ": "{}"},
        ]


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


def test_write_pandas_partial_columns(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny"},
                {"ID": 2, "FIRST_NAME": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "customers")

        cur.execute("select id, first_name, last_name from customers")

        # columns not in dataframe will receive their default value
        assert cur.fetchall() == [(1, "Jenny", None), (2, "Jasper", None)]


def test_write_pandas_dict_column_as_varchar(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table example (id str, vc varchar, o object)")

        df = pd.DataFrame(
            [("abc", {"kind": "vc", "count": 1}, {"kind": "obj", "amount": 2})], columns=["ID", "VC", "O"]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE")

        cur.execute("select * from example")

        # returned values are valid json strings
        # TODO: order object keys alphabetically like snowflake does
        assert cur.fetchall() == [("abc", '{"kind":"vc","count":1}', '{"kind":"obj","amount":2}')]

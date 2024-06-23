from __future__ import annotations

# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false
import datetime
import json
import tempfile
from decimal import Decimal

import pandas as pd
import pytest
import pytz
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools
from pandas.testing import assert_frame_equal
from snowflake.connector.cursor import ResultMetadata

import fakesnow
from tests.utils import dindent, indent


def test_alias_on_join(conn: snowflake.connector.SnowflakeConnection):
    *_, cur = conn.execute_string(
        """
        CREATE OR REPLACE TEMPORARY TABLE TEST (COL VARCHAR);
        INSERT INTO TEST (COL) VALUES ('VARCHAR1'), ('VARCHAR2');
        CREATE OR REPLACE TEMPORARY TABLE JOINED (COL VARCHAR, ANOTHER VARCHAR);
        INSERT INTO JOINED (COL, ANOTHER) VALUES ('CHAR1', 'JOIN');
        SELECT
            T.COL
            , SUBSTR(T.COL, 4) AS ALIAS
            , J.ANOTHER
        FROM TEST AS T
        LEFT JOIN JOINED AS J
        ON ALIAS = J.COL;
        """
    )
    assert cur.fetchall() == [("VARCHAR1", "CHAR1", "JOIN"), ("VARCHAR2", "CHAR2", None)]


def test_alter_table(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table table1 (id int)")
    cur.execute("alter table table1 add column name varchar(20)")
    cur.execute("select name from table1")


def test_array_size(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""select array_size(parse_json('["a","b"]'))""")
    assert cur.fetchall() == [(2,)]

    # when json is not an array
    cur.execute("""select array_size(parse_json('{"a":"b"}'))""")
    assert cur.fetchall() == [(None,)]


def test_array_agg(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table table1 (id number, name varchar)")
    values = [(1, "foo"), (2, "bar"), (1, "baz"), (2, "qux")]

    dcur.executemany("insert into table1 values (%s, %s)", values)

    dcur.execute("select array_agg(name) as names from table1")
    assert dindent(dcur.fetchall()) == [{"NAMES": '[\n  "foo",\n  "bar",\n  "baz",\n  "qux"\n]'}]

    # using over

    dcur.execute(
        """
            SELECT DISTINCT
                ID
                , ANOTHER
                , ARRAY_AGG(DISTINCT COL) OVER(PARTITION BY ID) AS COLS
            FROM (select column1 as ID, column2 as COL, column3 as ANOTHER from
            (VALUES (1, 's1', 'c1'),(1, 's2', 'c1'),(1, 's3', 'c1'),(2, 's1', 'c2'), (2,'s2','c2')))
            ORDER BY ID
            """
    )
    assert dindent(dcur.fetchall()) == [
        {"ID": 1, "ANOTHER": "c1", "COLS": '[\n  "s1",\n  "s2",\n  "s3"\n]'},
        {"ID": 2, "ANOTHER": "c2", "COLS": '[\n  "s1",\n  "s2"\n]'},
    ]


def test_array_agg_within_group(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE table1 (ID INT, amount INT)")

    # two unique ids, for id 1 there are 3 amounts, for id 2 there are 2 amounts
    values = [
        (2, 40),
        (1, 10),
        (1, 30),
        (2, 50),
        (1, 20),
    ]
    dcur.executemany("INSERT INTO TABLE1 VALUES (%s, %s)", values)

    dcur.execute("SELECT id, ARRAY_AGG(amount) WITHIN GROUP (ORDER BY amount DESC) amounts FROM table1 GROUP BY id")
    rows = dcur.fetchall()

    assert dindent(rows) == [
        {"ID": 1, "AMOUNTS": "[\n  30,\n  20,\n  10\n]"},
        {"ID": 2, "AMOUNTS": "[\n  50,\n  40\n]"},
    ]

    dcur.execute("SELECT id, ARRAY_AGG(amount) WITHIN GROUP (ORDER BY amount ASC) amounts FROM table1 GROUP BY id")
    rows = dcur.fetchall()

    assert dindent(rows) == [
        {"ID": 1, "AMOUNTS": "[\n  10,\n  20,\n  30\n]"},
        {"ID": 2, "AMOUNTS": "[\n  40,\n  50\n]"},
    ]


def test_binding_default_paramstyle(conn: snowflake.connector.SnowflakeConnection):
    assert snowflake.connector.paramstyle == "pyformat"
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (%s, %s, %s)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_binding_default_paramstyle_dict(conn: snowflake.connector.SnowflakeConnection):
    assert snowflake.connector.paramstyle == "pyformat"
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute(
            "insert into customers values (%(id)s, %(name)s, %(active)s)", {"id": 1, "name": "Jenny", "active": True}
        )
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_binding_qmark(_fakesnow: None):
    snowflake.connector.paramstyle = "qmark"

    with snowflake.connector.connect(database="db1", schema="schema1") as conn, conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (?, ?, ?)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]

        # this has no effect after connection created, so qmark style still works
        snowflake.connector.paramstyle = "pyformat"
        cur.execute("select * from customers where id = ?", (1,))


def test_clone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
    cur.execute("insert into customers values (1, 'Jenny', True)")

    cur.execute("create table customers2 clone db1.schema1.customers")
    cur.execute("select * from customers2")
    # TODO check tags are copied too
    assert cur.fetchall() == [(1, "Jenny", True)]


def test_close_conn(conn: snowflake.connector.SnowflakeConnection, cur: snowflake.connector.cursor.SnowflakeCursor):
    conn.close()
    with pytest.raises(snowflake.connector.errors.DatabaseError) as excinfo:
        conn.execute_string("select 1")

    # actual snowflake error message is:
    # 250002 (08003): Connection is closed
    assert "250002 (08003)" in str(excinfo.value)


def test_close_cur(conn: snowflake.connector.SnowflakeConnection, cur: snowflake.connector.cursor.SnowflakeCursor):
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
        cur.execute("use schema jaffles")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2, conn2.cursor() as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_connect_reuse_db():
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


def test_connect_db_path_can_create_database() -> None:
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path, fakesnow.patch(db_path=db_path):
        cursor = snowflake.connector.connect().cursor()
        cursor.execute("CREATE DATABASE db2")


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


def test_dateadd_date_cast(dcur: snowflake.connector.DictCursor):
    q = """
    SELECT
        dateadd(hour, 1, '2023-04-02'::date) as d_noop,
        dateadd(day, 1, '2023-04-02'::date) as d_day,
        dateadd(week, 1, '2023-04-02'::date) as d_week,
        dateadd(month, 1, '2023-04-02'::date) as d_month,
        dateadd(year, 1, '2023-04-02'::date) as d_year
    """

    dcur.execute(q)
    assert dcur.fetchall() == [
        {
            "D_NOOP": datetime.datetime(2023, 4, 2, 1, 0),
            "D_DAY": datetime.date(2023, 4, 3),
            "D_WEEK": datetime.date(2023, 4, 9),
            "D_MONTH": datetime.date(2023, 5, 2),
            "D_YEAR": datetime.date(2024, 4, 2),
        }
    ]


def test_dateadd_string_literal_timestamp_cast(dcur: snowflake.connector.cursor.DictCursor):
    q = """
    SELECT
        DATEADD('MINUTE', 3, '2023-04-02') AS D_MINUTE,
        DATEADD('HOUR', 3, '2023-04-02') AS D_HOUR,
        DATEADD('DAY', 3, '2023-04-02') AS D_DAY,
        DATEADD('WEEK', 3, '2023-04-02') AS D_WEEK,
        DATEADD('MONTH', 3, '2023-04-02') AS D_MONTH,
        DATEADD('YEAR', 3, '2023-04-02') AS D_YEAR
    ;
    """
    dcur.execute(q)

    assert dcur.fetchall() == [
        {
            "D_MINUTE": datetime.datetime(2023, 4, 2, 0, 3),
            "D_HOUR": datetime.datetime(2023, 4, 2, 3, 0),
            "D_DAY": datetime.datetime(2023, 4, 5, 0, 0),
            "D_WEEK": datetime.datetime(2023, 4, 23, 0, 0),
            "D_MONTH": datetime.datetime(2023, 7, 2, 0, 0),
            "D_YEAR": datetime.datetime(2026, 4, 2, 0, 0),
        }
    ]

    q = """
    SELECT
        DATEADD('MINUTE', 3, '2023-04-02 01:15:00') AS DT_MINUTE,
        DATEADD('HOUR', 3, '2023-04-02 01:15:00') AS DT_HOUR,
        DATEADD('DAY', 3, '2023-04-02 01:15:00') AS DT_DAY,
        DATEADD('WEEK', 3, '2023-04-02 01:15:00') AS DT_WEEK,
        DATEADD('MONTH', 3, '2023-04-02 01:15:00') AS DT_MONTH,
        DATEADD('YEAR', 3, '2023-04-02 01:15:00') AS DT_YEAR
    ;
    """
    dcur.execute(q)

    assert dcur.fetchall() == [
        {
            "DT_MINUTE": datetime.datetime(2023, 4, 2, 1, 18),
            "DT_HOUR": datetime.datetime(2023, 4, 2, 4, 15),
            "DT_DAY": datetime.datetime(2023, 4, 5, 1, 15),
            "DT_WEEK": datetime.datetime(2023, 4, 23, 1, 15),
            "DT_MONTH": datetime.datetime(2023, 7, 2, 1, 15),
            "DT_YEAR": datetime.datetime(2026, 4, 2, 1, 15),
        }
    ]


def test_datediff_string_literal_timestamp_cast(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT DATEDIFF(DAY, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-31,)]

    cur.execute("SELECT DATEDIFF(HOUR, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-744,)]

    cur.execute("SELECT DATEDIFF(week, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-4,)]

    # noop
    cur.execute("select '2023-04-02'::timestamp as c1, '2023-03-02'::timestamp as c2, DATEDIFF(minute, c1, c2) AS D")
    assert cur.fetchall() == [(datetime.datetime(2023, 4, 2, 0, 0), datetime.datetime(2023, 3, 2, 0, 0), -44640)]


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
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ TIMESTAMP_NTZ, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XTIMESTAMP_TZ TIMESTAMP_TZ, XDATE DATE, XTIME TIME,
            XBINARY BINARY, /* XARRAY ARRAY, XOBJECT OBJECT */ XVARIANT VARIANT
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
        ResultMetadata(name='XTIMESTAMP_NTZ', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP_NTZ9', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP_TZ', type_code=7, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XDATE', type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XTIME', type_code=12, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XBINARY', type_code=11, display_size=None, internal_size=8388608, precision=None, scale=None, is_nullable=True),
        # TODO: handle ARRAY and OBJECT see https://github.com/tekumara/fakesnow/issues/26
        # ResultMetadata(name='XARRAY', type_code=10, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        # ResultMetadata(name='XOBJECT', type_code=9, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XVARIANT', type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
    ]
    # fmt: on

    assert cur.describe("select * from example") == expected_metadata
    cur.execute("select * from example")
    assert cur.description == expected_metadata

    # test with params
    assert cur.describe("select * from example where XNUMBER = %s", (1,)) == expected_metadata
    cur.execute("select * from example where XNUMBER = %s", (1,))
    assert cur.description == expected_metadata

    # test semi-structured ops return variant ie: type_code=5
    # fmt: off
    assert (
        cur.describe("SELECT ['A', 'B'][0] as array_index, OBJECT_CONSTRUCT('k','v1')['k'] as object_key, ARRAY_CONSTRUCT('foo')::VARIANT[0] as variant_key")
        == [
            # NB: snowflake returns internal_size = 16777216 for all columns
            ResultMetadata(name="ARRAY_INDEX", type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
            ResultMetadata(name="OBJECT_KEY", type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
            ResultMetadata(name="VARIANT_KEY", type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True)
        ]
    )
    # fmt: on


def test_describe_table(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute(
        """
        create or replace table example (
            XBOOLEAN BOOLEAN, XDOUBLE DOUBLE, XFLOAT FLOAT,
            XNUMBER82 NUMBER(8,2), XNUMBER NUMBER, XDECIMAL DECIMAL, XNUMERIC NUMERIC,
            XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT,
            XVARCHAR20 VARCHAR(20), XVARCHAR VARCHAR, XTEXT TEXT,
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ TIMESTAMP_NTZ, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XTIMESTAMP_TZ TIMESTAMP_TZ, XDATE DATE, XTIME TIME,
            XBINARY BINARY, /* XARRAY ARRAY, XOBJECT OBJECT */ XVARIANT VARIANT
        )
        """
    )
    # this table's columns shouldn't appear when describing the example table
    dcur.execute("create table derived as select XVARCHAR20 from example")

    common = {
        "kind": "COLUMN",
        "null?": "Y",
        "default": None,
        "primary key": "N",
        "unique key": "N",
        "check": None,
        "expression": None,
        "comment": None,
        "policy name": None,
        "privacy domain": None,
    }
    expected = [
        {"name": "XBOOLEAN", "type": "BOOLEAN", **common},
        {"name": "XDOUBLE", "type": "FLOAT", **common},
        {"name": "XFLOAT", "type": "FLOAT", **common},
        {"name": "XNUMBER82", "type": "NUMBER(8,2)", **common},
        {"name": "XNUMBER", "type": "NUMBER(38,0)", **common},
        {"name": "XDECIMAL", "type": "NUMBER(38,0)", **common},
        {"name": "XNUMERIC", "type": "NUMBER(38,0)", **common},
        {"name": "XINT", "type": "NUMBER(38,0)", **common},
        {"name": "XINTEGER", "type": "NUMBER(38,0)", **common},
        {"name": "XBIGINT", "type": "NUMBER(38,0)", **common},
        {"name": "XSMALLINT", "type": "NUMBER(38,0)", **common},
        {"name": "XTINYINT", "type": "NUMBER(38,0)", **common},
        {"name": "XBYTEINT", "type": "NUMBER(38,0)", **common},
        {"name": "XVARCHAR20", "type": "VARCHAR(20)", **common},
        {"name": "XVARCHAR", "type": "VARCHAR(16777216)", **common},
        {"name": "XTEXT", "type": "VARCHAR(16777216)", **common},
        {"name": "XTIMESTAMP", "type": "TIMESTAMP_NTZ(9)", **common},
        {"name": "XTIMESTAMP_NTZ", "type": "TIMESTAMP_NTZ(9)", **common},
        {"name": "XTIMESTAMP_NTZ9", "type": "TIMESTAMP_NTZ(9)", **common},
        {"name": "XTIMESTAMP_TZ", "type": "TIMESTAMP_TZ(9)", **common},
        {"name": "XDATE", "type": "DATE", **common},
        {"name": "XTIME", "type": "TIME(9)", **common},
        {"name": "XBINARY", "type": "BINARY(8388608)", **common},
        {"name": "XVARIANT", "type": "VARIANT", **common},
    ]

    assert dcur.execute("describe table example").fetchall() == expected
    assert dcur.execute("describe table schema1.example").fetchall() == expected
    assert dcur.execute("describe table db1.schema1.example").fetchall() == expected
    assert [r.name for r in dcur.description] == [
        "name",
        "type",
        "kind",
        "null?",
        "default",
        "primary key",
        "unique key",
        "check",
        "expression",
        "comment",
        "policy name",
        "privacy domain",
    ]

    assert dcur.execute("describe table db1.schema1.derived").fetchall() == [
        # TODO: preserve varchar size when derived - this should be VARCHAR(20)
        {"name": "XVARCHAR20", "type": "VARCHAR(16777216)", **common},
    ]

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("describe table this_does_not_exist")

    # TODO: actual snowflake error is:
    # 002003 (42S02): SQL compilation error:
    # Table 'THIS_DOES_NOT_EXIST' does not exist or not authorized.
    assert "002003 (42S02): Catalog Error: Table with name THIS_DOES_NOT_EXIST does not exist!" in str(excinfo.value)


## descriptions are needed for ipython-sql/jupysql which describes every statement
def test_description_create_drop_database(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create database example")
    assert dcur.fetchall() == [{"status": "Database EXAMPLE successfully created."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
    # TODO: support drop database
    # dcur.execute("drop database example")
    # assert dcur.fetchall() == [{"status": "EXAMPLE successfully dropped."}]
    # assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip


def test_description_create_drop_schema(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create schema example")
    assert dcur.fetchall() == [{"status": "Schema EXAMPLE successfully created."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
    # drop current schema
    dcur.execute("drop schema schema1")
    assert dcur.fetchall() == [{"status": "SCHEMA1 successfully dropped."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip


def test_description_create_alter_drop_table(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table example (x int)")
    assert dcur.fetchall() == [{"status": "Table EXAMPLE successfully created."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
    dcur.execute("alter table example add column name varchar(20)")
    assert dcur.fetchall() == [{"status": "Statement executed successfully."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
    dcur.execute("drop table example")
    assert dcur.fetchall() == [{"status": "EXAMPLE successfully dropped."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip


def test_description_create_drop_view(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create view example(id) as select 1")
    assert dcur.fetchall() == [{"status": "View EXAMPLE successfully created."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
    dcur.execute("drop view example")
    assert dcur.fetchall() == [{"status": "EXAMPLE successfully dropped."}]
    assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip


def test_description_insert(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table example (x int)")
    dcur.execute("insert into example values (1), (2)")
    assert dcur.fetchall() == [{"number of rows inserted": 2}]
    # TODO: Snowflake is actually precision=19, is_nullable=False
    assert dcur.description == [ResultMetadata(name='number of rows inserted', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)]  # fmt: skip


def test_description_update(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table example (x int)")
    dcur.execute("insert into example values (1), (2), (3)")
    dcur.execute("update example set x=420 where x > 1")
    assert dcur.fetchall() == [{"number of rows updated": 2, "number of multi-joined rows updated": 0}]
    # TODO: Snowflake is actually precision=19, is_nullable=False
    # fmt: off
    assert dcur.description == [
        ResultMetadata(name='number of rows updated', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='number of multi-joined rows updated', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)
    ]
    # fmt: on


def test_description_delete(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table example (x int)")
    dcur.execute("insert into example values (1), (2), (3)")
    dcur.execute("delete from example where x>1")
    assert dcur.fetchall() == [{"number of rows deleted": 2}]
    # TODO: Snowflake is actually precision=19, is_nullable=False
    # fmt: off
    assert dcur.description == [
        ResultMetadata(name='number of rows deleted', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
    ]
    # fmt: on


def test_description_select(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("SELECT DATEDIFF( DAY, '2023-04-02'::DATE, '2023-04-05'::DATE) as days")
    assert dcur.fetchall() == [{"DAYS": 3}]
    # TODO: Snowflake is actually precision=9, is_nullable=False
    # fmt: off
    assert dcur.description == [
        ResultMetadata(name='DAYS', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
    ]
    # fmt: on


def test_equal_null(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select equal_null(NULL, NULL), equal_null(1, 1), equal_null(1, 2), equal_null(1, NULL)")
    assert cur.fetchall() == [(True, True, False, False)]


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
    assert cur2.fetchall() == [(1,)]


def test_fetchall(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        # no result set
        with pytest.raises(TypeError) as _:
            cur.fetchall()

        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]
        assert cur.fetchall() == []

    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert cur.fetchall() == []


def test_fetchone(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == (1, "Jenny", "P")
        assert cur.fetchone() == (2, "Jasper", "M")
        assert cur.fetchone() is None

    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"}
        assert cur.fetchone() == {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"}
        assert cur.fetchone() is None


def test_fetchmany(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        # no result set
        with pytest.raises(TypeError) as _:
            cur.fetchmany()

        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("insert into customers values (3, 'Jeremy', 'K')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchmany(2) == [(1, "Jenny", "P"), (2, "Jasper", "M")]
        assert cur.fetchmany(2) == [(3, "Jeremy", "K")]
        assert cur.fetchmany(2) == []

    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchmany(2) == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert cur.fetchmany(2) == [
            {"ID": 3, "FIRST_NAME": "Jeremy", "LAST_NAME": "K"},
        ]
        assert cur.fetchmany(2) == []


def test_fetch_pandas_all(cur: snowflake.connector.cursor.SnowflakeCursor):
    # no result set
    with pytest.raises(snowflake.connector.NotSupportedError) as _:
        cur.fetch_pandas_all()

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

    # can refetch
    assert_frame_equal(cur.fetch_pandas_all(), expected_df)


def test_flatten(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select t.id, flat.value:fruit from
        (
            select 1, parse_json('[{"fruit":"banana"}]')
            union
            select 2, parse_json('[{"fruit":"coconut"}, {"fruit":"durian"}]')
        ) as t(id, fruits), lateral flatten(input => t.fruits) AS flat
        order by id
        """
        # duckdb lateral join order is non-deterministic so order by id
        # within an id the order of fruits should match the json array
    )
    assert cur.fetchall() == [(1, '"banana"'), (2, '"coconut"'), (2, '"durian"')]


def test_flatten_value_cast_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select id, f.value::varchar as v
        from (select column1 as id, column2 as col from (values (1, 's1,s2,s3'), (2, 's1,s2'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id
        """
    )
    # should be raw string not json string with double quotes
    assert cur.fetchall() == [(1, "s1"), (1, "s2"), (1, "s3"), (2, "s1"), (2, "s2")]


def test_floats_are_64bit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (f float, f4 float4, f8 float8, d double, r real)")
    cur.execute("insert into example values (1.23, 1.23, 1.23, 1.23, 1.23)")
    cur.execute("select * from example")
    # 32 bit floats will return 1.2300000190734863 rather than 1.23
    assert cur.fetchall() == [(1.23, 1.23, 1.23, 1.23, 1.23)]


def test_get_path_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""select parse_json('{"fruit":"banana"}'):fruit""")
    assert cur.fetchall() == [('"banana"',)]

    # converting json to varchar returns unquoted string
    cur.execute("""select parse_json('{"fruit":"banana"}'):fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    # nested json
    cur.execute("""select get_path(parse_json('{"food":{"fruit":"banana"}}'), 'food.fruit')::varchar""")
    assert cur.fetchall() == [("banana",)]

    cur.execute("""select parse_json('{"food":{"fruit":"banana"}}'):food.fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    cur.execute("""select parse_json('{"food":{"fruit":"banana"}}'):food:fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    # json number is varchar
    cur.execute("""select parse_json('{"count":42}'):count""")
    assert cur.fetchall() == [("42",)]

    # lower/upper converts to varchar (ie: no quotes) ¯\_(ツ)_/¯
    cur.execute("""select upper(parse_json('{"fruit":"banana"}'):fruit)""")
    assert cur.fetchall() == [("BANANA",)]

    cur.execute("""select lower(parse_json('{"fruit":"banana"}'):fruit)""")
    assert cur.fetchall() == [("banana",)]

    # lower/upper converts json number to varchar too
    cur.execute("""select upper(parse_json('{"count":"42"}'):count)""")
    assert cur.fetchall() == [("42",)]


def test_get_path_precedence(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select {'K1': {'K2': 1}} as col where col:K1:K2 > 0")
    assert indent(cur.fetchall()) == [('{\n  "K1": {\n    "K2": 1\n  }\n}',)]

    cur.execute(
        """select parse_json('{"K1": "a", "K2": "b"}') as col, case when col:K1::VARCHAR = 'a' and col:K2::VARCHAR = 'b' then 'yes' end"""
    )
    assert indent(cur.fetchall()) == [('{\n  "K1": "a",\n  "K2": "b"\n}', "yes")]


def test_get_result_batches(cur: snowflake.connector.cursor.SnowflakeCursor):
    # no result set
    assert cur.get_result_batches() is None

    cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    cur.execute("insert into customers values (1, 'Jenny', 'P')")
    cur.execute("insert into customers values (2, 'Jasper', 'M')")
    cur.execute("select id, first_name, last_name from customers")
    batches = cur.get_result_batches()
    assert batches

    rows = [row for batch in batches for row in batch]
    assert rows == [(1, "Jenny", "P"), (2, "Jasper", "M")]
    assert sum(batch.rowcount for batch in batches) == 2


def test_get_result_batches_dict(dcur: snowflake.connector.cursor.DictCursor):
    # no result set
    assert dcur.get_result_batches() is None

    dcur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    dcur.execute("insert into customers values (1, 'Jenny', 'P')")
    dcur.execute("insert into customers values (2, 'Jasper', 'M')")
    dcur.execute("select id, first_name, last_name from customers")
    batches = dcur.get_result_batches()
    assert batches

    rows = [row for batch in batches for row in batch]
    assert rows == [
        {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
        {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
    ]
    assert sum(batch.rowcount for batch in batches) == 2

    assert_frame_equal(
        batches[0].to_pandas(),
        pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
                {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
            ]
        ),
    )


def test_identifier(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (x int)")
    cur.execute("insert into example values(1)")
    cur.execute("select * from identifier('example')")
    assert cur.fetchall() == [(1,)]


def test_nop_regexes():
    with fakesnow.patch(nop_regexes=["^CALL.*"]), snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("call this_procedure_does_not_exist('foo', 'bar);")
        assert cur.fetchall() == [("Statement executed successfully.",)]


def test_non_existent_table_throws_snowflake_exception(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")


def test_object_construct(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null)")

        # TODO: strip null within duckdb via python UDF
        def strip_none_values(d: dict) -> dict:
            return {k: v for k, v in d.items() if v}

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1,\n  "b": "BBBB"\n}')

    with conn.cursor() as cur:
        cur.execute("SELECT OBJECT_CONSTRUCT('a', 1, null, 'nulkeyed') as col")

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1\n}')

    with conn.cursor() as cur:
        cur.execute(
            "SELECT NULL as col, OBJECT_CONSTRUCT( 'k1', 'v1', 'k2', CASE WHEN ZEROIFNULL(col) + 1 >= 2 THEN 'v2' ELSE NULL END, 'k3', 'v3')"
        )

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[1])) == json.loads('{\n  "k1": "v1",\n  "k3": "v3"\n}')

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 as col, OBJECT_CONSTRUCT( 'k1', 'v1', 'k2', CASE WHEN ZEROIFNULL(col) + 1 >= 2 THEN 'v2' ELSE NULL END, 'k3', 'v3')"
        )

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[1])) == json.loads(
            '{\n  "k1": "v1",\n  "k2": "v2",\n  "k3": "v3"\n}'
        )


def test_percentile_cont(conn: snowflake.connector.SnowflakeConnection):
    *_, cur = conn.execute_string(
        """
        create or replace table aggr(k int, v decimal(10,2));
        insert into aggr (k, v) values
            (0,  0),
            (0, 10),
            (0, 20),
            (0, 30),
            (0, 40),
            (1, 10),
            (1, 20),
            (2, 10),
            (2, 20),
            (2, 25),
            (2, 30),
            (3, 60),
            (4, NULL);
        select k, percentile_cont(0.25) within group (order by v)
            from aggr
            group by k
            order by k;
        """
    )
    assert cur.fetchall() == [
        (0, Decimal("10.00000")),
        (1, Decimal("12.50000")),
        (2, Decimal("17.50000")),
        (3, Decimal("60.00000")),
        (4, None),
    ]


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


def test_random(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select random(420)")
    assert cur.fetchall() == [(-2595895151578578944,)]
    cur.execute("select random(420)")
    assert cur.fetchall() == [(-2595895151578578944,)]
    cur.execute("select random(419)")
    assert cur.fetchall() == [(4590143504000221184,)]
    assert cur.execute("select random()").fetchall() != cur.execute("select random()").fetchall()


def test_rowcount(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.rowcount is None
    cur.execute("create or replace table example(id int)")
    cur.execute("insert into example select * from (VALUES (1), (2), (3), (4));")
    assert cur.rowcount == 4
    cur.execute("select * from example where id > 1")
    assert cur.rowcount == 3
    cur.execute("update example set id = 22 where id > 2")
    assert cur.rowcount == 2


def test_sample(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example(id int)")
    cur.execute("insert into example select * from (VALUES (1), (2), (3), (4));")
    cur.execute("select * from example SAMPLE (50) SEED (420)")
    # sampling small sizes isn't exact
    assert cur.fetchall() == [(1,), (2,), (3,)]


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
    cur.execute("create or replace table semis (emails array, names object, notes variant)")
    cur.execute(
        """insert into semis(emails, names, notes) SELECT ['A', 'B'], OBJECT_CONSTRUCT('k','v1'), ARRAY_CONSTRUCT('foo')::VARIANT"""
    )
    cur.execute(
        """insert into semis(emails, names, notes) SELECT ['C','D'], parse_json('{"k": "v2"}'), parse_json('{"b": "ar"}')"""
    )

    # results are returned as strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)

    cur.execute("select emails from semis")
    assert indent(cur.fetchall()) == [('[\n  "A",\n  "B"\n]',), ('[\n  "C",\n  "D"\n]',)]

    cur.execute("select emails[0] from semis")
    assert cur.fetchall() == [('"A"',), ('"C"',)]

    cur.execute("select names['k'] from semis")
    assert cur.fetchall() == [('"v1"',), ('"v2"',)]

    cur.execute("select notes[0] from semis")
    assert cur.fetchall() == [('"foo"',), (None,)]

    cur.execute(
        """
            SELECT OBJECT_CONSTRUCT('key_1', 'one', 'key_2', NULL) AS WITHOUT_KEEP_NULL,
                   OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL) AS KEEP_NULL_1,
                   OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', NULL, 'two') AS KEEP_NULL_2
        """
    )
    assert indent(cur.fetchall()) == [
        ('{\n  "key_1": "one"\n}', '{\n  "key_1": "one",\n  "key_2": null\n}', '{\n  "key_1": "one"\n}')
    ]


@pytest.mark.xfail(
    reason="only partial supports exists to support sqlalchemy, see test_reflect",
)
def test_show_keys(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT UNIQUE)")
    dcur.execute("CREATE TABLE test_table2 (id INT, other_id INT, FOREIGN KEY (other_id) REFERENCES test_table(id))")

    dcur.execute("SHOW PRIMARY KEYS")
    primary_keys = dcur.fetchall()
    assert primary_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "TEST_TABLE",
            "column_name": "ID",
            "key_sequence": 1,
            "constraint_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_ID_pk",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW UNIQUE KEYS")
    unique_keys = dcur.fetchall()
    assert unique_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "TEST_TABLE",
            "column_name": "NAME",
            "key_sequence": 1,
            "constraint_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_NAME_uk",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW IMPORTED KEYS")
    foreign_keys = dcur.fetchall()
    assert foreign_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "pk_database_name": "DB1",
            "pk_schema_name": "SCHEMA1",
            "pk_table_name": "TEST_TABLE",
            "pk_column_name": "ID",
            "fk_database_name": "DB1",
            "fk_schema_name": "SCHEMA1",
            "fk_table_name": "TEST_TABLE2",
            "fk_column_name": "OTHER_ID",
            "key_sequence": 1,
            "update_rule": "NO ACTION",
            "delete_rule": "NO ACTION",
            "fk_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE2_OTHER_ID_fk",
            "pk_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_ID_pk",
            "deferrability": "NOT DEFERRABLE",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW PRIMARY KEYS IN SCHEMA")
    assert dcur.fetchall() == primary_keys

    dcur.execute("SHOW PRIMARY KEYS IN DATABASE")
    assert dcur.fetchall() == primary_keys


def test_show_objects(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create view view1 as select * from example")
    dcur.execute("show terse objects in db1.schema1")
    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "EXAMPLE",
            "kind": "TABLE",
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "VIEW1",
            "kind": "VIEW",
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
        },
    ]
    assert dcur.fetchall() == objects

    dcur.execute("show terse objects in database")
    assert dcur.fetchall() == [
        *objects,
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "databases",
            "kind": "VIEW",
            "database_name": "DB1",
            "schema_name": "information_schema",
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "views",
            "kind": "VIEW",
            "database_name": "DB1",
            "schema_name": "information_schema",
        },
    ]
    assert [r.name for r in dcur.description] == ["created_on", "name", "kind", "database_name", "schema_name"]

    dcur.execute("show objects").fetchall()
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "comment",
        # TODO: include these columns
        # "cluster_by",
        # "rows",
        # "bytes",
        # "owner",
        # "retention_time",
        # "owner_role_type",
        # "budget"
    ]


def test_show_schemas(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show terse schemas in database db1 limit 100")
    assert dcur.fetchall() == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "SCHEMA1",
            "kind": None,
            "database_name": "DB1",
            "schema_name": None,
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "information_schema",
            "kind": None,
            "database_name": "DB1",
            "schema_name": None,
        },
    ]
    assert [r.name for r in dcur.description] == ["created_on", "name", "kind", "database_name", "schema_name"]


def test_show_tables(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create view view1 as select * from example")
    dcur.execute("show terse tables")
    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "EXAMPLE",
            "kind": "TABLE",
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
        },
    ]
    # assert dcur.fetchall() == objects
    dcur.execute("show terse tables in db1.schema1")
    assert dcur.fetchall() == objects
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]

    dcur.execute("show tables in db1.schema1")
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "comment",
        # TODO: include these columns
        # "cluster_by",
        # "rows",
        # "bytes",
        # "owner",
        # "retention_time",
        # "automatic_clustering",
        # "change_tracking",
        # "search_optimization",
        # "search_optimization_progress",
        # "search_optimization_bytes",
        # "is_external",
        # "enable_schema_evolution",
        # "owner_role_type",
        # "is_event",
        # "budget",
        # "is_hybrid",
        # "is_iceberg",
    ]


def test_show_primary_keys(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE TABLE example (id int, name varchar, PRIMARY KEY (id, name))")

    dcur.execute("show primary keys")
    result = dcur.fetchall()

    assert result == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "EXAMPLE",
            "column_name": "ID",
            "key_sequence": 1,
            "constraint_name": "db1_schema1_example_pkey",
            "rely": "false",
            "comment": None,
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "EXAMPLE",
            "column_name": "NAME",
            "key_sequence": 1,
            "constraint_name": "db1_schema1_example_pkey",
            "rely": "false",
            "comment": None,
        },
    ]

    dcur.execute("show primary keys in schema db1.schema1")
    result2 = dcur.fetchall()
    assert result == result2

    # Assertion to sanity check that the above "in schema" filter isn't wrong, and in fact filters
    dcur.execute("show primary keys in schema db1.information_schema")
    result3 = dcur.fetchall()
    assert result3 == []


def test_split(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert indent(cur.execute("select split('a,b,c', ',')").fetchall()) == [('[\n  "a",\n  "b",\n  "c"\n]',)]


def test_sqlglot_regression(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute(
        """with SOURCE_TABLE AS (SELECT '2024-01-01' AS start_date)
            SELECT date(a.start_date) from SOURCE_TABLE AS a"""
    ).fetchone() == (datetime.date(2024, 1, 1),)


def test_sqlstate(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select 'hello world'")
    # sqlstate is None on success
    assert cur.sqlstate is None

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")

    assert cur.sqlstate == "42S02"


def test_sfqid(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.sfqid == "fakesnow"


def test_tags_noop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE table1 (id int)")
    cur.execute("ALTER TABLE table1 SET TAG foo='bar'")
    cur.execute("ALTER TABLE table1 MODIFY COLUMN name1 SET TAG foo='bar'")
    cur.execute("CREATE TAG cost_center COMMENT = 'cost_center tag'")


def test_to_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    # snowflake returns naive timestamps (ie: no timezone)
    cur.execute("SELECT to_timestamp(0)")
    assert cur.fetchall() == [(datetime.datetime(1970, 1, 1, 0, 0),)]

    cur.execute("SELECT to_timestamp('2013-04-05 01:02:03')")
    assert cur.fetchall() == [(datetime.datetime(2013, 4, 5, 1, 2, 3),)]

    cur.execute("SELECT to_timestamp_ntz('2013-04-05 01:02:03')")
    assert cur.fetchall() == [(datetime.datetime(2013, 4, 5, 1, 2, 3),)]


def test_timestamp_to_date(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        "SELECT to_date(to_timestamp(0)), to_date(cast(to_timestamp(0) as timestamp(9))), to_date('2024-01-26')"
    )
    assert cur.fetchall() == [(datetime.date(1970, 1, 1), datetime.date(1970, 1, 1), datetime.date(2024, 1, 26))]


def test_to_decimal(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/to_decimal#examples
    cur.execute("create or replace table number_conv(expr varchar);")
    cur.execute("insert into number_conv values ('12.3456'), ('98.76546');")
    cur.execute("select expr, to_decimal(expr),  to_number(expr, 10, 1), to_numeric(expr, 10, 8) from number_conv;")

    assert cur.fetchall() == [
        ("12.3456", 12, Decimal("12.3"), Decimal("12.34560000")),
        ("98.76546", 99, Decimal("98.8"), Decimal("98.76546000")),
    ]


def test_sha2(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/sha2#examples
    cur.execute(
        "select sha2('Snowflake') as a, sha2_hex('Snowflake') as b, sha2('Snowflake', 256) as c, sha2_hex('Snowflake', 256) as d;"
    )

    assert cur.fetchall() == [
        ("1dbd59f661d68b90724f21084396b865497173e4d2714f4d91cf05fa5fc5e18d",) * 4,
    ]


def test_try_parse_json(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("""SELECT TRY_PARSE_JSON('{"first":"foo", "last":"bar"}') AS j""")
    assert dindent(dcur.fetchall()) == [{"J": '{\n  "first": "foo",\n  "last": "bar"\n}'}]

    dcur.execute("""SELECT TRY_PARSE_JSON('{invalid: ,]') AS j""")
    assert dcur.fetchall() == [{"J": None}]


def test_try_to_decimal(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        "SELECT column1 AS orig_string, TRY_TO_DECIMAL(column1) AS dec, TRY_TO_DECIMAL(column1, 10, 2) AS dec_with_scale, TRY_TO_DECIMAL(column1, 4, 2) AS dec_with_range_err FROM VALUES ('345.123');"
    )
    assert cur.fetchall() == [
        (
            "345.123",
            Decimal("345"),
            Decimal("345.12"),
            None,
        ),
    ]


def test_transactions(conn: snowflake.connector.SnowflakeConnection):
    # test behaviours required for sqlalchemy

    conn.execute_string(
        """CREATE OR REPLACE TABLE table1 (i int);
            BEGIN TRANSACTION;
            INSERT INTO table1 (i) VALUES (1);"""
    )
    conn.rollback()
    conn.execute_string(
        """BEGIN TRANSACTION;
            INSERT INTO table1 (i) VALUES (2);"""
    )

    # transactions are per session, cursors are just different result sets,
    # so a new cursor will see the uncommitted values
    with conn.cursor() as cur:
        cur.execute("select * from table1")
        assert cur.fetchall() == [(2,)]

    conn.commit()

    with conn.cursor() as cur:
        # interleaved commit() doesn't lose result set because its on a different cursor
        cur.execute("select * from table1")
        conn.commit()
        assert cur.fetchall() == [(2,)]

    # check rollback and commit without transaction is a success (to mimic snowflake)
    # also check description can be retrieved, needed for ipython-sql/jupysql which runs description implicitly
    with conn.cursor() as cur:
        cur.execute("COMMIT")
        assert cur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
        assert cur.fetchall() == [("Statement executed successfully.",)]

        cur.execute("ROLLBACK")
        assert cur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]  # fmt: skip
        assert cur.fetchall() == [("Statement executed successfully.",)]


def test_trim_cast_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select trim(1), trim('  name 1  ')")
    assert cur.fetchall() == [("1", "name 1")]

    cur.execute("""select trim(parse_json('{"k1": "   v11  "}'):k1), trim(parse_json('{"k1": 21}'):k1)""")
    assert cur.fetchall() == [("v11", "21")]


def test_unquoted_identifiers_are_upper_cased(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table customers (id int, first_name varchar, last_name varchar)")
    dcur.execute("insert into customers values (1, 'Jenny', 'P')")
    dcur.execute("select first_name, first_name as fname from customers")

    assert dcur.fetchall() == [
        {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
    ]

    dcur.execute("select first_name, first_name as fname from customers")
    assert dcur.fetchall() == [
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
        cur.execute("select * from VALUES ('Amsterdam', 1), ('London', 2)")

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


def test_json_extract_cast_as_varchar(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE example (j VARIANT)")
    dcur.execute("""INSERT INTO example SELECT PARSE_JSON('{"str": "100", "num" : 200}')""")

    dcur.execute("SELECT j:str::varchar as j_str_varchar, j:num::varchar as j_num_varchar FROM example")
    assert dcur.fetchall() == [{"J_STR_VARCHAR": "100", "J_NUM_VARCHAR": "200"}]

    dcur.execute("SELECT j:str::number as j_str_number, j:num::number as j_num_number FROM example")
    assert dcur.fetchall() == [{"J_STR_NUMBER": 100, "J_NUM_NUMBER": 200}]

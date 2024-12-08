# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false

import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools
from snowflake.connector.cursor import ResultMetadata


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
        # TODO: is_nullable should be False for non-boolean columns
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
        # TODO: store actual size, ie: internal_size=20
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


def test_describe_view(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute(
        """
        create or replace table example (
            XVARCHAR VARCHAR
            -- ,XVARCHAR20 VARCHAR(20) -- TODO: preserve varchar size
        )
        """
    )

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
        {"name": "XVARCHAR", "type": "VARCHAR(16777216)", **common},
        # TODO: preserve varchar size
        # {"name": "XVARCHAR20", "type": "VARCHAR(20)", **common},
    ]

    dcur.execute("create view v1 as select * from example")
    assert dcur.execute("describe view v1").fetchall() == expected
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


def test_description_insert(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table example (x int)")
    dcur.execute("insert into example values (1), (2)")
    assert dcur.fetchall() == [{"number of rows inserted": 2}]
    # TODO: Snowflake is actually precision=19, is_nullable=False
    assert dcur.description == [ResultMetadata(name='number of rows inserted', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)]  # fmt: skip


def test_description_select(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("SELECT DATEDIFF( DAY, '2023-04-02'::DATE, '2023-04-05'::DATE) as days")
    assert dcur.fetchall() == [{"DAYS": 3}]
    # TODO: Snowflake is actually precision=9, is_nullable=False
    # fmt: off
    assert dcur.description == [
        ResultMetadata(name='DAYS', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
    ]
    # fmt: on


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


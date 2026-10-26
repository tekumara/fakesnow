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
    # for type_code mappings see indexes in FIELD_TYPES
    # https://github.com/snowflakedb/snowflake-connector-python/blob/2f346e490eb9e78e0c2e6c23ec55c3236fe4450b/src/snowflake/connector/constants.py#L126
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


def test_describe_timestamp_ltz_cast(cur: snowflake.connector.cursor.SnowflakeCursor):
    # duckdb has one type for both, snowflake describes them with their own type codes, see
    # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#label-python-connector-type-codes
    cur.execute("select '2026-10-26 01:02:03'::timestamp_ltz as l, '2026-10-26 01:02:03'::timestamp_tz as z")

    assert [(d.name, d.type_code) for d in cur.description] == [("L", 6), ("Z", 7)]


def test_describe_current_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/current_timestamp
    cur.execute("select current_timestamp as c, getdate() as g, systimestamp() as s")

    assert [(d.name, d.type_code) for d in cur.description] == [("C", 6), ("G", 6), ("S", 6)]


def test_describe_timestamp_ltz_column(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example (l timestamp_ltz, z timestamp_tz)")

    assert [(d.name, d.type_code) for d in cur.describe("select * from example")] == [("L", 6), ("Z", 7)]


def test_describe_table_timestamp_ltz(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example (l timestamp_ltz, z timestamp_tz)")

    cur.execute("describe table example")
    assert [(r[0], r[1]) for r in cur.fetchall()] == [("L", "TIMESTAMP_LTZ(9)"), ("Z", "TIMESTAMP_TZ(9)")]


def test_describe_table(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute(
        """
        create or replace table example (
            XBOOLEAN BOOLEAN, XDOUBLE DOUBLE, XFLOAT FLOAT,
            XNUMBER82 NUMBER(8,2), XNUMBER NUMBER, XDECIMAL DECIMAL, XNUMERIC NUMERIC,
            XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT,
            XVARCHAR20 VARCHAR(20), XVARCHAR VARCHAR, XTEXT TEXT,
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ TIMESTAMP_NTZ, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XTIMESTAMP_TZ TIMESTAMP_TZ, XDATE DATE, XTIME TIME,
            XBINARY BINARY, XARRAY ARRAY, XOBJECT OBJECT, XVARIANT VARIANT
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
        {"name": "XARRAY", "type": "ARRAY", **common},
        {"name": "XOBJECT", "type": "OBJECT", **common},
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
    # assert dcur.description == [ResultMetadata(name='status', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)]


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


def test_description_int128(dcur: snowflake.connector.cursor.DictCursor):
    # duckdb count_if returns int128 / hugeint
    dcur.execute("SELECT COUNT_IF(TRUE) AS n FROM (SELECT * FROM (VALUES (1), (2)))")

    # TODO: Snowflake is actually precision=13
    assert dcur.description == [ResultMetadata(name="N", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)]  # fmt: skip


def test_description_uint64(cur: snowflake.connector.cursor.DictCursor):
    cur.execute("""select array_size(parse_json('["a","b"]')) as c""")

    # TODO: Snowflake is actually precision=9
    assert cur.description == [ResultMetadata(name='C', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)]  # fmt: skip


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


@pytest.mark.parametrize(
    "query",
    [
        "select l from example",
        "select * from (select * from example)",
        "with c as (select * from example) select * from c",
        "select dateadd(day, 1, l) from example",
    ],
)
def test_describe_timestamp_ltz_projection(cur: snowflake.connector.cursor.SnowflakeCursor, query: str):
    cur.execute("create table example (l timestamp_ltz)")
    cur.execute(query)
    assert [d.type_code for d in cur.description] == [6]


def test_describe_timestamp_ltz_quoted(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""create table "MiXeD" ("l'Column" timestamp_ltz)""")
    cur.execute("""select "l'Column" from "MiXeD" """)
    assert [(d.name, d.type_code) for d in cur.description] == [("l'Column", 6)]


@pytest.mark.parametrize(
    "ddl,query",
    [
        ("create table if not exists example (l timestamp_tz)", "select l from example"),
        ("alter table example add column if not exists l timestamp_tz", "select l from example"),
        ("alter table example rename column l to m", "select m from example"),
        ("alter table example rename to renamed", "select l from renamed"),
    ],
)
def test_describe_timestamp_ltz_preserved(cur: snowflake.connector.cursor.SnowflakeCursor, ddl: str, query: str):
    cur.execute("create table example (l timestamp_ltz)")
    cur.execute(ddl)
    cur.execute(query)
    assert [d.type_code for d in cur.description] == [6]


def test_describe_timestamp_ltz_replaced(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example (l timestamp_ltz)")
    cur.execute("create or replace table example (l timestamp_tz)")
    cur.execute("select l from example")
    assert [d.type_code for d in cur.description] == [7]


def test_describe_timestamp_ltz_dropped_column(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example (l timestamp_ltz, n int)")
    cur.execute("alter table example drop column l")
    cur.execute("alter table example add column l timestamp_tz")
    cur.execute("select l from example")
    assert [d.type_code for d in cur.description] == [7]


@pytest.mark.parametrize("kind", ["table", "view"])
def test_describe_timestamp_ltz_created_as_cast(cur: snowflake.connector.cursor.SnowflakeCursor, kind: str):
    cur.execute(f"create {kind} example as select '2026-10-26'::timestamp_ltz as l")
    cur.execute("select l from example")
    assert [d.type_code for d in cur.description] == [6]


@pytest.mark.parametrize("stage", ["qualify", "annotate_types"])
@pytest.mark.parametrize("error", [ValueError, TypeError, AssertionError, KeyError])
def test_describe_timestamp_annotation_failure(stage: str, error: type[Exception]):
    # Fake-only: failed optional type inference must preserve DuckDB's fallback and the result set.
    from unittest.mock import patch

    import fakesnow

    with fakesnow.patch():
        cur = snowflake.connector.connect(database="db1", schema="s1").cursor()
        with patch(f"fakesnow.cursor.{stage}", side_effect=error("unsupported expression")):
            cur.execute("select '2026-10-26'::timestamp_ltz as l")
            assert [d.type_code for d in cur.description] == [7]
            row = cur.fetchone()
            assert row is not None
            assert row[0].isoformat() == "2026-10-26T00:00:00+00:00"

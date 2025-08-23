# ruff: noqa: E501

from datetime import datetime

import pytz
import snowflake.connector.cursor
from dirty_equals import IsDatetime
from snowflake.connector.cursor import ResultMetadata


def test_info_schema_table_comments(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_info_schema_columns_describe(cur: snowflake.connector.cursor.SnowflakeCursor):
    # test we can handle the column types returned from the info schema, which are created by duckdb
    # and so don't go through our transforms
    cur.execute("select column_name, ordinal_position from information_schema.columns")
    # fmt: off
    expected_metadata = [
        ResultMetadata(name='COLUMN_NAME', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='ORDINAL_POSITION', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)
    ]
    # fmt: on

    assert cur.description == expected_metadata


def test_describe_view_columns(dcur: snowflake.connector.cursor.DictCursor):
    cols = [
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
    dcur.execute("describe view information_schema.columns")
    result: list[dict] = dcur.fetchall()  # type: ignore
    assert list(result[0].keys()) == cols
    names = [r["name"] for r in result]
    # should contain snowflake-specific columns
    assert "COMMENT" in names
    # fmt: off
    assert dcur.description[:-1] == [
        ResultMetadata(name='name', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='type', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='kind', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='null?', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='default', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='primary key', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='unique key', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='check', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='expression', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='comment', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='policy name', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        # TODO: return correct type_code, see https://github.com/tekumara/fakesnow/issues/26
        # ResultMetadata(name='privacy domain', type_code=9, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True)
    ]
    # fmt: on


def test_info_schema_columns(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    dcur.execute("CREATE DATABASE db2")
    # should not be returned
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE TABLE db2.schema2.bar (id INTEGER)")

    dcur.execute(
        "SELECT table_catalog, table_schema, table_name, column_name FROM information_schema.columns where column_name = 'ID'"
    )

    assert dcur.fetchall() == [
        {
            "TABLE_CATALOG": "DB1",
            "TABLE_SCHEMA": "SCHEMA1",
            "TABLE_NAME": "FOO",
            "COLUMN_NAME": "ID",
        }
    ]


def test_info_schema_columns_numeric(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_info_schema_columns_other(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/data-types-datetime
    cur.execute(
        """
        create or replace table example (
            XTIMESTAMP TIMESTAMP, XTIMESTAMP_NTZ TIMESTAMP_NTZ, XTIMESTAMP_NTZ9 TIMESTAMP_NTZ(9), XTIMESTAMP_TZ TIMESTAMP_TZ, XDATE DATE, XTIME TIME,
            XBINARY BINARY, /* XARRAY ARRAY, XOBJECT OBJECT */ XVARIANT VARIANT
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
        ("XTIMESTAMP_NTZ", "TIMESTAMP_NTZ"),
        ("XTIMESTAMP_NTZ9", "TIMESTAMP_NTZ"),
        ("XTIMESTAMP_TZ", "TIMESTAMP_TZ"),
        ("XDATE", "DATE"),
        ("XTIME", "TIME"),
        ("XBINARY", "BINARY"),
        # TODO: support these types https://github.com/tekumara/fakesnow/issues/27
        # ("XARRAY", "ARRAY"),
        # ("XOBJECT", "OBJECT"),
        ("XVARIANT", "VARIANT"),
    ]


def test_info_schema_columns_text(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_info_schema_databases(dcur: snowflake.connector.cursor.DictCursor):
    # see https://docs.snowflake.com/en/sql-reference/info-schema/databases

    dcur.execute("create database db2")
    dcur.execute("select * from information_schema.databases")

    assert dcur.fetchall() == [
        {
            "DATABASE_NAME": "DB1",
            "DATABASE_OWNER": "SYSADMIN",
            "IS_TRANSIENT": "NO",
            "COMMENT": None,
            "CREATED": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "LAST_ALTERED": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "RETENTION_TIME": 1,
            "TYPE": "STANDARD",
        },
        {
            "DATABASE_NAME": "DB2",
            "DATABASE_OWNER": "SYSADMIN",
            "IS_TRANSIENT": "NO",
            "COMMENT": None,
            "CREATED": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "LAST_ALTERED": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "RETENTION_TIME": 1,
            "TYPE": "STANDARD",
        },
    ]


def test_info_schema_tables(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE foo (id INTEGER)")
    dcur.execute("INSERT INTO foo (id) VALUES (1)")
    dcur.execute("CREATE DATABASE db2")
    # should not be returned
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE TABLE db2.schema2.bar (name VARCHAR)")

    dcur.execute("SELECT * FROM information_schema.tables")

    assert dcur.fetchall() == [
        {
            "TABLE_CATALOG": "DB1",
            "TABLE_SCHEMA": "SCHEMA1",
            "TABLE_NAME": "FOO",
            "TABLE_OWNER": "SYSADMIN",
            "TABLE_TYPE": "BASE TABLE",
            "IS_TRANSIENT": "NO",
            "CLUSTERING_KEY": None,
            "ROW_COUNT": 1,
            "BYTES": 0,
            "RETENTION_TIME": 1,
            "SELF_REFERENCING_COLUMN_NAME": None,
            "REFERENCE_GENERATION": None,
            "USER_DEFINED_TYPE_CATALOG": None,
            "USER_DEFINED_TYPE_SCHEMA": None,
            "USER_DEFINED_TYPE_NAME": None,
            "IS_INSERTABLE_INTO": "YES",
            "IS_TYPED": "YES",
            "COMMIT_ACTION": None,
            "CREATED": IsDatetime(),
            "LAST_ALTERED": IsDatetime(),
            "LAST_DDL": IsDatetime(),
            "LAST_DDL_BY": "SYSADMIN",
            "AUTO_CLUSTERING_ON": "NO",
            "COMMENT": None,
            "IS_TEMPORARY": "NO",
            "IS_ICEBERG": "NO",
            "IS_DYNAMIC": "NO",
            "IS_IMMUTABLE": "NO",
            "IS_HYBRID": "NO",
        }
    ]


def test_info_schema_views_empty(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("SELECT * FROM information_schema.views")
    assert result
    assert result.fetchall() == []


def test_info_schema_views(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    dcur.execute("CREATE VIEW bar AS SELECT * FROM foo WHERE id > 5")
    dcur.execute("CREATE DATABASE db2")
    # should not be returned
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE TABLE db2.schema2.foo (id INTEGER, name VARCHAR)")
    dcur.execute("CREATE VIEW db2.schema2.baz AS SELECT * FROM db2.schema2.foo WHERE id > 5")

    dcur.execute("SELECT * FROM information_schema.views")

    assert dcur.fetchall() == [
        {
            "TABLE_CATALOG": "DB1",
            "TABLE_SCHEMA": "SCHEMA1",
            "TABLE_NAME": "BAR",
            "TABLE_OWNER": "SYSADMIN",
            "VIEW_DEFINITION": "CREATE VIEW SCHEMA1.BAR AS SELECT * FROM FOO WHERE (ID > 5);",
            "CHECK_OPTION": "NONE",
            "IS_UPDATABLE": "NO",
            "INSERTABLE_INTO": "NO",
            "IS_SECURE": "NO",
            "CREATED": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "LAST_ALTERED": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "LAST_DDL": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "LAST_DDL_BY": "SYSADMIN",
            "COMMENT": None,
        }
    ]


def test_type_column_is_not_null(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    for table in [
        "information_schema.databases",
        "information_schema.views",
        "information_schema.columns",
    ]:
        cur.execute(f"DESCRIBE VIEW {table}")
        result = cur.fetchall()
        data_types = [dt for (_, dt, *_) in result]
        nulls = [dt for dt in data_types if "NULL" in dt]
        assert not nulls

# ruff: noqa: E501

from datetime import datetime

import pytz
import snowflake.connector.cursor
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
        ResultMetadata(name='column_name', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='ordinal_position', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True)
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
    assert "comment" in names
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


def test_info_schema_columns(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
        cur.execute("CREATE DATABASE db2")
        # should not be returned
        cur.execute("CREATE SCHEMA db2.schema2")
        cur.execute("CREATE TABLE db2.schema2.bar (id INTEGER)")

        cur.execute(
            "SELECT table_catalog, table_schema, table_name, column_name FROM information_schema.columns where column_name = 'ID'"
        )

        assert cur.fetchall() == [
            {
                "table_catalog": "DB1",
                "table_schema": "SCHEMA1",
                "table_name": "FOO",
                "column_name": "ID",
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


def test_info_schema_databases(conn: snowflake.connector.SnowflakeConnection):
    # see https://docs.snowflake.com/en/sql-reference/info-schema/databases

    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create database db2")
        cur.execute("select * from information_schema.databases")

        assert cur.fetchall() == [
            {
                "database_name": "DB1",
                "database_owner": "SYSADMIN",
                "is_transient": "NO",
                "comment": None,
                "created": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
                "last_altered": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
                "retention_time": 1,
                "type": "STANDARD",
            },
            {
                "database_name": "DB2",
                "database_owner": "SYSADMIN",
                "is_transient": "NO",
                "comment": None,
                "created": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
                "last_altered": datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
                "retention_time": 1,
                "type": "STANDARD",
            },
        ]


def test_info_schema_tables(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("CREATE TABLE foo (id INTEGER)")
        cur.execute("CREATE DATABASE db2")
        # should not be returned
        cur.execute("CREATE SCHEMA db2.schema2")
        cur.execute("CREATE TABLE db2.schema2.bar (name VARCHAR)")

        cur.execute("SELECT table_catalog, table_schema, table_name FROM information_schema.tables")

        assert cur.fetchall() == [
            {
                "table_catalog": "DB1",
                "table_schema": "SCHEMA1",
                "table_name": "FOO",
            }
        ]


def test_info_schema_views_empty(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("SELECT * FROM information_schema.views")
    assert result
    assert result.fetchall() == []


def test_info_schema_views(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
        cur.execute("CREATE VIEW bar AS SELECT * FROM foo WHERE id > 5")
        cur.execute("CREATE DATABASE db2")
        # should not be returned
        cur.execute("CREATE SCHEMA db2.schema2")
        cur.execute("CREATE TABLE db2.schema2.foo (id INTEGER, name VARCHAR)")
        cur.execute("CREATE VIEW db2.schema2.baz AS SELECT * FROM db2.schema2.foo WHERE id > 5")

        cur.execute("SELECT * FROM information_schema.views")

        assert cur.fetchall() == [
            {
                "table_catalog": "DB1",
                "table_schema": "SCHEMA1",
                "table_name": "BAR",
                "table_owner": "SYSADMIN",
                "view_definition": "CREATE VIEW SCHEMA1.BAR AS SELECT * FROM FOO WHERE (ID > 5);",
                "check_option": "NONE",
                "is_updatable": "NO",
                "insertable_into": "NO",
                "is_secure": "NO",
                "created": datetime(1970, 1, 1, tzinfo=pytz.utc),
                "last_altered": datetime(1970, 1, 1, tzinfo=pytz.utc),
                "last_ddl": datetime(1970, 1, 1, tzinfo=pytz.utc),
                "last_ddl_by": "SYSADMIN",
                "comment": None,
            }
        ]


def test_type_column_is_not_null(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
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

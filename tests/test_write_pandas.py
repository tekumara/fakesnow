from __future__ import annotations

import datetime
import json

import pandas as pd
import pyarrow as pa
import pytest
import pytz
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools

from tests.utils import indent


def test_write_pandas_auto_create(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny"},
                {"ID": 2, "FIRST_NAME": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS", auto_create_table=True)

        cur.execute("select id, first_name from customers")

        assert cur.fetchall() == [(1, "Jenny"), (2, "Jasper")]


@pytest.mark.parametrize("use_logical_type", [None, False, True])
def test_write_pandas_auto_create_dtypes(conn: snowflake.connector.SnowflakeConnection, use_logical_type: bool | None):
    with conn.cursor() as cur:
        df = pd.DataFrame(
            {
                "INT32": pd.Series([1], dtype="int32"),
                "NULLABLE_INT64": pd.Series([1], dtype="Int64"),
                "UINT8": pd.Series([1], dtype="uint8"),
                "FLOAT32": pd.Series([1.5], dtype="float32"),
                "FLOAT64": pd.Series([1.5], dtype="float64"),
                "BOOL": pd.Series([True], dtype="bool"),
                "NULLABLE_BOOL": pd.Series([True], dtype="boolean"),
                "CATEGORY": pd.Series(["a"], dtype="category"),
                "CATEGORY_INT": pd.Series([1], dtype="category"),
                "CATEGORY_FLOAT": pd.Series([1.5], dtype="category"),
                "STRING": pd.Series(["a"], dtype="string"),
                "OBJECT": pd.Series(["a"], dtype="object"),
            }
        )
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=use_logical_type
        )

        cur.execute("describe table example")

        assert [(r[0], r[1]) for r in cur.fetchall()] == [
            ("INT32", "NUMBER(38,0)"),
            ("NULLABLE_INT64", "NUMBER(38,0)"),
            ("UINT8", "NUMBER(38,0)"),
            ("FLOAT32", "FLOAT"),
            ("FLOAT64", "FLOAT"),
            ("BOOL", "BOOLEAN"),
            ("NULLABLE_BOOL", "BOOLEAN"),
            ("CATEGORY", "VARCHAR(16777216)"),
            ("CATEGORY_INT", "NUMBER(38,0)"),
            ("CATEGORY_FLOAT", "FLOAT"),
            ("STRING", "VARCHAR(16777216)"),
            ("OBJECT", "VARCHAR(16777216)"),
        ]

        cur.execute("select * from example")
        assert cur.fetchall() == [(1, 1, 1, 1.5, 1.5, True, True, "a", 1, 1.5, "a", "a")]


def test_write_pandas_float16_null(conn: snowflake.connector.SnowflakeConnection):
    df = pd.DataFrame({"C": pd.Series([1.5, None], dtype="float16")})
    snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE", auto_create_table=True)

    with conn.cursor() as cur:
        cur.execute("describe table example")
        assert [(r[0], r[1]) for r in cur.fetchall()] == [("C", "BINARY(8388608)")]

        cur.execute("select * from example")
        assert cur.fetchall() == [(b"\x00>",), (None,)]


def test_write_pandas_auto_create_timestamps(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        at = pd.Timestamp("2025-01-01 12:00")
        df = pd.DataFrame(
            {
                # the precision of the staged column doesn't change what snowflake reads
                "NS": pd.Series([at]).astype("datetime64[ns]"),
                "US": pd.Series([at]).astype("datetime64[us]"),
                "S": pd.Series([at]).astype("datetime64[s]"),
            }
        )
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=True
        )

        cur.execute("describe table example")
        assert [(r[0], r[1]) for r in cur.fetchall()] == [
            ("NS", "TIMESTAMP_NTZ(9)"),
            ("US", "TIMESTAMP_NTZ(9)"),
            ("S", "TIMESTAMP_NTZ(9)"),
        ]

        cur.execute("select * from example")
        naive = datetime.datetime(2025, 1, 1, 12, 0)
        assert cur.fetchall() == [(naive, naive, naive)]


def test_write_pandas_auto_create_timestamp_tz(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        at = pd.Timestamp("2025-01-01 12:00", tz="UTC")
        df = pd.DataFrame({"TZ": pd.Series([at]).astype("datetime64[us, UTC]")})
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=True
        )

        cur.execute("select tz from example")
        assert cur.fetchall() == [(datetime.datetime(2025, 1, 1, 12, 0, tzinfo=pytz.utc),)]


@pytest.mark.xfail(reason="a timestamp_ltz column is described as timestamp_tz")
def test_write_pandas_auto_create_timestamp_tz_described_as_ltz(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        df = pd.DataFrame({"TZ": pd.Series([pd.Timestamp("2025-01-01 12:00", tz="UTC")])})
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=True
        )

        cur.execute("describe table example")
        assert [(r[0], r[1]) for r in cur.fetchall()] == [("TZ", "TIMESTAMP_LTZ(9)")]


def test_write_pandas_auto_create_time(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        at = datetime.time(12, 0)
        df = pd.DataFrame(
            {
                "US": pd.Series([at], dtype=pd.ArrowDtype(pa.time64("us"))),
                "NS": pd.Series([at], dtype=pd.ArrowDtype(pa.time64("ns"))),
            }
        )
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=True
        )

        cur.execute("describe table example")
        assert [(r[0], r[1]) for r in cur.fetchall()] == [("US", "TIME(9)"), ("NS", "TIME(9)")]

        cur.execute("select * from example")
        assert cur.fetchall() == [(at, at)]


def test_write_pandas_auto_create_category_of_timestamps(conn: snowflake.connector.SnowflakeConnection):
    # parquet stages a category as its category dtype, so USE_LOGICAL_TYPE decides this column too
    with conn.cursor() as cur:
        df = pd.DataFrame(
            {"TS": pd.Series([pd.Timestamp("2025-01-01 12:00")], dtype="datetime64[us]").astype("category")}
        )
        snowflake.connector.pandas_tools.write_pandas(
            conn, df, "EXAMPLE", auto_create_table=True, use_logical_type=True
        )

        cur.execute("describe table example")
        assert [(r[0], r[1]) for r in cur.fetchall()] == [("TS", "TIMESTAMP_NTZ(9)")]

        cur.execute("select * from example")
        assert cur.fetchall() == [(datetime.datetime(2025, 1, 1, 12, 0),)]


def test_write_pandas_auto_create_unsupported_dtype(conn: snowflake.connector.SnowflakeConnection):
    df = pd.DataFrame({"C": pd.Series([pd.Period("2025-01", freq="M")])})

    with pytest.raises(NotImplementedError, match="sql_type dtype=period"):
        snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE", auto_create_table=True)


def test_write_pandas_quoted_column_names(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as dcur:
        # colunmn names with spaces
        dcur.execute('create table customers (id int, "first name" varchar)')
        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "first name": "Jenny"},
                {"ID": 2, "first name": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS")

        dcur.execute("select * from customers")

        assert dcur.fetchall() == [
            {"ID": 1, "first name": "Jenny"},
            {"ID": 2, "first name": "Jasper"},
        ]


def test_write_pandas_array(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar, ORDERS array)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P", "ORDERS": ["A", "B"]},
                {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M", "ORDERS": ["C", "D"]},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS")

        cur.execute("select * from customers")

        assert indent(cur.fetchall()) == [
            (1, "Jenny", "P", '[\n  "A",\n  "B"\n]'),
            (2, "Jasper", "M", '[\n  "C",\n  "D"\n]'),
        ]


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


def test_write_pandas_partial_columns(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny"},
                {"ID": 2, "FIRST_NAME": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS")

        cur.execute("select id, first_name, last_name from customers")

        # columns not in dataframe will receive their default value
        assert cur.fetchall() == [(1, "Jenny", None), (2, "Jasper", None)]


def test_write_pandas_dict_as_varchar(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create or replace table example (vc varchar, o object)")

        df = pd.DataFrame([({"kind": "vc", "count": 1}, {"kind": "obj", "amount": 2})], columns=["VC", "O"])
        snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE")

        cur.execute("select * from example")

        # returned values are valid json strings
        # NB: snowflake orders object keys alphabetically, we don't
        r = cur.fetchall()
        assert [(sort_keys(r[0][0], indent=None), sort_keys(r[0][1], indent=2))] == [
            ('{"count":1,"kind":"vc"}', '{\n  "amount": 2,\n  "kind": "obj"\n}')
        ]


def test_write_pandas_dict_different_keys(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create or replace table customers (notes variant)")

        df = pd.DataFrame.from_records(
            [
                # rows have dicts with unique keys and values
                {"NOTES": {"k": "v1"}},
                # test single and double quoting
                {"NOTES": {"k2": ["v'2", 'v"3']}},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS")

        cur.execute("select * from customers")

        assert indent(cur.fetchall()) == [('{\n  "k": "v1"\n}',), ('{\n  "k2": [\n    "v\'2",\n    "v\\"3"\n  ]\n}',)]


def test_write_pandas_db_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create database db2")
        cur.execute("create schema db2.schema2")
        cur.execute("create or replace table db2.schema2.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny"},
                {"ID": 2, "FIRST_NAME": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS", "DB2", "SCHEMA2")

        cur.execute("select id, first_name, last_name from db2.schema2.customers")

        # columns not in dataframe will receive their default value
        assert cur.fetchall() == [(1, "Jenny", None), (2, "Jasper", None)]


def test_write_pandas_overwrite(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar)")

        df = pd.DataFrame.from_records([{"ID": 1, "FIRST_NAME": "Jenny"}])
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS")

        df2 = pd.DataFrame.from_records([{"ID": 2, "FIRST_NAME": "Jasper"}])
        snowflake.connector.pandas_tools.write_pandas(conn, df2, "CUSTOMERS", overwrite=True)

        cur.execute("select id, first_name from customers")

        # existing rows are replaced, not appended to
        assert cur.fetchall() == [(2, "Jasper")]


def test_write_pandas_overwrite_auto_create(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, AGE int)")

        df = pd.DataFrame.from_records([{"ID": 1, "AGE": 20}])
        snowflake.connector.pandas_tools.write_pandas(conn, df, "CUSTOMERS", auto_create_table=True)

        df2 = pd.DataFrame.from_records([{"ID": 2, "AGE": 30}])
        snowflake.connector.pandas_tools.write_pandas(conn, df2, "CUSTOMERS", auto_create_table=True, overwrite=True)

        cur.execute("select id, age from customers")

        # the table is dropped and recreated, so only the new rows remain
        assert cur.fetchall() == [(2, 30)]


def sort_keys(sdict: str, indent: int | None = 2) -> str:
    return json.dumps(
        json.loads(sdict, object_pairs_hook=lambda x: dict(sorted(x))),
        indent=indent,
        separators=None if indent else (",", ":"),
    )

from __future__ import annotations

import datetime
import json

import pandas as pd
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


def sort_keys(sdict: str, indent: int | None = 2) -> str:
    return json.dumps(
        json.loads(sdict, object_pairs_hook=lambda x: dict(sorted(x))),
        indent=indent,
        separators=None if indent else (",", ":"),
    )

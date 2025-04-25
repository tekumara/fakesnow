# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false
from __future__ import annotations

import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
from dirty_equals import IsUUID
from pandas.testing import assert_frame_equal
from snowflake.connector.cursor import ResultMetadata


def test_binding_pyformat(conn: snowflake.connector.SnowflakeConnection):
    # check pyformat is the default paramstyle
    assert snowflake.connector.paramstyle == "pyformat"
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (%s, %s, %s)", (1, "Jenny", True))
        cur.execute(
            "insert into customers values (%(id)s, %(name)s, %(active)s)", {"id": 2, "name": "Jasper", "active": False}
        )
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True), (2, "Jasper", False)]


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


def test_binding_conn_kwarg(_fakesnow: None):
    assert snowflake.connector.paramstyle == "pyformat"

    with (
        snowflake.connector.connect(database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
    ):
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (?, ?, ?)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_executemany(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

    customers = [(1, "Jenny", "P"), (2, "Jasper", "M")]
    cur.executemany("insert into customers (id, first_name, last_name) values (%s,%s,%s)", customers)

    cur.execute("select id, first_name, last_name from customers")
    assert cur.fetchall() == customers


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    *_, cur = conn.execute_string(
        """
        create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
        -- test comments are ignored
        select count(*) customers
        """
    )
    assert cur.fetchall() == [(1,)]


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

        # mimic jupysql fetchmany behaviour
        assert cur.fetchmany(2) == [(1, "Jenny", "P"), (2, "Jasper", "M")]
        assert cur.fetchmany(5) == [(3, "Jeremy", "K")]
        assert cur.fetchmany(5) == []

    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchmany(2) == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert cur.fetchmany(5) == [
            {"ID": 3, "FIRST_NAME": "Jeremy", "LAST_NAME": "K"},
        ]
        assert cur.fetchmany(5) == []


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
    # integers have dtype int64 (TODO: snowflake returns int8)
    assert_frame_equal(cur.fetch_pandas_all(), expected_df)

    # can refetch
    assert_frame_equal(cur.fetch_pandas_all(), expected_df)


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


def test_sqlstate(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select 'hello world'")
    # sqlstate is None on success
    assert cur.sqlstate is None

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")

    assert cur.sqlstate == "42S02"


def test_sfqid(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert not cur.sfqid
    cur.execute("select 1")
    assert cur.sfqid == IsUUID()


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

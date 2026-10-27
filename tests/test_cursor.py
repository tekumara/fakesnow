from __future__ import annotations

from typing import cast

import duckdb
import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
from dirty_equals import IsUUID
from pandas.testing import assert_frame_equal

import fakesnow
from fakesnow.cursor import FakeSnowflakeCursor


def test_unrelated_invalid_input_error(_fakesnow: None):
    # DuckDB-only diagnostic: unrelated errors must not be mistaken for our length checks.
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as conn,
        pytest.raises(duckdb.InvalidInputException, match="is too long and would be truncated"),
    ):
        conn.cursor().execute("select error('is too long and would be truncated')")


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
        cur.execute("insert into identifier(?) values (?, ?, ?)", ("CUSTOMERS", 1, "Jenny", True))
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


@pytest.mark.parametrize(
    "metadata_statement,metadata_arg",
    [
        # these statements add metadata to SUCCESS_NOP
        ("ALTER TABLE table1 ALTER id COMMENT 'column comment'", "col_comments"),
        ("COMMENT ON TABLE table1 IS 'table comment'", "table_comment"),
        ("ALTER TABLE table1 SET COMMENT = 'table comment'", "table_comment"),
        ("ALTER SESSION SET AUTOCOMMIT = FALSE", "set_autocommit"),
    ],
)
def test_success_nop_metadata_is_not_inherited(metadata_statement: str, metadata_arg: str):
    custom_args = {"col_comments", "table_comment", "set_autocommit"}

    with fakesnow.patch(nop_regexes=["^CALL.*"]):
        with snowflake.connector.connect(database="db1", schema="schema1") as conn, conn.cursor() as cur:
            fake_cur = cast(FakeSnowflakeCursor, cur)
            fake_cur.execute("create table table1 (id int)")

            fake_cur.execute(metadata_statement)
            metadata_result = fake_cur._last_transformed  # noqa: SLF001
            assert metadata_result is not None
            assert metadata_arg in metadata_result.args

        with snowflake.connector.connect() as conn, conn.cursor() as cur:
            # The skipped CALL produces a SUCCESS_NOP which must not inherit metadata from the prior
            # statement. Leaked table_comment metadata would cause and insert into the
            # information-schema and produce an AssertionError
            cur.execute("call this_procedure_does_not_exist('foo', 'bar);")
            assert cur.fetchall() == [("Statement executed successfully.",)]

            fake_cur = cast(FakeSnowflakeCursor, cur)
            nop_result = fake_cur._last_transformed  # noqa: SLF001
            assert nop_result is not None
            assert custom_args.isdisjoint(nop_result.args)


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


def test_execute_async_and_get_results_from_sfqid(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute_async("SELECT 1 AS col1, 2 AS col2")
    sfqid = cur.sfqid
    assert sfqid
    cur.get_results_from_sfqid(sfqid)
    assert cur.fetchall() == [(1, 2)]
    assert cur.description

    cur.execute_async("SELECT 1 AS col1, 2 AS col2 where 1=0")
    sfqid = cur.sfqid
    assert sfqid
    cur.get_results_from_sfqid(sfqid)
    assert cur.fetchall() == []
    assert cur.description


def test_get_results_from_sfqid_not_found(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.get_results_from_sfqid("00000000-0000-0000-0000-000000000000")
    with pytest.raises(
        snowflake.connector.errors.DatabaseError, match="Cannot retrieve data on the status of this query"
    ) as exc:
        cur.fetchall()
    assert exc.value.errno == -1

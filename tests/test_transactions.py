# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false, reportOptionalSubscript=false

from __future__ import annotations

import pytest
import snowflake.connector
from snowflake.connector.cursor import ResultMetadata


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


def test_autocommit_false(conn: snowflake.connector.SnowflakeConnection):
    conn.autocommit(False)

    # test rollback scenario: insert, commit, insert, rollback
    with conn.cursor() as cur:
        cur.execute("create or replace table rollback_test (id int)")
        cur.execute("insert into rollback_test values (1)")
        assert cur.execute("select sum(id) from rollback_test").fetchone() == (1,)
        conn.commit()

        cur.execute("insert into rollback_test values (2)")
        assert cur.execute("select sum(id) from rollback_test").fetchone() == (3,)
        conn.rollback()

        assert cur.execute("select sum(id) from rollback_test").fetchone() == (1,)

    # test implicit commit after DDL statement as per
    # https://docs.snowflake.com/en/sql-reference/transactions#ddl
    with conn.cursor(snowflake.connector.cursor.DictCursor) as dcur:
        dcur.execute("create or replace table implicit_ddl_commit (id int)")
        conn.rollback()

        # should exist after rollback
        assert (
            len(
                dcur.execute(
                    "SELECT * FROM information_schema.tables WHERE TABLE_NAME = 'IMPLICIT_DDL_COMMIT'"
                ).fetchall()
            )
            == 1
        )


def test_autocommit_false_mix_transaction_start(conn: snowflake.connector.SnowflakeConnection):
    conn.autocommit(False)

    with conn.cursor() as cur:
        cur.execute("create or replace table mix_transaction_start (id int)")

        # mix explicitly started transaction and implicit transaction started by DML statement
        cur.execute("begin transaction")
        cur.execute("insert into mix_transaction_start values (1)")

        conn.commit()
        conn.commit()

        # two implicit transactions started by DML statements
        cur.execute("insert into mix_transaction_start values (2)")
        cur.execute("insert into mix_transaction_start values (3)")

        assert cur.execute("select count(*) from mix_transaction_start").fetchone() == (3,)


def test_autocommit_false_implicit_transaction(conn: snowflake.connector.SnowflakeConnection):
    # An implicit BEGIN TRANSACTION is executed at:
    # The first DML statement after a transaction ends. This is true regardless of what ended the preceding transaction
    # (for example, a DDL statement, or an explicit COMMIT or ROLLBACK).
    # see https://docs.snowflake.com/en/sql-reference/transactions#autocommit
    #
    # this test ensures that an implicit transaction is NOT started by a SELECT (or when autocommit is set)
    # and so inserts will be visible from other connections (ie: sessions)
    #
    # this is what snowflake calls global read consistency see
    # https://docs.snowflake.com/en/sql-reference/transactions#read-consistency-across-sessions

    with (
        snowflake.connector.connect(database="db1", schema="schema1") as conn2,
        conn.cursor() as cur,
        conn2.cursor() as cur2,
    ):
        conn2.autocommit(False)

        cur.execute("create or replace table implicit_tx_test (id int)")
        cur.execute("insert into implicit_tx_test values (1)")

        # verify visible in other connection
        assert cur2.execute("select count(*) from implicit_tx_test").fetchone() == (1,)

        cur.execute("insert into implicit_tx_test values (2)")

        # verify visible in other connection
        assert cur2.execute("select count(*) from implicit_tx_test").fetchone() == (2,)


def test_autocommit_alter_session(conn: snowflake.connector.SnowflakeConnection):
    # tests that ALTER SESSION SET AUTOCOMMIT = FALSE commits the active transaction
    # as per https://docs.snowflake.com/en/sql-reference/transactions#autocommit
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE alter_autocommit (id int)")
        cur.execute("INSERT INTO alter_autocommit VALUES (1)")

        # This should trigger a commit
        cur.execute("ALTER SESSION SET AUTOCOMMIT = FALSE")

        # Now if we rollback, the row should still be there because ALTER SESSION committed it.
        conn.rollback()

        assert cur.execute("SELECT count(*) FROM alter_autocommit").fetchone()[0] == 1

        cur.execute("ALTER SESSION SET AUTOCOMMIT = TRUE")

        # Verify in autocommit mode (transaction commits immediately)
        cur.execute("INSERT INTO alter_autocommit VALUES (2)")
        conn.rollback()  # Should be no-op and not rollback the insert

        assert cur.execute("SELECT count(*) FROM alter_autocommit").fetchone()[0] == 2


def test_autocommit_create_stage(conn: snowflake.connector.SnowflakeConnection):
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE table1 (id int)")
        # create stage should trigger a commit
        cur.execute("CREATE STAGE stage1")
        # this should succeed
        cur.execute("INSERT INTO table1 VALUES (1)")


def test_conn_autocommit_false(_fakesnow: None):
    # create table upfront
    with snowflake.connector.connect(database="db1", schema="schema1") as conn, conn.cursor() as cur:
        cur.execute("create or replace table rollback_test (id int)")

    with snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn, conn.cursor() as cur:
        # an implicit transaction should be started
        cur.execute("insert into rollback_test values (1)")
        conn.rollback()

        assert cur.execute("select count(*) from rollback_test").fetchone() == (0,)


def test_conn_context_exit_commits(_fakesnow: None):
    with (
        snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("CREATE OR REPLACE TABLE context_exit (id int)")
        cur.execute("INSERT INTO context_exit VALUES (1)")

    with snowflake.connector.connect(database="db1", schema="schema1") as conn:
        assert conn.cursor().execute("SELECT count(*) FROM context_exit").fetchone() == (1,)


def test_conn_context_exit_with_exception_rolls_back(_fakesnow: None):
    with (
        pytest.raises(snowflake.connector.errors.ProgrammingError) as _,
        snowflake.connector.connect(database="db1", schema="schema1", autocommit=False) as conn,
        conn.cursor() as cur,
    ):
        cur.execute("CREATE OR REPLACE TABLE context_exit (id int)")
        cur.execute("INSERT INTO context_exit VALUES (1)")

        # throws exception
        cur.execute("SELECT * FROM this_table_does_not_exist")

    with snowflake.connector.connect(database="db1", schema="schema1") as conn:
        assert conn.cursor().execute("SELECT count(*) FROM context_exit").fetchone() == (0,)

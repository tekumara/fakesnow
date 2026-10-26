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


def test_autocommit_create_file_format_does_not_commit_active_transaction(
    conn: snowflake.connector.SnowflakeConnection,
):
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE file_format_transaction_test (id int)")
        cur.execute("INSERT INTO file_format_transaction_test VALUES (1)")
        cur.execute("CREATE FILE FORMAT file_format_transaction_test TYPE='CSV'")
        conn.rollback()

        assert cur.execute("SELECT count(*) FROM file_format_transaction_test").fetchone() == (0,)
        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="already exists"):
            cur.execute("CREATE FILE FORMAT file_format_transaction_test TYPE='CSV'")


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


@pytest.mark.parametrize("failing_sql", ["DESC TABLE missing_table", "SELECT * FROM missing_table"])
def test_autocommit_false_failed_statement_keeps_transaction(
    conn: snowflake.connector.SnowflakeConnection, failing_sql: str
):
    # a failed statement rolls back only its own changes, the transaction stays active and can be committed
    # see https://docs.snowflake.com/en/sql-reference/transactions#failed-statements-within-a-transaction
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE failed_statement (id int)")
        cur.execute("INSERT INTO failed_statement VALUES (1)")

        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute(failing_sql)

        assert cur.execute("SELECT count(*) FROM failed_statement").fetchone() == (1,)
        conn.commit()
        conn.rollback()
        assert cur.execute("SELECT count(*) FROM failed_statement").fetchone() == (1,)


def test_autocommit_false_runtime_failure_commit_raises(conn: snowflake.connector.SnowflakeConnection):
    # Deviates from Snowflake, which rolls back only the failed statement and commits the earlier insert, see
    # https://docs.snowflake.com/en/sql-reference/transactions#failed-statements-within-a-transaction
    # DuckDB has no savepoints, so a statement that fails at runtime aborts the whole transaction. Rather than
    # commit silently dropping the earlier insert, fakesnow raises and requires an explicit rollback.
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE OR REPLACE TABLE runtime_failure (id int)")
        cur.execute("INSERT INTO runtime_failure VALUES (1)")

        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("INSERT INTO runtime_failure VALUES ('x')")

        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="transaction was aborted"):
            conn.commit()
        conn.rollback()
        assert cur.execute("SELECT count(*) FROM runtime_failure").fetchone() == (0,)


@pytest.mark.parametrize(
    "commit_sql",
    [
        "COMMIT",
        "CREATE TABLE next_table (id int)",
        "ALTER SESSION SET AUTOCOMMIT = TRUE",
        "ALTER SESSION SET AUTOCOMMIT = FALSE",
    ],
)
def test_runtime_failure_sql_commit_raises(conn: snowflake.connector.SnowflakeConnection, commit_sql: str):
    # Deviates from Snowflake: DuckDB cannot preserve earlier writes after a runtime failure.
    # Every explicit or implicit commit must report that loss instead of claiming success.
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE runtime_sql_commit (id int)")
        cur.execute("INSERT INTO runtime_sql_commit VALUES (1)")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("INSERT INTO runtime_sql_commit VALUES ('x')")

        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="transaction was aborted") as excinfo:
            cur.execute(commit_sql)
        assert (excinfo.value.errno, excinfo.value.sqlstate) == (9999, "99999")
        conn.rollback()
        assert cur.execute("SELECT count(*) FROM runtime_sql_commit").fetchone() == (0,)


@pytest.mark.parametrize("mode", [False, True])
def test_runtime_failure_autocommit_raises(conn: snowflake.connector.SnowflakeConnection, mode: bool):
    # Deviates from Snowflake: changing AUTOCOMMIT cannot preserve writes after a DuckDB runtime failure.
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE runtime_autocommit (id int)")
        cur.execute("INSERT INTO runtime_autocommit VALUES (1)")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("INSERT INTO runtime_autocommit VALUES ('x')")

        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="transaction was aborted"):
            conn.autocommit(mode)
        conn.rollback()
        assert cur.execute("SELECT count(*) FROM runtime_autocommit").fetchone() == (0,)


def test_runtime_failure_rollback(conn: snowflake.connector.SnowflakeConnection):
    conn.autocommit(False)
    with conn.cursor() as cur:
        cur.execute("CREATE TABLE runtime_rollback (id int)")
        cur.execute("INSERT INTO runtime_rollback VALUES (1)")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("INSERT INTO runtime_rollback VALUES ('x')")

        conn.rollback()
        cur.execute("INSERT INTO runtime_rollback VALUES (2)")
        conn.commit()
        assert cur.execute("SELECT * FROM runtime_rollback").fetchall() == [(2,)]


def test_context_exit_failed_commit_closes_connection(_fakesnow: None):
    # Deviates from Snowflake: the commit fails after a runtime error, but context exit must still close.
    conn = snowflake.connector.connect(database="db1", schema="schema1", autocommit=False)
    with (
        pytest.raises(snowflake.connector.errors.ProgrammingError, match="transaction was aborted"),
        conn,
        conn.cursor() as cur,
    ):
        cur.execute("CREATE TABLE runtime_context_exit (id int)")
        cur.execute("INSERT INTO runtime_context_exit VALUES (1)")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur.execute("INSERT INTO runtime_context_exit VALUES ('x')")

    assert conn.is_closed()


def test_commit_closed_connection(conn: snowflake.connector.SnowflakeConnection):
    conn.close()
    with pytest.raises(snowflake.connector.errors.DatabaseError) as excinfo:
        conn.commit()
    assert (excinfo.value.errno, excinfo.value.sqlstate) == (250002, "08003")

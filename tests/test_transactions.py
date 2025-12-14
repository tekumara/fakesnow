# ruff: noqa: E501

from __future__ import annotations

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

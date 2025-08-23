import re

import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms import create_table_autoincrement, sequence_nextval


def test_autoincrement(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT, name VARCHAR)")
    cur.execute("insert into test_table(name) values ('foo'), ('bar')")
    cur.execute("select * from test_table")
    assert cur.fetchall() == [(1, "foo"), (2, "bar")]

    # recreate the table with a different sequence
    cur.execute(
        "CREATE or replace TABLE test_table(id NUMERIC NOT NULL IDENTITY start 10 increment 5 order, name VARCHAR)"
    )
    cur.execute("insert into test_table(name) values ('foo'), ('bar')")
    cur.execute("select * from test_table")
    assert cur.fetchall() == [(10, "foo"), (15, "bar")]


def test_autoincrement_transform() -> None:
    expr = sqlglot.parse_one(
        "create table test_table (id numeric autoincrement start 10 increment 5 order)",
        dialect="snowflake",
    )
    seq_stmt, table_stmt = create_table_autoincrement(expr)
    seq_sql = seq_stmt.sql()
    table_sql = table_stmt.sql()

    m_seq = re.search(r"CREATE SEQUENCE (\S+) START WITH 10 INCREMENT BY 5", seq_sql)
    assert m_seq, f"Unexpected SEQUENCE SQL: {seq_sql}"
    seq_name = m_seq.group(1)
    assert re.match(r"^_fs_seq_test_table_id_[0-9a-f]{8}$", seq_name)

    assert table_sql == f"CREATE TABLE test_table (id DECIMAL(38, 0) DEFAULT NEXTVAL('{seq_name}'))"


def test_sequence(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE OR REPLACE SEQUENCE seq_01 START WITH 1 INCREMENT BY 1")
    assert dcur.fetchall() == [{"status": "Sequence SEQ_01 successfully created."}]

    dcur.execute("SELECT seq_01.nextval, seq_01.nextval again")
    assert dcur.fetchall() == [{"NEXTVAL": 1, "AGAIN": 2}]


def test_sequence_nextval_transform() -> None:
    assert (
        sqlglot.parse_one("SELECT seq_01.nextval").transform(sequence_nextval).sql()
        == "SELECT NEXTVAL('seq_01') AS NEXTVAL"
    )
    assert (
        sqlglot.parse_one("select seq_01.nextval again").transform(sequence_nextval).sql()
        == "SELECT NEXTVAL('seq_01') AS again"
    )


def test_sequence_with_increment(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE OR REPLACE SEQUENCE seq_5 START = 10 INCREMENT = 5")
    dcur.execute("SELECT seq_5.nextval a, seq_5.nextval b, seq_5.nextval c, seq_5.nextval d")
    assert dcur.fetchall() == [{"A": 10, "B": 15, "C": 20, "D": 25}]

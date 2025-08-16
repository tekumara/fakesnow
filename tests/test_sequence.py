import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms import sequence_nextval


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

import snowflake.connector

conn_info = dict(
    user="XXXX",
    password="YYYY",
    account="ZZZZ",
)


def test_example(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists jaffles")
        cur.execute("drop table if exists customers")
        cur.execute("create table jaffles (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into jaffles values (1, 'Jenny', 'P')")
        cur.execute("insert into jaffles values (2, 'Jasper', 'M')")

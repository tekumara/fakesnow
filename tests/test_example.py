import pytest
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
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        result = cur.fetchall()

        assert result == [(1, "Jenny", "P"), (2, "Jasper", "M")]

def test_non_existant_table_throws_snowflake_exception(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("select * from this_table_does_not_exist")

def test_create_fully_qualified_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema if not exists staging.jaffles")

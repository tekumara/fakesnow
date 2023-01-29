import pytest
import snowflake.connector

conn_info = dict(
    user="XXXX",
    password="YYYY",
    account="ZZZZ",
)


def test_example(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        cur.execute("select id, first_name, last_name from customers")
        result = cur.fetchall()

        assert result == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_qualified_with_database(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create schema marts.jaffles")
        cur.execute("create table marts.jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")


def test_uses_connection_database_and_schema(conn: snowflake.connector.SnowflakeConnection):

    with conn.cursor() as cur:
        cur.execute("create schema marts.jaffles")

    with snowflake.connector.connect(database="marts", schema="jaffles") as conn:
        with conn.cursor() as cur:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
            cur.execute("insert into customers values (1, 'Jenny', 'P')")
            cur.execute("insert into customers values (2, 'Jasper', 'M')")
            cur.execute("select id, first_name, last_name from marts.jaffles.customers")
            result = cur.fetchall()

            assert result == [(1, "Jenny", "P"), (2, "Jasper", "M")]

def test_use_schema(conn: snowflake.connector.SnowflakeConnection):

    with snowflake.connector.connect() as conn:
        with conn.cursor() as cur:
            cur.execute("create schema jaffles")
            cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
            cur.execute("use schema jaffles")
            cur.execute("insert into customers values (1, 'Jenny', 'P')")

def test_non_existant_table_throws_snowflake_exception(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("select * from this_table_does_not_exist")


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    [_, cur2] = conn.execute_string(
        """ create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
            select count(*) customers """
    )
    assert [(1,)] == cur2.fetchall()

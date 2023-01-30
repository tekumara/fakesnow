import pytest
import snowflake.connector
import snowflake.connector.cursor


def test_fetchall(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_fetchall_dict_cursor(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]


def test_fetchone(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == (1, "Jenny", "P")
        assert cur.fetchone() == (2, "Jasper", "M")
        assert not cur.fetchone()

def test_fetchone_dict_cursor(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchone() == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
        ]
        assert cur.fetchone() == [
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert not cur.fetchone()


def test_connect_with_database_and_schema(conn: snowflake.connector.SnowflakeConnection):
    # connect without default database and schema
    with snowflake.connector.connect() as conn1:
        with conn1.cursor() as cur:
            # use the table's fully qualified name
            cur.execute("create schema marts.jaffles")
            cur.execute("create table marts.jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
            cur.execute("insert into marts.jaffles.customers values (1, 'Jenny', 'P')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2:
        with conn2.cursor() as cur:
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

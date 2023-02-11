import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools


def test_describe(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute(
            "create table customers (ID int, CNAME varchar, AMOUNT decimal(10,2), PCT real, UPDATE_AT timestamp)"
        )
        metadata = cur.describe("select * from customers")

        # fmt: off
        assert metadata == [
            snowflake.connector.cursor.ResultMetadata(
                name="ID", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True              # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="CNAME", type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True, # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="AMOUNT", type_code=0, display_size=None, internal_size=None, precision=10, scale=2, is_nullable=True,         # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name="PCT", type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,       # type: ignore # noqa: E501
            ),
            snowflake.connector.cursor.ResultMetadata(
                name='UPDATE_AT', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True        # type: ignore # noqa: E501
            ),
        ]
        # fmt: on


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


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    [_, cur2] = conn.execute_string(
        """ create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
            select count(*) customers """
    )
    assert [(1,)] == cur2.fetchall()


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


def test_get_result_batches(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [(1, "Jenny", "P"), (2, "Jasper", "M")]
        assert sum(batch.rowcount for batch in batches) == 2


def test_get_result_batches_dict(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")
        cur.execute("select id, first_name, last_name from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
        assert sum(batch.rowcount for batch in batches) == 2


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


def test_write_pandas(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
                {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "customers")

        cur.execute("select id, first_name, last_name from customers")

        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]

import datetime
import json

import pandas as pd
import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.pandas_tools
from pandas.testing import assert_frame_equal


def test_binding_default_paramstyle(conn: snowflake.connector.SnowflakeConnection):
    assert conn._paramstyle == "pyformat"  # noqa: SLF001
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (%s, %s, %s)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_binding_qmark(conn: snowflake.connector.SnowflakeConnection):
    conn._paramstyle = "qmark"  # noqa: SLF001
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
        cur.execute("insert into customers values (?, ?, ?)", (1, "Jenny", True))
        cur.execute("select * from customers")
        assert cur.fetchall() == [(1, "Jenny", True)]


def test_connect_auto_create(_fakesnow: None):
    with snowflake.connector.connect(database="db1", schema="schema1"):
        # creates db2 and schema1
        pass

    with snowflake.connector.connect(database="db1", schema="schema1"):
        # connects again and reuses db1 and schema1
        pass


def test_connect_without_database(_fakesnow_no_auto_create: None):
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM customers")

        # actual snowflake error message is:
        #
        # 002003 (42S02): SQL compilation error:
        # Object 'CUSTOMERS' does not exist or not authorized.
        # assert (
        #     "002003 (42S02): Catalog Error: Table with name customers does not exist!"
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM jaffles.customers")

        assert (
            "090105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create schema jaffles")

        assert (
            "090105 (22000): Cannot perform CREATE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("use schema jaffles")

        # assert (
        #     "002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        assert (
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )


def test_connect_without_schema(_fakesnow: None):
    # database will be created but not schema
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        assert not conn.schema

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("SELECT * FROM customers")

        # actual snowflake error message is:
        #
        # 002003 (42S02): SQL compilation error:
        # Object 'CUSTOMERS' does not exist or not authorized.
        # assert (
        #     "002003 (42S02): Catalog Error: Table with name customers does not exist!"
        #     in str(excinfo.value)
        # )

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        conn.execute_string("CREATE SCHEMA schema1; USE SCHEMA schema1;")
        assert conn.schema == "SCHEMA1"


def test_connect_different_sessions_use_database(_fakesnow_no_auto_create: None):
    # connect without default database and schema
    with snowflake.connector.connect() as conn1, conn1.cursor() as cur:
        # use the table's fully qualified name
        cur.execute("create database marts")
        cur.execute("create schema marts.jaffles")
        cur.execute("create table marts.jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
        cur.execute("insert into marts.jaffles.customers values (1, 'Jenny', 'P')")

        # use database and schema
        cur.execute("use database marts")
        cur.execute("use schema jaffles")
        cur.execute("insert into customers values (2, 'Jasper', 'M')")

    # in a separate connection, connect using the database and schema from above
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn2, conn2.cursor() as cur:
        cur.execute("select id, first_name, last_name from customers")
        assert cur.fetchall() == [(1, "Jenny", "P"), (2, "Jasper", "M")]


def test_connect_with_non_existent_db_or_schema(_fakesnow_no_auto_create: None):
    # can connect with db that doesn't exist
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        # but no valid database set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090105 (22000): Cannot perform CREATE TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        # database still present on connection
        assert conn.database == "MARTS"

        cur.execute("CREATE database marts")

    # can connect with schema that doesn't exist
    with snowflake.connector.connect(database="marts", schema="jaffles") as conn, conn.cursor() as cur:
        # but no valid schema set
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )

        # schema still present on connection
        assert conn.schema == "JAFFLES"


def test_current_database_schema(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select current_database(), current_schema()")

        assert cur.fetchall() == [
            {"current_database()": "DB1", "current_schema()": "SCHEMA1"},
        ]


def test_describe(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        create table customers (
            ID int, CNAME varchar, AMOUNT decimal(10,2), PCT real, ACTIVE boolean,
            UPDATE_AT timestamp, UPDATE_AT_NTZ timestamp_ntz(9), INSERTIONDATE DATE
        )
        """
    )
    # fmt: off
    expected_metadata = [
        snowflake.connector.cursor.ResultMetadata(
            name="ID", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True                  # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="CNAME", type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True,     # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="AMOUNT", type_code=0, display_size=None, internal_size=None, precision=10, scale=2, is_nullable=True,             # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="PCT", type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,           # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="ACTIVE", type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,       # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='UPDATE_AT', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True            # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='UPDATE_AT_NTZ', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True        # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='INSERTIONDATE', type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True  # type: ignore # noqa: E501
        ),
    ]
    # fmt: on

    assert cur.describe("select * from customers") == expected_metadata

    cur.execute("select * from customers")
    assert cur.description == expected_metadata


def test_describe_with_params(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        create table customers (
            ID int, CNAME varchar, AMOUNT decimal(10,2), PCT real, ACTIVE boolean,
            UPDATE_AT timestamp, UPDATE_AT_NTZ timestamp_ntz(9), INSERTIONDATE DATE
        )
        """
    )
    # fmt: off
    expected_metadata = [
        snowflake.connector.cursor.ResultMetadata(
            name="ID", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True                  # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="CNAME", type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True,     # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="AMOUNT", type_code=0, display_size=None, internal_size=None, precision=10, scale=2, is_nullable=True,             # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="PCT", type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,           # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name="ACTIVE", type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True,       # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='UPDATE_AT', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True            # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='UPDATE_AT_NTZ', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True        # type: ignore # noqa: E501
        ),
        snowflake.connector.cursor.ResultMetadata(
            name='INSERTIONDATE', type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True  # type: ignore # noqa: E501
        ),
    ]
    # fmt: on

    assert cur.describe("select * from customers where id = ?", (1,)) == expected_metadata

    cur.execute("select * from customers where id = ?", (1,))
    assert cur.description == expected_metadata


def test_executemany(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

    customers = [(1, "Jenny", "P"), (2, "Jasper", "M")]
    cur.executemany("insert into customers (id, first_name, last_name) values (%s,%s,%s)", customers)

    cur.execute("select id, first_name, last_name from customers")
    assert cur.fetchall() == customers


def test_execute_string(conn: snowflake.connector.SnowflakeConnection):
    [_, cur2] = conn.execute_string(
        """ create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar);
            select count(*) customers """
    )
    assert [(1,)] == cur2.fetchall()


def test_fetchall(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_fetchone(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_fetch_pandas_all(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    cur.execute("insert into customers values (1, 'Jenny', 'P')")
    cur.execute("insert into customers values (2, 'Jasper', 'M')")
    cur.execute("select id, first_name, last_name from customers")

    expected_df = pd.DataFrame.from_records(
        [
            {"ID": 1, "FIRST_NAME": "Jenny", "LAST_NAME": "P"},
            {"ID": 2, "FIRST_NAME": "Jasper", "LAST_NAME": "M"},
        ]
    )
    assert_frame_equal(cur.fetch_pandas_all(), expected_df, check_dtype=False)


def test_get_result_batches(cur: snowflake.connector.cursor.SnowflakeCursor):
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


def test_non_existent_table_throws_snowflake_exception(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")


def test_object_construct(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null)")

    # TODO: strip null within duckdb via python UDF
    def strip_none_values(d: dict) -> dict:
        return {k: v for k, v in d.items() if v}

    result = cur.fetchone()
    assert isinstance(result, tuple)
    assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1,\n  "b": "BBBB"\n}')


def test_regex(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select regexp_replace('abc123', '\\\\D', '')")
    assert cur.fetchone() == ("123",)


def test_schema_create_and_use(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema jaffles")
    cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    cur.execute("use schema jaffles")
    # fully qualified works too
    cur.execute("use schema db1.jaffles")
    cur.execute("insert into customers values (1, 'Jenny', 'P')")


def test_schema_drop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create schema jaffles")
    cur.execute("create table jaffles.customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")
    # dropping schema drops its contents
    cur.execute("drop schema jaffles")


def test_semi_structured_types(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table semis (emails array, name object, notes variant)")
    cur.execute(
        """insert into semis(emails, name, notes) SELECT [1, 2], parse_json('{"k": "v1"}'), parse_json('["foo"]')"""
    )
    cur.execute(
        """insert into semis(emails, name, notes) VALUES ([3,4], parse_json('{"k": "v2"}'), parse_json('["bar"]'))"""  # noqa: E501
    )

    cur.execute("select emails[0] from semis")
    # returned as strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)
    assert cur.fetchall() == [("1",), ("3",)]

    cur.execute("select name['k'] from semis")
    # returned as json strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)
    assert cur.fetchall() == [('"v1"',), ('"v2"',)]


def test_table_comments(cur: snowflake.connector.cursor.SnowflakeCursor):
    def read_comment() -> str:
        cur.execute(
            """SELECT COALESCE(COMMENT, '') FROM INFORMATION_SCHEMA.TABLES
                    WHERE TABLE_NAME = 'INGREDIENTS' AND TABLE_SCHEMA = 'SCHEMA1' LIMIT 1"""
        )
        return cur.fetchall()[0][0]

    cur.execute("CREATE TABLE ingredients (id int) COMMENT = 'cheese'")
    assert read_comment() == "cheese"
    cur.execute("COMMENT ON TABLE ingredients IS 'pepperoni'")
    assert read_comment() == "pepperoni"
    cur.execute("COMMENT IF EXISTS ON TABLE schema1.ingredients IS 'mushrooms'")
    assert read_comment() == "mushrooms"


def test_tags_noop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE table1 (id int)")
    cur.execute("ALTER TABLE table1 SET TAG foo='bar'")
    cur.execute("ALTER TABLE table1 MODIFY COLUMN name1 SET TAG foo='bar'")


def test_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT to_timestamp(0)")
    assert cur.fetchall() == [(datetime.datetime(1970, 1, 1, 0, 0),)]


def test_timestamp_to_date(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT to_date(to_timestamp(0)), to_date(cast(to_timestamp(0) as timestamp(9)))")
    assert cur.fetchall() == [(datetime.date(1970, 1, 1), datetime.date(1970, 1, 1))]


def test_unquoted_identifiers_are_upper_cased(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("create table customers (id int, first_name varchar, last_name varchar)")
        cur.execute("insert into customers values (1, 'Jenny', 'P')")
        cur.execute("select first_name, first_name as fname from customers")

        assert cur.fetchall() == [
            {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
        ]

        cur.execute("select first_name, first_name as fname from customers")
        batches = cur.get_result_batches()
        assert batches

        rows = [row for batch in batches for row in batch]
        assert rows == [
            {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
        ]


def test_use_invalid_schema(_fakesnow: None):
    # database will be created but not schema
    with snowflake.connector.connect(database="marts") as conn, conn.cursor() as cur:
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
            cur.execute("use schema this_does_not_exist")

        # assert (
        #     "002043 (02000): SQL compilation error:\nObject does not exist, or operation cannot be performed."
        #     in str(excinfo.value)
        # )

        # invalid schema doesn't get set on the connection
        assert not conn.schema

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute("create table foobar (i int)")

        assert (
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."  # noqa: E501
            in str(excinfo.value)
        )


def test_values(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("SELECT * FROM VALUES ('Amsterdam', 1), ('London', 2)")

        assert cur.fetchall() == [
            {"column1": "Amsterdam", "column2": 1},
            {"column1": "London", "column2": 2},
        ]

        cur.execute("SELECT column2, column1 FROM VALUES ('Amsterdam', 1), ('London', 2)")

        assert cur.fetchall() == [
            {"column2": 1, "column1": "Amsterdam"},
            {"column2": 2, "column1": "London"},
        ]


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


def test_write_pandas_partial_columns(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table customers (ID int, FIRST_NAME varchar, LAST_NAME varchar)")

        df = pd.DataFrame.from_records(
            [
                {"ID": 1, "FIRST_NAME": "Jenny"},
                {"ID": 2, "FIRST_NAME": "Jasper"},
            ]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "customers")

        cur.execute("select id, first_name, last_name from customers")

        # columns not in dataframe will receive their default value
        assert cur.fetchall() == [(1, "Jenny", None), (2, "Jasper", None)]


def test_write_pandas_dict_column_as_varchar(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("create table example (id str, vc varchar, o object)")

        df = pd.DataFrame(
            [("abc", {"kind": "vc", "count": 1}, {"kind": "obj", "amount": 2})], columns=["ID", "VC", "O"]
        )
        snowflake.connector.pandas_tools.write_pandas(conn, df, "EXAMPLE")

        cur.execute("select * from example")

        # returned values are valid json strings
        # TODO: order object keys alphabetically like snowflake does
        assert cur.fetchall() == [("abc", '{"kind":"vc","count":1}', '{"kind":"obj","amount":2}')]

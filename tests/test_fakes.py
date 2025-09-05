# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false

from __future__ import annotations

import datetime
import re
import tempfile
from decimal import Decimal

import pytest
import snowflake.connector
import snowflake.connector.cursor
from snowflake.connector.errors import ProgrammingError

import fakesnow
from tests.utils import dindent, indent


def test_alias_on_join(conn: snowflake.connector.SnowflakeConnection):
    *_, cur = conn.execute_string(
        """
        CREATE OR REPLACE TEMPORARY TABLE TEST (COL VARCHAR);
        INSERT INTO TEST (COL) VALUES ('VARCHAR1'), ('VARCHAR2');
        CREATE OR REPLACE TEMPORARY TABLE JOINED (COL VARCHAR, ANOTHER VARCHAR);
        INSERT INTO JOINED (COL, ANOTHER) VALUES ('CHAR1', 'JOIN');
        SELECT
            T.COL
            , SUBSTR(T.COL, 4) AS ALIAS
            , J.ANOTHER
        FROM TEST AS T
        LEFT JOIN JOINED AS J
        ON ALIAS = J.COL;
        """
    )
    assert cur.fetchall() == [("VARCHAR1", "CHAR1", "JOIN"), ("VARCHAR2", "CHAR2", None)]


def test_alter_table(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table table1 (id int)")
    dcur.execute("alter table table1 add column name varchar(20)")
    dcur.execute("alter table if exists table1 add column col1 int, col2 varchar(50)")
    dcur.execute("alter table if exists table1 add column if not exists col3 int, col2 varchar(50)")
    dcur.execute("select id, name, col1, col2, col3 from table1")
    assert dcur.execute("alter table table1 cluster by (name)").fetchall() == [
        {"status": "Statement executed successfully."}
    ]


def test_array_size(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""select array_size(parse_json('["a","b"]'))""")
    assert cur.fetchall() == [(2,)]

    # when json is not an array
    cur.execute("""select array_size(parse_json('{"a":"b"}'))""")
    assert cur.fetchall() == [(None,)]

    cur.execute("""select array_size([])""")
    assert cur.fetchall() == [(0,)]


def test_array_agg(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("create table table1 (id number, name varchar)")
    values = [(1, "foo"), (2, "bar"), (1, "baz"), (2, "qux")]

    dcur.executemany("insert into table1 values (%s, %s)", values)

    dcur.execute("select array_agg(name) as names from table1")
    assert dindent(dcur.fetchall()) == [{"NAMES": '[\n  "foo",\n  "bar",\n  "baz",\n  "qux"\n]'}]

    # using over

    dcur.execute(
        """
            SELECT DISTINCT
                ID
                , ANOTHER
                , ARRAY_AGG(DISTINCT COL) OVER(PARTITION BY ID) AS COLS
            FROM (select column1 as ID, column2 as COL, column3 as ANOTHER from
            (VALUES (1, 's1', 'c1'),(1, 's2', 'c1'),(1, 's3', 'c1'),(2, 's1', 'c2'), (2,'s2','c2')))
            ORDER BY ID
            """
    )
    assert dindent(dcur.fetchall()) == [
        {"ID": 1, "ANOTHER": "c1", "COLS": '[\n  "s1",\n  "s2",\n  "s3"\n]'},
        {"ID": 2, "ANOTHER": "c2", "COLS": '[\n  "s1",\n  "s2"\n]'},
    ]


def test_array_agg_within_group(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE table1 (ID INT, amount INT)")

    # two unique ids, for id 1 there are 3 amounts, for id 2 there are 2 amounts
    values = [
        (2, 40),
        (1, 10),
        (1, 30),
        (2, 50),
        (1, 20),
    ]
    dcur.executemany("INSERT INTO TABLE1 VALUES (%s, %s)", values)

    dcur.execute("SELECT id, ARRAY_AGG(amount) WITHIN GROUP (ORDER BY amount DESC) amounts FROM table1 GROUP BY id")
    rows = dcur.fetchall()

    assert dindent(rows) == [
        {"ID": 1, "AMOUNTS": "[\n  30,\n  20,\n  10\n]"},
        {"ID": 2, "AMOUNTS": "[\n  50,\n  40\n]"},
    ]

    dcur.execute("SELECT id, ARRAY_AGG(amount) WITHIN GROUP (ORDER BY amount ASC) amounts FROM table1 GROUP BY id")
    rows = dcur.fetchall()

    assert dindent(rows) == [
        {"ID": 1, "AMOUNTS": "[\n  10,\n  20,\n  30\n]"},
        {"ID": 2, "AMOUNTS": "[\n  40,\n  50\n]"},
    ]


def test_clone(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table customers (ID int, FIRST_NAME varchar, ACTIVE boolean)")
    cur.execute("insert into customers values (1, 'Jenny', True)")

    cur.execute("create table customers2 clone db1.schema1.customers")
    cur.execute("select * from customers2")
    # TODO check tags are copied too
    assert cur.fetchall() == [(1, "Jenny", True)]


def test_create_database_respects_if_not_exists() -> None:
    with tempfile.TemporaryDirectory(prefix="fakesnow-test") as db_path, fakesnow.patch(db_path=db_path):
        cursor = snowflake.connector.connect().cursor()
        cursor.execute("CREATE DATABASE db2")

        with pytest.raises(ProgrammingError, match='Database "DB2" is already attached with path'):
            cursor.execute("CREATE DATABASE db2")  # Fails as db already exists.

        cursor.execute("CREATE DATABASE IF NOT EXISTS db2")


def test_create_table_as(dcur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    dcur.execute("create or replace table t1(id varchar) as select column1 from values (1)")
    dcur.execute("select * from t1")
    assert dcur.fetchall() == [{"ID": "1"}]

    dcur.execute("create or replace table t1(id varchar) as select * from values (1)")
    dcur.execute("select * from t1")
    assert dcur.fetchall() == [{"ID": "1"}]

    dcur.execute("create or replace table t1(id varchar) as (select column1 from values (1))")
    dcur.execute("select * from t1")
    assert dcur.fetchall() == [{"ID": "1"}]

    dcur.execute("create or replace table t1(id varchar) as (select * from values (1))")
    dcur.execute("select * from t1")
    assert dcur.fetchall() == [{"ID": "1"}]


def test_dateadd_date_cast(dcur: snowflake.connector.DictCursor):
    q = """
    SELECT
        dateadd(hour, 1, '2023-04-02'::date) as d_noop,
        dateadd(day, 1, '2023-04-02'::date) as d_day,
        dateadd(week, 1, '2023-04-02'::date) as d_week,
        dateadd(month, 1, '2023-04-02'::date) as d_month,
        dateadd(year, 1, '2023-04-02'::date) as d_year
    """

    dcur.execute(q)
    assert dcur.fetchall() == [
        {
            "D_NOOP": datetime.datetime(2023, 4, 2, 1, 0),
            "D_DAY": datetime.date(2023, 4, 3),
            "D_WEEK": datetime.date(2023, 4, 9),
            "D_MONTH": datetime.date(2023, 5, 2),
            "D_YEAR": datetime.date(2024, 4, 2),
        }
    ]


def test_dateadd_string_literal_timestamp_cast(dcur: snowflake.connector.cursor.DictCursor):
    q = """
    SELECT
        DATEADD('MINUTE', 3, '2023-04-02') AS D_MINUTE,
        DATEADD('HOUR', 3, '2023-04-02') AS D_HOUR,
        DATEADD('DAY', 3, '2023-04-02') AS D_DAY,
        DATEADD('WEEK', 3, '2023-04-02') AS D_WEEK,
        DATEADD('MONTH', 3, '2023-04-02') AS D_MONTH,
        DATEADD('YEAR', 3, '2023-04-02') AS D_YEAR
    ;
    """
    dcur.execute(q)

    assert dcur.fetchall() == [
        {
            "D_MINUTE": datetime.datetime(2023, 4, 2, 0, 3),
            "D_HOUR": datetime.datetime(2023, 4, 2, 3, 0),
            "D_DAY": datetime.datetime(2023, 4, 5, 0, 0),
            "D_WEEK": datetime.datetime(2023, 4, 23, 0, 0),
            "D_MONTH": datetime.datetime(2023, 7, 2, 0, 0),
            "D_YEAR": datetime.datetime(2026, 4, 2, 0, 0),
        }
    ]

    q = """
    SELECT
        DATEADD('MINUTE', 3, '2023-04-02 01:15:00') AS DT_MINUTE,
        DATEADD('HOUR', 3, '2023-04-02 01:15:00') AS DT_HOUR,
        DATEADD('DAY', 3, '2023-04-02 01:15:00') AS DT_DAY,
        DATEADD('WEEK', 3, '2023-04-02 01:15:00') AS DT_WEEK,
        DATEADD('MONTH', 3, '2023-04-02 01:15:00') AS DT_MONTH,
        DATEADD('YEAR', 3, '2023-04-02 01:15:00') AS DT_YEAR
    ;
    """
    dcur.execute(q)

    assert dcur.fetchall() == [
        {
            "DT_MINUTE": datetime.datetime(2023, 4, 2, 1, 18),
            "DT_HOUR": datetime.datetime(2023, 4, 2, 4, 15),
            "DT_DAY": datetime.datetime(2023, 4, 5, 1, 15),
            "DT_WEEK": datetime.datetime(2023, 4, 23, 1, 15),
            "DT_MONTH": datetime.datetime(2023, 7, 2, 1, 15),
            "DT_YEAR": datetime.datetime(2026, 4, 2, 1, 15),
        }
    ]


def test_datediff_string_literal_timestamp_cast(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT DATEDIFF(DAY, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-31,)]

    cur.execute("SELECT DATEDIFF(HOUR, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-744,)]

    cur.execute("SELECT DATEDIFF(week, '2023-04-02', '2023-03-02') AS D")
    assert cur.fetchall() == [(-4,)]

    # noop
    cur.execute("select '2023-04-02'::timestamp as c1, '2023-03-02'::timestamp as c2, DATEDIFF(minute, c1, c2) AS D")
    assert cur.fetchall() == [(datetime.datetime(2023, 4, 2, 0, 0), datetime.datetime(2023, 3, 2, 0, 0), -44640)]


def test_equal_null(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select equal_null(NULL, NULL), equal_null(1, 1), equal_null(1, 2), equal_null(1, NULL)")
    assert cur.fetchall() == [(True, True, False, False)]


def test_error_syntax(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        # duckdb.ParserException
        cur.execute("create table tb1")

    assert "001003 (42000)" in str(excinfo.value)
    assert cur.sqlstate == "42000"

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        # sqlglot.errors.ParseError
        cur.execute("show tables db1.schema1")

    assert "001003 (42000)" in str(excinfo.value)
    assert cur.sqlstate == "42000"


def test_error_not_implemented(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("SELECT TO_DECIMAL('1.2345', '99.9')")
    assert "not implemented. Please raise an issue via https://github.com/tekumara/fakesnow/issues/new" in str(
        excinfo.value
    )


def test_floats_are_64bit(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (f float, f4 float4, f8 float8, d double, r real)")
    cur.execute("insert into example values (1.23, 1.23, 1.23, 1.23, 1.23)")
    cur.execute("select * from example")
    # 32 bit floats will return 1.2300000190734863 rather than 1.23
    assert cur.fetchall() == [(1.23, 1.23, 1.23, 1.23, 1.23)]


def test_hex_decode_binary(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT HEX_DECODE_BINARY('EDF1439075A83A447FB8B630DDC9C8DE')")
    # NB: Snowflake returns bytesarray instead of bytes
    assert cur.fetchall() == [(b"\xed\xf1C\x90u\xa8:D\x7f\xb8\xb60\xdd\xc9\xc8\xde",)]


def test_identifier(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (x int)")
    cur.execute("insert into example values(1)")
    cur.execute("select * from identifier('example')")
    assert cur.fetchall() == [(1,)]


def test_identifier_with_dollar_character(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # shouldn't error
    cur.execute("CREATE DATABASE ORG$INTERNAL")


def test_number_38_0_is_int(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table example (i1 number(38,0))")
    cur.execute("insert into example values (123)")
    cur.execute("select * from example")
    results = cur.fetchall()
    assert results == [(123,)]
    # should not be Decimal
    assert isinstance(results[0][0], int)


def test_non_existent_table_throws_snowflake_exception(cur: snowflake.connector.cursor.SnowflakeCursor):
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as _:
        cur.execute("select * from this_table_does_not_exist")


def test_percentile_cont(conn: snowflake.connector.SnowflakeConnection):
    *_, cur = conn.execute_string(
        """
        create or replace table aggr(k int, v decimal(10,2));
        insert into aggr (k, v) values
            (0,  0),
            (0, 10),
            (0, 20),
            (0, 30),
            (0, 40),
            (1, 10),
            (1, 20),
            (2, 10),
            (2, 20),
            (2, 25),
            (2, 30),
            (3, 60),
            (4, NULL);
        select k, percentile_cont(0.25) within group (order by v)
            from aggr
            group by k
            order by k;
        """
    )
    assert cur.fetchall() == [
        (0, Decimal("10.00000")),
        (1, Decimal("12.50000")),
        (2, Decimal("17.50000")),
        (3, Decimal("60.00000")),
        (4, None),
    ]


def test_quoted_identifiers_ignore_case(dcur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # no op
    dcur.execute("alter session set quoted_identifiers_ignore_case = false")
    assert dcur.fetchall() == [{"status": "Statement executed successfully."}]

    # no op
    dcur.execute("alter session unset quoted_identifiers_ignore_case")

    with pytest.raises(ProgrammingError) as excinfo:
        dcur.execute("alter session set quoted_identifiers_ignore_case = true")

    assert "not implemented" in str(excinfo.value)

    # other alter session params not implemented
    with pytest.raises(ProgrammingError) as excinfo:
        dcur.execute("alter session set JSON_INDENT = 1")

    assert "not implemented" in str(excinfo.value)

    with pytest.raises(ProgrammingError) as excinfo:
        dcur.execute("alter session unset JSON_INDENT")

    assert "not implemented" in str(excinfo.value)


def test_regex(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select regexp_replace('abc123', '\\\\D', '')")
    assert cur.fetchone() == ("123",)


def test_regex_substr(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/regexp_substr
    string1 = "It was the best of times, it was the worst of times."

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+\\\\w+')")
    assert cur.fetchone() == ("the best",)

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+\\\\w+', 1, 2)")
    assert cur.fetchone() == ("the worst",)

    cur.execute(f"select regexp_substr('{string1}', 'the\\\\W+(\\\\w+)', 1, 2, 'e', 1)")
    assert cur.fetchone() == ("worst",)


def test_random(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select random(420)")
    assert cur.fetchall() == [(-4068260216279105536,)]
    cur.execute("select random(420)")
    assert cur.fetchall() == [(-4068260216279105536,)]
    cur.execute("select random(419)")
    assert cur.fetchall() == [(1460638274662493184,)]
    assert cur.execute("select random()").fetchall() != cur.execute("select random()").fetchall()


def test_rowcount(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.rowcount is None
    cur.execute("create or replace table example(id int)")
    cur.execute("insert into example select * from (VALUES (1), (2), (3), (4));")
    assert cur.rowcount == 4
    cur.execute("select * from example where id > 1")
    assert cur.rowcount == 3
    cur.execute("update example set id = 22 where id > 2")
    assert cur.rowcount == 2


def test_sample(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create table example(id int)")
    cur.execute("insert into example select * from (VALUES (1), (2), (3), (4));")
    cur.execute("select * from example SAMPLE (50) SEED (999)")
    # sampling small sizes isn't exact
    assert cur.fetchall() == [(1,), (2,), (3,)]


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


def test_select_from_values(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute("select * from values ('Amsterdam', 1), ('London', 2)")

        assert cur.fetchall() == [
            {"COLUMN1": "Amsterdam", "COLUMN2": 1},
            {"COLUMN1": "London", "COLUMN2": 2},
        ]

        cur.execute(
            "SELECT column2, column1, parse_json(column3) as pj FROM VALUES ('Amsterdam', 1, '[]'), ('London', 2, '{}')"
        )

        assert cur.fetchall() == [
            {"COLUMN2": 1, "COLUMN1": "Amsterdam", "PJ": "[]"},
            {"COLUMN2": 2, "COLUMN1": "London", "PJ": "{}"},
        ]


def test_split(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert indent(cur.execute("select split('a,b,c', ',')").fetchall()) == [('[\n  "a",\n  "b",\n  "c"\n]',)]


def test_sqlglot_regression(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute(
        """with SOURCE_TABLE AS (SELECT '2024-01-01' AS start_date)
            SELECT date(a.start_date) from SOURCE_TABLE AS a"""
    ).fetchone() == (datetime.date(2024, 1, 1),)


def test_string_constant(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute("""
        select $$hello
world$$""").fetchall() == [("hello\nworld",)]


def test_tags_noop(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE table1 (id int)")
    cur.execute("ALTER TABLE table1 SET TAG foo='bar'")
    cur.execute("ALTER TABLE table1 MODIFY COLUMN name1 SET TAG foo='bar'")
    cur.execute("CREATE TAG cost_center COMMENT = 'cost_center tag'")
    cur.execute("DROP TAG cost_center")


def test_to_date(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        "SELECT to_date(to_timestamp(0)), to_date(cast(to_timestamp(0) as timestamp(9))), to_date('2024-01-26')"
    )
    assert cur.fetchall() == [(datetime.date(1970, 1, 1), datetime.date(1970, 1, 1), datetime.date(2024, 1, 26))]


def test_to_decimal(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/to_decimal#examples
    cur.execute("create or replace table number_conv(expr varchar);")
    cur.execute("insert into number_conv values ('12.3456'), ('98.76546');")
    cur.execute("select expr, to_decimal(expr),  to_number(expr, 10, 1), to_numeric(expr, 10, 8) from number_conv;")

    assert cur.fetchall() == [
        ("12.3456", 12, Decimal("12.3"), Decimal("12.34560000")),
        ("98.76546", 99, Decimal("98.8"), Decimal("98.76546000")),
    ]


def test_truncate(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE TABLE example (i INTEGER)")
    dcur.execute("INSERT INTO example VALUES (1)")

    dcur.execute("TRUNCATE TABLE example")
    assert dcur.fetchall() == [{"status": "Statement executed successfully."}]

    dcur.execute("SELECT i FROM example")
    assert dcur.fetchall() == []


def test_sha2(cur: snowflake.connector.cursor.SnowflakeCursor):
    # see https://docs.snowflake.com/en/sql-reference/functions/sha2#examples
    cur.execute(
        "select sha2('Snowflake') as a, sha2_hex('Snowflake') as b, sha2('Snowflake', 256) as c, sha2_hex('Snowflake', 256) as d;"
    )

    assert cur.fetchall() == [
        ("1dbd59f661d68b90724f21084396b865497173e4d2714f4d91cf05fa5fc5e18d",) * 4,
    ]


def test_try_to_decimal(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        "SELECT column1 AS orig_string, TRY_TO_DECIMAL(column1) AS dec, TRY_TO_DECIMAL(column1, 10, 2) AS dec_with_scale, TRY_TO_DECIMAL(column1, 4, 2) AS dec_with_range_err FROM VALUES ('345.123');"
    )
    assert cur.fetchall() == [
        (
            "345.123",
            Decimal("345"),
            Decimal("345.12"),
            None,
        ),
    ]


def test_trim_cast_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select trim(1), trim('  name 1  ')")
    assert cur.fetchall() == [("1", "name 1")]

    cur.execute("""select trim(parse_json('{"k1": "   v11  "}'):k1), trim(parse_json('{"k1": 21}'):k1)""")
    assert cur.fetchall() == [("v11", "21")]


def test_unquoted_identifiers_are_upper_cased(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table customers (id int, first_name varchar, last_name varchar)")
    dcur.execute("insert into customers values (1, 'Jenny', 'P')")
    dcur.execute("select first_name, first_name as fname from customers")

    assert dcur.fetchall() == [
        {"FIRST_NAME": "Jenny", "FNAME": "Jenny"},
    ]

    dcur.execute("select first_name, first_name as fname from customers")
    assert dcur.fetchall() == [
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
            "090106 (22000): Cannot perform CREATE TABLE. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
            in str(excinfo.value)
        )


# Snowflake SQL variables: https://docs.snowflake.com/en/sql-reference/session-variables#using-variables-in-sql
#
# Variables are scoped to the session (Eg. The connection, not the cursor)
# [x] Simple scalar variables: SET var1 = 1;
# [x] Unset variables: UNSET var1;
# [x] Simple SQL expression variables: SET INCREMENTAL_DATE = DATEADD( 'DAY', -7, CURRENT_DATE());
# [x] Basic use of variables in SQL using $ syntax: SELECT $var1;
# [ ] Multiple variables: SET (var1, var2) = (1, 'hello');
# [ ] Variables set via 'properties' on the connection https://docs.snowflake.com/en/sql-reference/session-variables#setting-variables-on-connection
# [ ] Using variables via the IDENTIFIER function: INSERT INTO IDENTIFIER($my_table_name) (i) VALUES (42);
# [ ] Session variable functions: https://docs.snowflake.com/en/sql-reference/session-variables#session-variable-functions
def test_variables(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("SET var1 = 1;")
        cur.execute("SET var2 = 'hello';")
        cur.execute("SET var3 = DATEADD( 'DAY', -7, '2024-10-09');")

        cur.execute("select $var1, $var2, $var3;")
        assert cur.fetchall() == [(1, "hello", datetime.datetime(2024, 10, 2, 0, 0))]

        cur.execute("CREATE TABLE example (id int, name varchar);")
        cur.execute("INSERT INTO example VALUES (10, 'hello'), (20, 'world');")
        cur.execute("select id, name from example where name = $var2;")
        assert cur.fetchall() == [(10, "hello")]

        cur.execute("UNSET var3;")
        with pytest.raises(
            snowflake.connector.errors.ProgrammingError, match=re.escape("Session variable '$VAR3' does not exist")
        ):
            cur.execute("select $var3;")

    # variables are scoped to the session, so they should be available in a new cursor.
    with conn.cursor() as cur:
        cur.execute("select $var1, $var2")
        assert cur.fetchall() == [(1, "hello")]

    # but not in a new connection.
    with (
        snowflake.connector.connect() as conn,
        conn.cursor() as cur,
        pytest.raises(
            snowflake.connector.errors.ProgrammingError, match=re.escape("Session variable '$VAR1' does not exist")
        ),
    ):
        cur.execute("select $var1;")

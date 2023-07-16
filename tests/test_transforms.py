import sqlglot
from sqlglot import exp

from fakesnow.transforms import (
    SUCCESS_NOP,
    as_describe,
    create_database,
    drop_schema_cascade,
    extract_comment,
    extract_text_length,
    float_to_double,
    indices_to_json_extract,
    information_schema_columns_snowflake,
    information_schema_tables_ext,
    integer_precision,
    object_construct,
    parse_json,
    regex_replace,
    regex_substr,
    semi_structured_types,
    set_schema,
    tag,
    timestamp_ntz_ns,
    to_date,
    upper_case_unquoted_identifiers,
    values_columns,
)


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )


def test_create_database() -> None:
    e = sqlglot.parse_one("create database foobar").transform(create_database)
    assert e.sql() == "ATTACH DATABASE ':memory:' AS foobar"
    assert e.args["create_db_name"] == "foobar"


def test_drop_schema_cascade() -> None:
    assert (
        sqlglot.parse_one("drop schema schema1").transform(drop_schema_cascade).sql() == "DROP schema schema1 CASCADE"
    )


def test_extract_comment() -> None:
    table1 = exp.Table(this=exp.Identifier(this="table1", quoted=False))

    e = sqlglot.parse_one("create table table1(id int) comment = 'foo bar'").transform(extract_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == (table1, "foo bar")

    e = sqlglot.parse_one("create table table1(id int) comment = foobar").transform(extract_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == (table1, "foobar")

    e = sqlglot.parse_one("COMMENT ON TABLE table1 IS 'comment1'").transform(extract_comment)
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["table_comment"] == (table1, "comment1")

    e = sqlglot.parse_one("ALTER TABLE table1 SET COMMENT = 'comment1'", read="snowflake").transform(extract_comment)
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["table_comment"] == (table1, "comment1")


def test_extract_text_length() -> None:
    sql = "CREATE TABLE table1 (t1 VARCHAR, t2 VARCHAR(10), t3 TEXT(20), i1 BIGINT)"
    e = sqlglot.parse_one(sql).transform(extract_text_length)
    assert e.sql() == sql
    assert e.args["text_lengths"] == [("t1", 16777216), ("t2", 10), ("t3", 20)]


def test_float_to_double() -> None:
    assert (
        sqlglot.parse_one("create table example (f float, f4 float4, f8 float8, d double, r real)")
        .transform(float_to_double)
        .sql()
        == "CREATE TABLE example (f DOUBLE, f4 DOUBLE, f8 DOUBLE, d DOUBLE, r DOUBLE)"
    )


def test_indices_to_object() -> None:
    assert (
        sqlglot.parse_one("SELECT myarray[0] FROM table1").transform(indices_to_json_extract).sql()
        == "SELECT JSON_EXTRACT(myarray, '$[0]') FROM table1"
    )

    assert (
        sqlglot.parse_one("SELECT name['k'] FROM semi").transform(indices_to_json_extract).sql(dialect="duckdb")
        == "SELECT name -> '$.k' FROM semi"
    )


def test_integer_precision() -> None:
    assert (
        sqlglot.parse_one(
            """
                create table example (
                    XNUMBER82 NUMBER(8, 2), XNUMBER NUMBER,  XDECIMAL DECIMAL, XNUMERIC NUMERIC,
                    XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT
                )
            """,
            read="snowflake",
        )
        .transform(integer_precision)
        .sql(dialect="duckdb")
        == "CREATE TABLE example (XNUMBER82 DECIMAL(8, 2), XNUMBER BIGINT, XDECIMAL BIGINT, XNUMERIC BIGINT, XINT BIGINT, XINTEGER BIGINT, XBIGINT BIGINT, XSMALLINT BIGINT, XTINYINT BIGINT, XBYTEINT BIGINT)"  # noqa: E501
    )


def test_information_schema_columns_snowflake() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
        .transform(information_schema_columns_snowflake)
        .sql()
        == "SELECT * FROM INFORMATION_SCHEMA.COLUMNS_SNOWFLAKE"
    )


def test_information_schema_tables_ext() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.TABLES").transform(information_schema_tables_ext).sql()
        == "SELECT * FROM INFORMATION_SCHEMA.TABLES LEFT JOIN information_schema.tables_ext ON tables.table_catalog = tables_ext.ext_table_catalog AND tables.table_schema = tables_ext.ext_table_schema AND tables.table_name = tables_ext.ext_table_name"  # noqa: E501
    )


def test_object_construct() -> None:
    assert (
        sqlglot.parse_one("SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null)", read="snowflake")
        .transform(object_construct)
        .sql(dialect="duckdb")
        == "SELECT TO_JSON({'a': 1, 'b': 'BBBB', 'c': NULL})"
    )


def test_parse_json() -> None:
    assert (
        sqlglot.parse_one("""insert into table1 (name) select parse_json('{"first":"foo", "last":"bar"}')""")
        .transform(parse_json)
        .sql()
        == """INSERT INTO table1 (name) SELECT JSON('{"first":"foo", "last":"bar"}')"""
    )


def test_regex_replace() -> None:
    assert (
        sqlglot.parse_one("SELECT regexp_replace('abc123', '\\\\D', '')").transform(regex_replace).sql()
        == "SELECT REGEXP_REPLACE('abc123', '\\D', '', 'g')"
    )


def test_regex_substr() -> None:
    assert (
        sqlglot.parse_one("SELECT regexp_substr(string1, 'the\\\\W+\\\\w+')").transform(regex_substr).sql()
        == "SELECT REGEXP_EXTRACT_ALL(string1[1 : ], 'the\\W+\\w+', 0, '')[1]"
    )


def test_semi_structured_types() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name object)").transform(semi_structured_types).sql()
        == "CREATE TABLE table1 (name JSON)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name array)").transform(semi_structured_types).sql(dialect="duckdb")
        == "CREATE TABLE table1 (name JSON[])"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name variant)").transform(semi_structured_types).sql()
        == "CREATE TABLE table1 (name JSON)"
    )


def test_tag() -> None:
    assert sqlglot.parse_one("ALTER TABLE table1 SET TAG foo='bar'", read="snowflake").transform(tag) == SUCCESS_NOP


def test_timestamp_ntz_ns() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE table1(ts TIMESTAMP_NTZ(9))", read="snowflake")
        .transform(timestamp_ntz_ns)
        .sql(dialect="duckdb")
        == "CREATE TABLE table1 (ts TIMESTAMP)"
    )


def test_to_date() -> None:
    assert (
        sqlglot.parse_one("SELECT to_date(to_timestamp(0))").transform(to_date).sql()
        == "SELECT CAST(DATE_TRUNC('day', TO_TIMESTAMP(0)) AS DATE)"
    )


def test_use() -> None:
    assert (
        sqlglot.parse_one("use database marts").transform(set_schema, current_database=None).sql()
        == "SET schema = 'marts.main'"
    )

    assert (
        sqlglot.parse_one("USE SCHEMA foo").transform(set_schema, current_database=None).sql()
        == "SET schema = 'missing_database.foo'"
    )

    assert (
        sqlglot.parse_one("use schema bar").transform(set_schema, current_database="foo").sql()
        == "SET schema = 'foo.bar'"
    )

    assert (
        sqlglot.parse_one("use schema foo.bar").transform(set_schema, current_database="marts").sql()
        == "SET schema = 'foo.bar'"
    )


def test_upper_case_unquoted_identifiers() -> None:
    assert (
        sqlglot.parse_one("select name, name as fname from table1").transform(upper_case_unquoted_identifiers).sql()
        == "SELECT NAME, NAME AS FNAME FROM TABLE1"
    )


def test_values_columns() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM VALUES ('Amsterdam', 1)").transform(values_columns).sql()
        == """SELECT * FROM (VALUES ('Amsterdam', 1)) AS _("COLUMN1", "COLUMN2")"""
    )

    # values without select aren't transformed
    assert (
        sqlglot.parse_one("INSERT INTO cities VALUES ('Amsterdam', 1)").transform(values_columns).sql()
        == "INSERT INTO cities VALUES ('Amsterdam', 1)"
    )

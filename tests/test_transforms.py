import sqlglot

from fakesnow.transforms import (
    SUCCESS_NO_OP,
    array_indices,
    as_describe,
    create_database,
    drop_schema_cascade,
    extract_comment,
    join_information_schema_ext,
    parse_json,
    regex,
    semi_structured_types,
    set_schema,
    tag,
    upper_case_unquoted_identifiers,
)


def test_array_indicies() -> None:
    assert (
        sqlglot.parse_one("SELECT myarray[0] FROM table1").transform(array_indices).sql()
        == "SELECT myarray[1] FROM table1"
    )


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )


def test_create_database() -> None:
    e = sqlglot.parse_one("create database foobar").transform(create_database)
    assert e.sql() == "ATTACH DATABASE ':memory:' AS foobar"
    assert e.args["db_name"] == "foobar"


def test_drop_schema_cascade() -> None:
    assert (
        sqlglot.parse_one("drop schema schema1").transform(drop_schema_cascade).sql() == "DROP schema schema1 CASCADE"
    )


def test_extract_comment() -> None:
    e = sqlglot.parse_one("create table table1(id int) comment = 'foo bar'").transform(extract_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == "foo bar"

    e = sqlglot.parse_one("create table table1(id int) comment = foobar").transform(extract_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == "foobar"

    e = sqlglot.parse_one("COMMENT ON TABLE table1 IS 'comment1'").transform(extract_comment)
    assert e.sql() == "COMMENT ON TABLE table1 IS 'comment1'"
    assert e.args["table_comment"] == "comment1"


def test_information_schema_ext() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.TABLES").transform(join_information_schema_ext).sql()
        == "SELECT * FROM INFORMATION_SCHEMA.TABLES LEFT JOIN information_schema.tables_ext ON tables.table_catalog = tables_ext.ext_table_catalog AND tables.table_schema = tables_ext.ext_table_schema AND tables.table_name = tables_ext.ext_table_name"  # noqa: e501
    )


def test_parse_json() -> None:
    assert (
        sqlglot.parse_one("""insert into table1 (name) select parse_json('{"first":"foo", "last":"bar"}')""")
        .transform(parse_json)
        .sql()
        == """INSERT INTO table1 (name) SELECT JSON('{"first":"foo", "last":"bar"}')"""
    )


def test_regex() -> None:
    assert (
        sqlglot.parse_one("SELECT regexp_replace('abc123', '\\\\D', '')").transform(regex).sql()
        == "SELECT REGEXP_REPLACE('abc123', '\\D', '', 'g')"
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
    assert sqlglot.parse_one("ALTER TABLE table1 SET TAG foo='bar'", read="snowflake").transform(tag) == SUCCESS_NO_OP


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

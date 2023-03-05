import sqlglot

from fakesnow.transforms import (
    SUCCESS_NO_OP,
    as_describe,
    create_database,
    drop_schema_cascade,
    extract_comment,
    join_information_schema_ext,
    set_schema,
    tag,
    upper_case_unquoted_identifiers,
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


def test_information_schema_ext() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.TABLES").transform(join_information_schema_ext).sql()
        == "SELECT * FROM INFORMATION_SCHEMA.TABLES LEFT JOIN information_schema.tables_ext ON tables.table_catalog = tables_ext.ext_table_catalog AND tables.table_schema = tables_ext.ext_table_schema AND tables.table_name = tables_ext.ext_table_name"  # noqa: e501
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


def test_drop_schema_cascade() -> None:
    assert (
        sqlglot.parse_one("drop schema schema1").transform(drop_schema_cascade).sql() == "DROP schema schema1 CASCADE"
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
        sqlglot.parse_one("select name, name as fname from customers").transform(upper_case_unquoted_identifiers).sql()
        == "SELECT NAME, NAME AS FNAME FROM CUSTOMERS"
    )

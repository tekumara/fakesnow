import sqlglot

from fakesnow.transforms import as_describe, create_database, join_information_schema_ext, remove_comment, set_schema


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


def test_remove_comment() -> None:
    e = sqlglot.parse_one("create table table1(id int) comment = 'foo bar'").transform(remove_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == "foo bar"

    e = sqlglot.parse_one("create table table1(id int) comment = foobar").transform(remove_comment)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == "foobar"


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

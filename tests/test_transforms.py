import sqlglot

from fakesnow.transforms import as_describe, create_database, set_schema


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


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )


def test_create_database() -> None:
    assert (
        sqlglot.parse_one("create database foobar").transform(create_database).sql()
        == "ATTACH DATABASE ':memory:' AS foobar"
    )

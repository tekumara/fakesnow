import sqlglot

from fakesnow.transforms import as_describe, qualified_schema, set_schema


def test_qualified_schema() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM marts.jaffles.customers").transform(qualified_schema).sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(qualified_schema).sql()
        == "SELECT * FROM jaffles.customers"
    )

    assert sqlglot.parse_one("SELECT * FROM customers").transform(qualified_schema).sql() == "SELECT * FROM customers"

    assert (
        sqlglot.parse_one("CREATE SCHEMA marts.jaffles").transform(qualified_schema).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("create schema marts.jaffles").transform(qualified_schema).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert sqlglot.parse_one("CREATE SCHEMA jaffles").transform(qualified_schema).sql() == "CREATE SCHEMA jaffles"

    assert (
        sqlglot.parse_one("DROP SCHEMA marts.jaffles").transform(qualified_schema).sql()
        == "DROP SCHEMA marts_jaffles"
    )

    assert sqlglot.parse_one("USE SCHEMA foo").transform(qualified_schema).sql() == "USE SCHEMA foo"


def test_set_schema() -> None:
    assert sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql() == "SET schema = foo"

    assert sqlglot.parse_one("use schema bar").transform(set_schema).sql() == "SET schema = bar"


def test_as_desribe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )

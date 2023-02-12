import sqlglot
import pytest
import snowflake.connector.errors

from fakesnow.transforms import as_describe, qualified_schema, set_schema


def test_qualified_schema_table() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM customers")
        .transform(qualified_schema, database="marts", schema="jaffles")
        .sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(qualified_schema, database="marts").sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM marts.jaffles.customers").transform(qualified_schema).sql()
        == "SELECT * FROM marts_jaffles.customers"
    )


    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("SELECT * FROM customers").transform(qualified_schema, database="marts").sql()

    assert (
        "90105 (22000): Cannot perform SELECT. This session does not have a current schema. Call 'USE SCHEMA', or use a qualified name."
        in str(excinfo.value)
    )

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(qualified_schema).sql()

    assert (
        "90105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
        in str(excinfo.value)
    )


def test_qualified_schema_schema() -> None:
    assert (
        sqlglot.parse_one("CREATE SCHEMA marts.jaffles").transform(qualified_schema).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("create schema marts.jaffles").transform(qualified_schema).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(qualified_schema, database="marts").sql()

    assert (
        "090105 (22000): Cannot perform CREATE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
        in str(excinfo.value)
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(qualified_schema, database="marts").sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("DROP SCHEMA marts.jaffles").transform(qualified_schema).sql() == "DROP SCHEMA marts_jaffles"
    )

    assert sqlglot.parse_one("USE SCHEMA foo").transform(qualified_schema).sql() == "USE SCHEMA foo"


def test_set_schema() -> None:
    assert sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql() == "SET schema = foo"

    assert sqlglot.parse_one("use schema bar").transform(set_schema).sql() == "SET schema = bar"


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )

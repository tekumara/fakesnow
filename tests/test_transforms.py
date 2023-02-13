import sqlglot
import pytest
import snowflake.connector.errors

from fakesnow.transforms import as_describe, database_prefix, set_schema


def test_database_prefix_for_select_exp() -> None:
    # no current database doesn't error on transform but will fail on execution
    assert (
        sqlglot.parse_one("SELECT * FROM customers")
        .transform(database_prefix)
        .sql()
        == "SELECT * FROM customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM customers")
        .transform(database_prefix, current_database="marts")
        .sql()
        == "SELECT * FROM customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix, current_database="marts").sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM marts.jaffles.customers").transform(database_prefix, current_database="db1").sql()
        == "SELECT * FROM marts_jaffles.customers"
    )


    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix).sql()

    assert (
        "090105 (22000): Cannot perform SELECT. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
        in str(excinfo.value)
    )


def test_database_prefix_for_create_table_exp() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE jaffles.customers (ID INT)").transform(database_prefix, current_database="marts").sql()
        == "CREATE TABLE marts_jaffles.customers (ID INT)"
    )

    ## TODO: check
    assert (
        sqlglot.parse_one("CREATE TABLE customers (ID INT)").transform(database_prefix).sql()
        == "CREATE TABLE customers (ID INT)"
    )


def test_database_prefix_for_schema_exp() -> None:
    assert (
        sqlglot.parse_one("CREATE SCHEMA marts.jaffles").transform(database_prefix).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA marts.jaffles").transform(database_prefix, current_database="db1").sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(database_prefix).sql()

    assert (
        "090105 (22000): Cannot perform CREATE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
        in str(excinfo.value)
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(database_prefix, current_database="marts").sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    # variants in casing and command

    assert (
        sqlglot.parse_one("create schema marts.jaffles").transform(database_prefix).sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("DROP SCHEMA marts.jaffles").transform(database_prefix).sql() == "DROP SCHEMA marts_jaffles"
    )

    # use schema

    assert sqlglot.parse_one("USE SCHEMA jaffles").transform(database_prefix, current_database="marts").sql() == "USE SCHEMA marts_jaffles"

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        sqlglot.parse_one("USE SCHEMA jaffles").transform(database_prefix).sql()

    # NB: snowflake will generate a Object does not exist here. The error we return is more specific.
    assert (
        "090105 (22000): Cannot perform USE SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name."
        in str(excinfo.value)
    )


def test_set_schema() -> None:
    assert sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql() == "SET schema = foo"

    assert sqlglot.parse_one("use schema bar").transform(set_schema).sql() == "SET schema = bar"


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )

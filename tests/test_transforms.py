import sqlglot

from fakesnow.transforms import as_describe, database_prefix, set_schema


def test_database_prefix_for_select_exp() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM customers")
        .transform(database_prefix, current_database="marts", current_schema="jaffles")
        .sql()
        == "SELECT * FROM customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix, current_database="marts").sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM marts.jaffles.customers")
        .transform(database_prefix, current_database="db1")
        .sql()
        == "SELECT * FROM marts_jaffles.customers"
    )

    # unqualified and no current database

    assert (
        sqlglot.parse_one("SELECT * FROM customers").transform(database_prefix, current_database=None).sql()
        == "SELECT * FROM unqualified_and_no_current_database.customers"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix, current_database=None).sql()
        == "SELECT * FROM unqualified_and_no_current_database_jaffles.customers"
    )

    # unqualified and no current schema

    assert (
        sqlglot.parse_one("SELECT * FROM customers")
        .transform(database_prefix, current_database="marts", current_schema=None)
        .sql()
        == "SELECT * FROM unqualified_and_no_current_schema.customers"
    )


def test_database_prefix_for_table_exp() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE customers (ID INT)")
        .transform(database_prefix, current_database="marts", current_schema="jaffles")
        .sql()
        == "CREATE TABLE customers (ID INT)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE jaffles.customers (ID INT)")
        .transform(database_prefix, current_database="marts")
        .sql()
        == "CREATE TABLE marts_jaffles.customers (ID INT)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE customers (ID INT)").transform(database_prefix, current_database=None).sql()
        == "CREATE TABLE unqualified_and_no_current_database.customers (ID INT)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE customers (ID INT)")
        .transform(database_prefix, current_database="marts", current_schema=None)
        .sql()
        == "CREATE TABLE unqualified_and_no_current_schema.customers (ID INT)"
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

    assert (
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(database_prefix, current_database="marts").sql()
        == "CREATE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA jaffles").transform(database_prefix, current_database=None).sql()
        == "CREATE SCHEMA unqualified_and_no_current_database_jaffles"
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

    assert (
        sqlglot.parse_one("USE SCHEMA jaffles").transform(database_prefix, current_database="marts").sql()
        == "USE SCHEMA marts_jaffles"
    )

    assert (
        sqlglot.parse_one("USE SCHEMA jaffles").transform(database_prefix, current_database=None).sql()
        == "USE SCHEMA unqualified_and_no_current_database_jaffles"
    )


def test_use_database() -> None:
    assert (
        sqlglot.parse_one("use database marts").transform(database_prefix, current_database=None).sql()
        == "USE database marts"
    )


def test_set_schema() -> None:
    assert sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql() == "SET schema = foo"

    assert sqlglot.parse_one("use schema bar").transform(set_schema).sql() == "SET schema = bar"


def test_as_describe() -> None:
    assert (
        sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        == "DESCRIBE SELECT name FROM CUSTOMERS"
    )

import sqlglot

from fakesnow.checks import is_unqualified_table_expression


def test_check_unqualified_select() -> None:
    assert is_unqualified_table_expression(sqlglot.parse_one("SELECT * FROM customers")) == (True, True)

    assert is_unqualified_table_expression(sqlglot.parse_one("SELECT * FROM jaffles.customers")) == (True, False)

    assert is_unqualified_table_expression(sqlglot.parse_one("SELECT * FROM marts.jaffles.customers")) == (False, False)


def test_check_unqualified_create_table() -> None:
    assert is_unqualified_table_expression(sqlglot.parse_one("CREATE TABLE customers (ID INT)")) == (True, True)

    assert is_unqualified_table_expression(sqlglot.parse_one("CREATE TABLE jaffles.customers (ID INT)")) == (
        True,
        False,
    )


def test_check_unqualified_drop_table() -> None:
    assert is_unqualified_table_expression(sqlglot.parse_one("DROP TABLE customers")) == (True, True)

    assert is_unqualified_table_expression(sqlglot.parse_one("DROP TABLE jaffles.customers")) == (
        True,
        False,
    )


def test_check_unqualified_schema() -> None:
    # assert is_unqualified_table_expression(sqlglot.parse_one("CREATE SCHEMA jaffles")) == (True, False)

    # assert is_unqualified_table_expression(sqlglot.parse_one("CREATE SCHEMA marts.jaffles")) ==  (False, False)

    assert is_unqualified_table_expression(sqlglot.parse_one("USE SCHEMA jaffles")) == (True, False)

    assert is_unqualified_table_expression(sqlglot.parse_one("USE SCHEMA marts.jaffles")) == (False, False)


def test_check_unqualified_database() -> None:
    assert is_unqualified_table_expression(sqlglot.parse_one("CREATE DATABASE marts")) == (False, False)

    assert is_unqualified_table_expression(sqlglot.parse_one("USE DATABASE marts")) == (False, False)


def test_check_unqualified_select_cte_uses_base_table() -> None:
    expression = sqlglot.parse_one("WITH cte AS (SELECT * FROM marts.jaffles.customers) SELECT * FROM cte")

    assert is_unqualified_table_expression(expression) == (False, False)


def test_check_cte_with_mixed_qualification_uses_all_base_tables() -> None:
    expression = sqlglot.parse_one(
        "WITH cte AS (SELECT * FROM marts.jaffles.customers JOIN orders ON TRUE) SELECT * FROM cte"
    )

    assert is_unqualified_table_expression(expression) == (True, True)


def test_check_table_function_does_not_require_current_database() -> None:
    expression = sqlglot.parse_one("SELECT * FROM table(flatten([1, 2]))", read="snowflake")

    assert is_unqualified_table_expression(expression) == (False, False)

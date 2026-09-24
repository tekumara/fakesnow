import sqlglot

from fakesnow.transforms.options import parse_options


def test_parse_options_converts_parenthesized_value() -> None:
    expression = sqlglot.parse_one("CREATE FILE FORMAT csv_fmt TYPE=CSV NULL_IF=('NULL')", read="snowflake")

    assert parse_options(list(expression.args["properties"])) == {"TYPE": "CSV", "NULL_IF": ["NULL"]}


def test_parse_options_converts_tuple_value() -> None:
    expression = sqlglot.parse_one("CREATE FILE FORMAT csv_fmt TYPE=CSV NULL_IF=('NULL','')", read="snowflake")

    assert parse_options(list(expression.args["properties"])) == {"TYPE": "CSV", "NULL_IF": ["NULL", ""]}


def test_parse_options_skips_temporary_property() -> None:
    expression = sqlglot.parse_one("CREATE TEMPORARY FILE FORMAT csv_fmt TYPE=CSV", read="snowflake")

    assert parse_options(list(expression.args["properties"])) == {"TYPE": "CSV"}

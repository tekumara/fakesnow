import sqlglot
from sqlglot import exp

import fakesnow.expr as expr


def parse(s: str) -> exp.Expression:
    return sqlglot.parse_one(s, read="snowflake")


def test_key_command_create() -> None:
    assert expr.key_command(parse("drop schema foobar")) == "DROP SCHEMA"
    assert expr.key_command(parse("create table customers(id int)")) == "CREATE TABLE"


def test_key_command_use() -> None:
    assert expr.key_command(parse("use database foobar")) == "USE DATABASE"
    assert expr.key_command(parse("use schema foobar")) == "USE SCHEMA"


def test_key_command_set() -> None:
    assert expr.key_command(parse("set schema = 'foobar'")) == "SET"


def test_key_command_select() -> None:
    assert expr.key_command(parse("select * from foobar")) == "SELECT"

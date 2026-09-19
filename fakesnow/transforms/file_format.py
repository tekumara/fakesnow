from __future__ import annotations

from typing import Any

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp


def option_value(value: Expr | None) -> Any:  # noqa: ANN401
    """Convert a file format option value expression to a python value."""
    if isinstance(value, exp.Literal):
        return value.this if value.is_string else int(value.this)
    if isinstance(value, exp.Boolean):
        return value.this
    if isinstance(value, exp.Paren):
        return [option_value(value.this)]
    if isinstance(value, exp.Tuple):
        return [option_value(e) for e in value.expressions]
    if isinstance(value, exp.Var):
        return value.this
    raise NotImplementedError(f"{value.__class__.__name__} as a file format option value")


def format_options(properties: list[Expr]) -> dict[str, Any]:
    """Convert file format properties to a dict of option name -> python value."""
    options: dict[str, Any] = {}
    for prop in properties:
        if isinstance(prop, exp.TemporaryProperty):
            continue
        assert isinstance(prop, exp.Property), f"{prop.__class__} is not a Property"
        assert isinstance(prop.this, exp.Var), f"{prop.this.__class__} is not a Var"
        options[prop.this.name.upper()] = option_value(prop.args.get("value"))
    return options


def create_file_format(
    expression: Expr,
    current_database: str | None,
    current_schema: str | None,
) -> Expr:
    """Transform CREATE FILE FORMAT to an INSERT statement for the fake file formats table."""
    if not (
        isinstance(expression, exp.Create)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, str)
        and kind.upper() == "FILE FORMAT"
        and (table := expression.find(exp.Table))
    ):
        return expression

    ident = table.this
    if not isinstance(ident, exp.Identifier):
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\nInvalid identifier type {ident.__class__.__name__} for file format name.",
            errno=1003,
            sqlstate="42000",
        )

    catalog = table.catalog or current_database
    schema = table.db or current_schema
    format_name = ident.this
    properties = expression.args.get("properties") or []
    options = format_options(list(properties))

    # File-format creation persists independently of the active transaction on Snowflake.
    # Keep its metadata outside DuckDB's transaction so it cannot commit preceding DML.
    transformed = sqlglot.parse_one("SELECT 1", read="duckdb")
    transformed.args["create_file_format_name"] = format_name
    transformed.args["create_file_format_key"] = (catalog, schema, format_name)
    transformed.args["create_file_format_options"] = options
    transformed.args["create_file_format_replace"] = bool(expression.args.get("replace"))
    transformed.args["create_file_format_if_not_exists"] = bool(expression.args.get("exists"))
    return transformed

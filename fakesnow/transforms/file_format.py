from __future__ import annotations

import datetime
import json
from typing import Any

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.transforms.stage import parts_from_var


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
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    replace = expression.args.get("replace")
    if_not_exists = expression.args.get("exists")

    properties = expression.args.get("properties") or []
    options = format_options(list(properties))
    format_type = str(options.get("TYPE", "CSV")).upper()
    options_json = json.dumps(options).replace("'", "''")

    guard = (
        ""
        if replace
        else f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM _fs_global._fs_information_schema._fs_file_formats
            WHERE name = '{format_name}' AND database_name = '{catalog}' AND schema_name = '{schema}'
        )"""
    )
    insert_sql = f"""
        INSERT {"OR REPLACE" if replace else ""} INTO _fs_global._fs_information_schema._fs_file_formats
        (created_on, name, database_name, schema_name, type, options)
        SELECT
            '{now}', '{format_name}', '{catalog}', '{schema}', '{format_type}', '{options_json}'
        {guard}
        """
    transformed = sqlglot.parse_one(insert_sql, read="duckdb")
    transformed.args["create_file_format_name"] = format_name
    transformed.args["create_file_format_if_not_exists"] = if_not_exists
    return transformed


def lookup_file_format(
    duck_conn: DuckDBPyConnection,
    name: str,
    current_database: str | None,
    current_schema: str | None,
) -> dict[str, Any]:
    """Return the stored options of a named file format.

    Raises if the file format does not exist.
    """
    database_name, schema_name, format_name = parts_from_var(name, current_database, current_schema)

    duck_conn.execute(
        """
        SELECT options FROM _fs_global._fs_information_schema._fs_file_formats
        WHERE database_name = ? AND schema_name = ? AND name = ?
        """,
        (database_name, schema_name, format_name),
    )
    if result := duck_conn.fetchone():
        return json.loads(result[0])

    raise snowflake.connector.errors.ProgrammingError(
        msg=f"SQL compilation error:\nFile format '{format_name}' does not exist or not authorized.",
        errno=2003,
        sqlstate="02000",
    )

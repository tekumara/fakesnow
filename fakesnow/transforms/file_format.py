from __future__ import annotations

import datetime
import json

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp

from fakesnow.transforms.options import parse_options


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

    options = parse_options(expression.args.get("properties") or [])
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

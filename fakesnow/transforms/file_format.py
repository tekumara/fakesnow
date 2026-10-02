from __future__ import annotations

import datetime
import json
from typing import Any, cast

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.transforms.options import parse_options
from fakesnow.transforms.stage import parts_from_var

# Defaults for the CSV format options handle_csv (fakesnow/copy_into.py) understands.
CSV_DEFAULT_OPTIONS: dict[str, Any] = {
    "FIELD_DELIMITER": ",",
    "SKIP_HEADER": 0,
    # Snowflake's real default is \, but DuckDB can't replicate escaping unenclosed
    # fields, so handle_csv only accepts NONE. Report the value fakesnow honors.
    "ESCAPE_UNENCLOSED_FIELD": "NONE",
    "FIELD_OPTIONALLY_ENCLOSED_BY": "NONE",
    "NULL_IF": ["\\N"],
    "COMPRESSION": "AUTO",
    "EMPTY_FIELD_AS_NULL": True,
}


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
    comment = str(options.pop("COMMENT", "")).replace("'", "''")
    # PARQUET is also loadable by COPY, but ReadParquet has no file-format options to report.
    defaults = CSV_DEFAULT_OPTIONS if format_type == "CSV" else {}
    options = {"TYPE": format_type, **defaults, **options}
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
        (created_on, name, database_name, schema_name, type, options, comment)
        SELECT
            '{now}', '{format_name}', '{catalog}', '{schema}', '{format_type}', '{options_json}', '{comment}'
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
    """Return the named format's options for COPY.

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
        return cast(dict[str, Any], json.loads(result[0]))

    raise snowflake.connector.errors.ProgrammingError(
        msg=f"SQL compilation error:\nFile format '{format_name}' does not exist or not authorized.",
        errno=2003,
        sqlstate="02000",
    )

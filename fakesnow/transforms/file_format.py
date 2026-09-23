from __future__ import annotations

import datetime
import json
from typing import Any

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp

from fakesnow.transforms.stage import parts_from_var

# Defaults reported by SHOW FILE FORMATS, including options not explicitly set by CREATE.
DEFAULT_OPTIONS: dict[str, dict[str, Any]] = {
    "CSV": {
        "RECORD_DELIMITER": "\n",
        "FIELD_DELIMITER": ",",
        "FILE_EXTENSION": None,
        "SKIP_HEADER": 0,
        "PARSE_HEADER": False,
        "DATE_FORMAT": "AUTO",
        "TIME_FORMAT": "AUTO",
        "TIMESTAMP_FORMAT": "AUTO",
        "BINARY_FORMAT": "HEX",
        "ESCAPE": "NONE",
        "ESCAPE_UNENCLOSED_FIELD": "\\",
        "TRIM_SPACE": False,
        "FIELD_OPTIONALLY_ENCLOSED_BY": "NONE",
        "NULL_IF": ["\\N"],
        "COMPRESSION": "AUTO",
        "ERROR_ON_COLUMN_COUNT_MISMATCH": True,
        "VALIDATE_UTF8": True,
        "SKIP_BLANK_LINES": False,
        "REPLACE_INVALID_CHARACTERS": False,
        "EMPTY_FIELD_AS_NULL": True,
        "SKIP_BYTE_ORDER_MARK": True,
        "ENCODING": "UTF8",
        "MULTI_LINE": True,
    },
    "JSON": {
        "FILE_EXTENSION": None,
        "DATE_FORMAT": "AUTO",
        "TIME_FORMAT": "AUTO",
        "TIMESTAMP_FORMAT": "AUTO",
        "BINARY_FORMAT": "HEX",
        "TRIM_SPACE": False,
        "NULL_IF": [],
        "COMPRESSION": "AUTO",
        "ENABLE_OCTAL": False,
        "ALLOW_DUPLICATE": False,
        "STRIP_OUTER_ARRAY": False,
        "STRIP_NULL_VALUES": False,
        "IGNORE_UTF8_ERRORS": False,
        "REPLACE_INVALID_CHARACTERS": False,
        "SKIP_BYTE_ORDER_MARK": True,
        "MULTI_LINE": True,
    },
    "AVRO": {
        "TRIM_SPACE": False,
        "NULL_IF": [],
        "COMPRESSION": "AUTO",
        "REPLACE_INVALID_CHARACTERS": False,
    },
    "ORC": {
        "TRIM_SPACE": False,
        "NULL_IF": [],
        "REPLACE_INVALID_CHARACTERS": False,
    },
    "PARQUET": {
        "TRIM_SPACE": False,
        "NULL_IF": [],
        "COMPRESSION": "AUTO",
        "BINARY_AS_TEXT": True,
        "REPLACE_INVALID_CHARACTERS": False,
        "USE_LOGICAL_TYPE": False,
        "USE_VECTORIZED_SCANNER": False,
    },
    "XML": {
        "COMPRESSION": "AUTO",
        "IGNORE_UTF8_ERRORS": False,
        "PRESERVE_SPACE": False,
        "STRIP_OUTER_ELEMENT": False,
        "DISABLE_SNOWFLAKE_DATA": False,
        "DISABLE_AUTO_CONVERT": False,
        "REPLACE_INVALID_CHARACTERS": False,
        "SKIP_BYTE_ORDER_MARK": True,
    },
}


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
        if isinstance(prop, exp.SchemaCommentProperty):
            options["COMMENT"] = option_value(prop.this)
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
    comment = str(options.pop("COMMENT", "")).replace("'", "''")
    options = {"TYPE": format_type, **DEFAULT_OPTIONS.get(format_type, {}), **options}
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
    """Return the options of a named file format that differ from its type's defaults.

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
        return _non_default_options(json.loads(result[0]))

    raise snowflake.connector.errors.ProgrammingError(
        msg=f"SQL compilation error:\nFile format '{format_name}' does not exist or not authorized.",
        errno=2003,
        sqlstate="02000",
    )


def _non_default_options(options: dict[str, Any]) -> dict[str, Any]:
    defaults = DEFAULT_OPTIONS.get(str(options.get("TYPE", "CSV")).upper(), {})
    return {name: value for name, value in options.items() if name not in defaults or defaults[name] != value}

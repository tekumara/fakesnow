from __future__ import annotations

import datetime
import json
from typing import Any

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr, exp

from fakesnow.transforms.options import parse_options

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

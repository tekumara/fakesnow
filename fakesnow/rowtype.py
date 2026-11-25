import re
from collections.abc import Iterable
from typing import TypedDict

# Connector 5 re-exports this for backwards compatibility, but does not mark it public for type checkers.
from snowflake.connector.constants import FIELD_ID_TO_NAME  # pyright: ignore[reportPrivateImportUsage]
from snowflake.connector.cursor import ResultMetadata

_TYPE_CODE_BY_NAME = {name.lower(): code for code, name in FIELD_ID_TO_NAME.items()}


class ColumnInfo(TypedDict):
    name: str
    database: str
    schema: str
    table: str
    nullable: bool
    type: str
    byteLength: int | None
    length: int | None
    scale: int | None
    precision: int | None
    collation: str | None


duckdb_to_sf_type = {
    '"NULL"': "text",
    "BIGINT": "fixed",
    "BLOB": "binary",
    "BOOLEAN": "boolean",
    "DATE": "date",
    "DECIMAL": "fixed",
    "DOUBLE": "real",
    "HUGEINT": "fixed",
    "INTEGER": "fixed",
    "JSON": "variant",
    "TIME": "time",
    "TIMESTAMP WITH TIME ZONE": "timestamp_tz",
    "TIMESTAMP_NS": "timestamp_ntz",
    "TIMESTAMP": "timestamp_ntz",
    "UBIGINT": "fixed",
    "VARCHAR": "text",
}


def columns_as_rowtype(columns: Iterable[tuple[str, str]]) -> list[ColumnInfo]:
    """Convert DuckDB column names and logical types to Snowflake rowtype."""

    def as_column_info(column_name: str, column_type: str) -> ColumnInfo:
        if not (sf_type := duckdb_to_sf_type.get("DECIMAL" if column_type.startswith("DECIMAL") else column_type)):
            raise NotImplementedError(f"for column type {column_type}")

        info: ColumnInfo = {
            "name": column_name,
            # TODO
            "database": "",
            "schema": "",
            "table": "",
            # TODO
            "nullable": True,
            "type": sf_type,
            "byteLength": None,
            "length": None,
            "scale": None,
            "precision": None,
            "collation": None,
        }

        if column_type.startswith("DECIMAL"):
            match = re.search(r"\((\d+),(\d+)\)", column_type)
            info["precision"] = int(match[1]) if match else 38
            info["scale"] = int(match[2]) if match else 0
        elif sf_type == "fixed":
            info["precision"] = 38
            info["scale"] = 0
        elif sf_type == "text":
            # TODO: fetch actual varchar size
            info["byteLength"] = 16777216
            info["length"] = 16777216
        elif sf_type.startswith("time"):
            info["precision"] = 0
            info["scale"] = 9
        elif sf_type == "binary":
            info["byteLength"] = 8388608
            info["length"] = 8388608

        return info

    return [as_column_info(column_name, column_type) for column_name, column_type in columns]


def describe_as_rowtype(describe_results: list) -> list[ColumnInfo]:
    return columns_as_rowtype((name, type_) for name, type_, _null, _key, _default, _extra in describe_results)


def rowtype_as_result_metadata(rowtype: Iterable[ColumnInfo]) -> list[ResultMetadata]:
    return [
        ResultMetadata(
            c["name"],
            _TYPE_CODE_BY_NAME[c["type"]],
            c["length"] if c["type"] == "text" else None,
            c["length"],
            c["precision"],
            c["scale"],
            c["nullable"],
        )
        for c in rowtype
    ]


def describe_as_result_metadata(describe_results: list) -> list[ResultMetadata]:
    return rowtype_as_result_metadata(describe_as_rowtype(describe_results))

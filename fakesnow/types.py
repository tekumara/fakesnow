import re
from typing import Optional, TypedDict

from snowflake.connector.cursor import ResultMetadata


class ColumnInfo(TypedDict):
    name: str
    database: str
    schema: str
    table: str
    nullable: bool
    type: str
    byteLength: Optional[int]
    length: Optional[int]
    scale: Optional[int]
    precision: Optional[int]
    collation: Optional[str]


duckdb_to_sf_type = {
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


def describe_as_rowtype(describe_results: list) -> list[ColumnInfo]:
    """Convert duckdb column type to snowflake rowtype returned by the API."""

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

    column_infos = [
        as_column_info(column_name, column_type)
        for (column_name, column_type, _null, _key, _default, _extra) in describe_results
    ]
    return column_infos


def describe_as_result_metadata(describe_results: list) -> list[ResultMetadata]:
    return [ResultMetadata.from_column(c) for c in describe_as_rowtype(describe_results)]  # pyright: ignore[reportArgumentType]

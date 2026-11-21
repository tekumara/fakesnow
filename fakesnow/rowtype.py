import importlib
import re
from typing import TypedDict

from snowflake.connector.cursor import ResultMetadata

try:  # connector 5.x
    _type_code = importlib.import_module("snowflake.connector._internal.type_codes").get_type_code
except ModuleNotFoundError:  # connector 4.x
    _field_name_to_id = importlib.import_module("snowflake.connector.constants").FIELD_NAME_TO_ID

    def _type_code(snowflake_type: str) -> int:
        return _field_name_to_id[snowflake_type.upper()]


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
    return [
        ResultMetadata(
            c["name"],
            _type_code(c.get("extTypeName") or c["type"]),
            None,
            c["length"],
            c["precision"],
            c["scale"],
            c["nullable"],
        )
        for c in describe_as_rowtype(describe_results)
    ]

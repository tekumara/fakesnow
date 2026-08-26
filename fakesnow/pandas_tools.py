from __future__ import annotations

import json
import struct
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal

from duckdb import DuckDBPyConnection

from fakesnow.conn import FakeSnowflakeConnection

if TYPE_CHECKING:
    # don't require pandas or numpy at import time
    import numpy as np
    import pandas as pd


CopyResult = tuple[
    str,
    str,
    int,
    int,
    int,
    int,
    str | None,
    int | None,
    int | None,
    str | None,
]

WritePandasResult = tuple[
    bool,
    int,
    int,
    Sequence[CopyResult],
]


def sql_type(dtype: np.dtype | pd.api.extensions.ExtensionDtype, *, use_logical_type: bool = False) -> str:
    import pandas as pd

    if pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_integer_dtype(dtype):
        return "NUMBER"
    elif str(dtype) == "float16":
        return "BINARY"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_string_dtype(dtype):
        return "VARCHAR"
    elif isinstance(dtype, pd.CategoricalDtype):
        # parquet stages a category as a dictionary of its own type, which is what snowflake infers
        return sql_type(dtype.categories.dtype, use_logical_type=use_logical_type)
    elif use_logical_type and pd.api.types.is_datetime64_any_dtype(dtype):
        # without USE_LOGICAL_TYPE snowflake infers a number from the epoch offset the staged
        # parquet column holds, except for a tz-aware sub-nanosecond one, see
        # https://docs.snowflake.com/en/sql-reference/sql/create-file-format#label-parquet-format-type-options
        return "TIMESTAMP_LTZ" if isinstance(dtype, pd.DatetimeTZDtype) else "TIMESTAMP_NTZ"
    elif use_logical_type and _is_time(dtype):
        return "TIME"
    else:
        raise NotImplementedError(f"sql_type {dtype=}")


def _is_time(dtype: np.dtype | pd.api.extensions.ExtensionDtype) -> bool:
    # pandas has no time dtype of its own, only an arrow-backed one
    import pandas as pd
    import pyarrow as pa

    return isinstance(dtype, pd.ArrowDtype) and pa.types.is_time(dtype.pyarrow_dtype)


def write_pandas(
    conn: FakeSnowflakeConnection,
    df: pd.DataFrame,
    table_name: str,
    database: str | None = None,
    schema: str | None = None,
    chunk_size: int | None = None,
    compression: str = "gzip",
    on_error: str = "abort_statement",
    parallel: int = 4,
    quote_identifiers: bool = True,
    auto_create_table: bool = False,
    create_temp_table: bool = False,
    overwrite: bool = False,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
    use_logical_type: bool | None = None,
    **kwargs: Any,
) -> WritePandasResult:
    name = table_name
    if schema:
        name = f"{schema}.{name}"
    if database:
        name = f"{database}.{name}"

    if auto_create_table:
        cols = [f"{c} {sql_type(t, use_logical_type=bool(use_logical_type))}" for c, t in df.dtypes.to_dict().items()]

        if overwrite:
            # overwrite drops and recreates the table, so its schema matches the dataframe
            conn.cursor().execute(f"DROP TABLE IF EXISTS {name}")

        conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {name} ({','.join(cols)})")
    elif overwrite:
        # overwrite truncates the existing table before loading
        conn.cursor().execute(f"TRUNCATE TABLE {name}")

    count = _insert_df(conn._duck_conn, df, name)  # noqa: SLF001

    # mocks https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
    mock_copy_results = [("fakesnow/file0.txt", "LOADED", count, count, 1, 0, None, None, None, None)]

    # return success
    return (True, len(mock_copy_results), count, mock_copy_results)


def _insert_df(duck_conn: DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> int:
    import pandas as pd

    # Objects in dataframes are written as parquet structs, and snowflake loads parquet structs as json strings.
    # Whereas duckdb analyses a dataframe see https://duckdb.org/docs/api/python/data_ingestion.html#pandas-dataframes--object-columns
    # and converts a object to the most specific type possible, eg: dict -> STRUCT, MAP or varchar, and list -> LIST
    # For dicts see https://github.com/duckdb/duckdb/pull/3985 and https://github.com/duckdb/duckdb/issues/9510
    #
    # When the rows have dicts with different keys there isn't a single STRUCT that can cover them, so the type is
    # varchar and value a string containing a struct representation. In order to support dicts with different keys
    # we first convert the dicts to json strings. A pity we can't do something inside duckdb and avoid the dataframe
    # copy and transform in python.

    df = df.copy()

    # Snowflake loads staged half floats as their two-byte IEEE-754 representation.
    for col in df.select_dtypes(include=["float16"]).columns:
        df[col] = df[col].map(lambda value: struct.pack("<e", value) if pd.notna(value) else None)

    # Identify columns of type object
    object_cols = df.select_dtypes(include=["object"]).columns

    # Apply json.dumps to these columns
    for col in object_cols:
        # don't jsonify string
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    escaped_cols = ",".join(f'"{col}"' for col in df.columns.to_list())
    duck_conn.execute(f"INSERT INTO {table_name}({escaped_cols}) SELECT * FROM df")

    return duck_conn.fetchall()[0][0]

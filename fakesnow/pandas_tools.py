from __future__ import annotations

import json
from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional

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
    Optional[str],
    Optional[int],
    Optional[int],
    Optional[str],
]

WritePandasResult = tuple[
    bool,
    int,
    int,
    Sequence[CopyResult],
]


def sql_type(dtype: np.dtype) -> str:
    if str(dtype) == "int64":
        return "NUMBER"
    elif str(dtype) == "object":
        return "VARCHAR"
    else:
        raise NotImplementedError(f"sql_type {dtype=}")


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
    **kwargs: Any,
) -> WritePandasResult:
    name = table_name
    if schema:
        name = f"{schema}.{name}"
    if database:
        name = f"{database}.{name}"

    if auto_create_table:
        cols = [f"{c} {sql_type(t)}" for c, t in df.dtypes.to_dict().items()]

        conn.cursor().execute(f"CREATE TABLE IF NOT EXISTS {name} ({','.join(cols)})")

    count = _insert_df(conn._duck_conn, df, name)  # noqa: SLF001

    # mocks https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
    mock_copy_results = [("fakesnow/file0.txt", "LOADED", count, count, 1, 0, None, None, None, None)]

    # return success
    return (True, len(mock_copy_results), count, mock_copy_results)


def _insert_df(duck_conn: DuckDBPyConnection, df: pd.DataFrame, table_name: str) -> int:
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

    # Identify columns of type object
    object_cols = df.select_dtypes(include=["object"]).columns

    # Apply json.dumps to these columns
    for col in object_cols:
        # don't jsonify string
        df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, (dict, list)) else x)

    escaped_cols = ",".join(f'"{col}"' for col in df.columns.to_list())
    duck_conn.execute(f"INSERT INTO {table_name}({escaped_cols}) SELECT * FROM df")

    return duck_conn.fetchall()[0][0]

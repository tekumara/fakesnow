from __future__ import annotations

from collections.abc import Sequence
from typing import TYPE_CHECKING, Any, Literal, Optional

import numpy as np

if TYPE_CHECKING:
    import pandas as pd

    from fakesnow.conn import FakeSnowflakeConnection

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

    count = conn._insert_df(df, name)  # noqa: SLF001

    # mocks https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output
    mock_copy_results = [("fakesnow/file0.txt", "LOADED", count, count, 1, 0, None, None, None, None)]

    # return success
    return (True, len(mock_copy_results), count, mock_copy_results)

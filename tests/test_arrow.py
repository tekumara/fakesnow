import pandas as pd
import pyarrow as pa

from fakesnow.arrow import to_ipc, with_sf_metadata


def test_with_sf_metadata() -> None:
    assert with_sf_metadata(pa.schema([pa.field("'name'", pa.string())])) == pa.schema(
        [pa.field("'name'", pa.string(), metadata={"logicalType": "TEXT"})]
    )


def test_ipc_writes_sf_metadata() -> None:
    df = pd.DataFrame.from_dict(
        {
            "'HELLO WORLD'": ["hello world"],
        }
    )

    table = pa.Table.from_pandas(df)
    table_bytes = to_ipc(table)

    batch = next(iter(pa.ipc.open_stream(table_bytes)))

    # field and schema metadata is ignored
    assert pa.table(batch) == table
    assert batch.schema.field(0).metadata == {b"logicalType": b"TEXT"}, "Missing Snowflake field metadata"

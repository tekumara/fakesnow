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

    wbatch = pa.RecordBatch.from_pandas(df)
    wbytes = to_ipc(wbatch)

    rbatch = next(iter(pa.ipc.open_stream(wbytes)))

    # field and schema metadata is ignored
    assert rbatch == wbatch
    assert rbatch.schema.field(0).metadata == {b"logicalType": b"TEXT"}, "Missing Snowflake field metadata"

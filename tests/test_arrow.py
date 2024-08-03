from base64 import b64decode

import pandas as pd
import pyarrow as pa

from fakesnow.arrow import to_ipc, with_sf_metadata


def test_with_sf_metadata() -> None:
    # see https://arrow.apache.org/docs/python/api/datatypes.html
    def f(t: pa.DataType) -> dict:
        return with_sf_metadata(pa.schema([pa.field(str(t), t)])).field(0).metadata

    assert f(pa.string()) == {b"logicalType": b"TEXT"}
    assert f(pa.decimal128(10, 2)) == {b"logicalType": b"FIXED", b"precision": b"10", b"scale": b"2"}


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


def test_read_base64() -> None:
    # select to_decimal('12.3456', 10,2)
    rowset_b64 = "/////5gBAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAYAAAAAAASABgACAAAAAcADAAAABAAFAASAAAAAAAAAhQAAAA0AQAACAAAACgAAAAAAAAAGwAAAFRPX0RFQ0lNQUwoJzEyLjM0NTYnLCAxMCwyKQAGAAAA0AAAAKAAAAB8AAAAVAAAACwAAAAEAAAAUP///xAAAAAEAAAAAQAAAFQAAAAJAAAAZmluYWxUeXBlAAAAdP///xAAAAAEAAAAAQAAADIAAAAKAAAAYnl0ZUxlbmd0aAAAmP///xAAAAAEAAAAAQAAADAAAAAKAAAAY2hhckxlbmd0aAAAvP///xAAAAAEAAAAAQAAADIAAAAFAAAAc2NhbGUAAADc////EAAAAAQAAAACAAAAMTAAAAkAAABwcmVjaXNpb24AAAAIAAwABAAIAAgAAAAUAAAABAAAAAUAAABGSVhFRAAAAAsAAABsb2dpY2FsVHlwZQAIAAwACAAHAAgAAAAAAAABEAAAAAAAAAD/////iAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADBAAYAAAACAAAAAAAAAAAAAoAGAAMAAQACAAKAAAAPAAAABAAAAABAAAAAAAAAAAAAAACAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAgAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAADTBAAAAAAAAA=="  # noqa: E501

    f = b64decode(rowset_b64)
    reader = pa.ipc.open_stream(f)

    batch = next(reader)

    field = batch.schema.field(0)
    assert field == pa.field(name="TO_DECIMAL('12.3456', 10,2)", type=pa.int16(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"FIXED",
        b"precision": b"10",
        b"scale": b"2",
        b"charLength": b"0",
        b"byteLength": b"2",
        b"finalType": b"T",
    }

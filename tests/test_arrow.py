from base64 import b64decode

import pandas as pd
import pyarrow as pa

from fakesnow.arrow import to_ipc, with_sf_metadata
from fakesnow.types import ColumnInfo, describe_as_rowtype


def rowtype(types: list[str]) -> list[ColumnInfo]:
    return describe_as_rowtype([("test", typ, None, None, None, None) for typ in types])


def test_with_sf_metadata() -> None:
    # see https://arrow.apache.org/docs/python/api/datatypes.html
    def f(t: pa.DataType, rowtype: list[ColumnInfo]) -> dict:
        return with_sf_metadata(pa.schema([pa.field(str(t), t)]), rowtype).field(0).metadata

    assert f(pa.string(), rowtype(["VARCHAR"])) == {
        b"logicalType": b"TEXT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"16777216",
    }
    assert f(pa.decimal128(10, 2), rowtype(["DECIMAL(10,2)"])) == {
        b"logicalType": b"FIXED",
        b"precision": b"10",
        b"scale": b"2",
        b"charLength": b"0",
    }


def test_ipc_writes_sf_metadata() -> None:
    df = pd.DataFrame.from_dict(
        {
            "'HELLO WORLD'": ["hello world"],
        }
    )

    table = pa.Table.from_pandas(df)
    table_bytes = to_ipc(table, rowtype(["VARCHAR"]))

    batch = next(iter(pa.ipc.open_stream(table_bytes)))

    # field and schema metadata is ignored
    assert pa.table(batch) == table
    assert batch.schema.field(0).metadata == {
        b"logicalType": b"TEXT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"16777216",
    }


def test_read_base64_from_actual_snowflake_result() -> None:
    # select true, 1::int, 2.0::float, to_decimal('12.3456', 10,2), 'hello'
    rowset_b64 = "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"  # noqa: E501
    f = b64decode(rowset_b64)
    reader = pa.ipc.open_stream(f)

    batch = next(reader)

    field = batch.schema.field(0)
    assert field == pa.field(name="TRUE", type=pa.bool_())
    assert field.metadata == {
        b"logicalType": b"BOOLEAN",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"0",
        b"byteLength": b"1",
        b"finalType": b"T",
    }

    field = batch.schema.field(1)
    assert field == pa.field(name="1::INT", type=pa.int8(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"FIXED",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"0",
        b"byteLength": b"1",
        b"finalType": b"T",
    }

    field = batch.schema.field(2)
    assert field == pa.field(name="2.0::FLOAT", type=pa.float64(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"REAL",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"0",
        b"byteLength": b"8",
        b"finalType": b"T",
    }

    field = batch.schema.field(3)
    assert field == pa.field(name="TO_DECIMAL('12.3456', 10,2)", type=pa.int16(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"FIXED",
        b"precision": b"10",
        b"scale": b"2",
        b"charLength": b"0",
        b"byteLength": b"2",
        b"finalType": b"T",
    }

    field = batch.schema.field(4)
    assert field == pa.field(name="'HELLO'", type=pa.string(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"TEXT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"5",
        b"byteLength": b"20",
        b"finalType": b"T",
    }

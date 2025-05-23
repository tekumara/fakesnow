# ruff: noqa: E501

import datetime
from base64 import b64decode

import pandas as pd
import pyarrow as pa
import pytz

from fakesnow.arrow import timestamp_to_sf_struct, to_ipc, to_sf_schema
from fakesnow.rowtype import ColumnInfo, describe_as_rowtype


def rowtype(types: list[str]) -> list[ColumnInfo]:
    return describe_as_rowtype([("test", type_, None, None, None, None) for type_ in types])


def test_with_sf_schema() -> None:
    # see https://arrow.apache.org/docs/python/api/datatypes.html
    def f(t: pa.DataType, rowtype: list[ColumnInfo]) -> dict:
        return to_sf_schema(pa.schema([pa.field(str(t), t)]), rowtype).field(0).metadata  # pyright: ignore reportReturnType

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


def test_ipc() -> None:
    df = pd.DataFrame.from_dict(
        {
            "'HELLO WORLD'": ["hello world"],
        }
    )

    table = pa.Table.from_pandas(df)
    table_bytes = to_ipc(table)

    batch = next(iter(pa.ipc.open_stream(table_bytes)))

    assert pa.table(batch) == table


# TODO: cover to_sf


def test_timestamp_to_sf_struct():
    timestamp = datetime.datetime(2013, 4, 5, 1, 2, 3, 123456, tzinfo=pytz.utc)
    # this is what duckdb returns
    timestamp_array = pa.array([timestamp], type=pa.timestamp("us", tz="UTC"))

    result = timestamp_to_sf_struct(timestamp_array)

    assert result == pa.StructArray.from_arrays(
        arrays=[
            pa.array([1365123723], type=pa.int64()),
            pa.array([123456000], type=pa.int32()),
            pa.array([1440], type=pa.int32()),
        ],
        fields=[
            pa.field("epoch", nullable=False, type=pa.int64()),
            pa.field("fraction", nullable=False, type=pa.int32()),
            pa.field("timezone", nullable=False, type=pa.int32()),
        ],
    )

    timestamp = datetime.datetime(2013, 4, 5, 1, 2, 3, 123456)
    # this is what duckdb returns
    timestamp_array = pa.array([timestamp], type=pa.timestamp("us"))

    result = timestamp_to_sf_struct(timestamp_array)

    assert result == pa.StructArray.from_arrays(
        arrays=[
            pa.array([1365123723], type=pa.int64()),
            pa.array([123456000], type=pa.int32()),
        ],
        fields=[
            pa.field("epoch", nullable=False, type=pa.int64()),
            pa.field("fraction", nullable=False, type=pa.int32()),
        ],
    )


def test_read_base64_from_actual_snowflake_result() -> None:
    # select
    #         true, 1::int, 2.0::float, to_decimal('12.3456', 10,2), 'hello', 'hello'::varchar(20),
    #         to_date('2018-04-15'), to_time('04:15:29.123456'), to_timestamp_tz('2013-04-05 01:02:03.123456'), to_timestamp_ntz('2013-04-05 01:02:03.123456'),
    #         X'41424320E29D84', ARRAY_CONSTRUCT('foo'), OBJECT_CONSTRUCT('k','v1'), 'vary'::VARIANT    rowset_b64 = "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",
    #         array_size(parse_json('["a","b"]')) as ARRAY_SIZE
    rowset_b64 = "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"

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

    field = batch.schema.field(5)
    assert field == pa.field(name="'HELLO'::VARCHAR(20)", type=pa.string(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"TEXT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"20",
        b"byteLength": b"80",
        b"finalType": b"T",
    }

    field = batch.schema.field(6)
    assert field == pa.field(name="TO_DATE('2018-04-15')", type=pa.date32(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"DATE",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"0",
        b"byteLength": b"4",
        b"finalType": b"T",
    }

    field = batch.schema.field(7)
    assert field == pa.field(name="TO_TIME('04:15:29.123456')", type=pa.int64(), nullable=False)
    assert field.metadata == {
        b"logicalType": b"TIME",
        b"precision": b"0",
        b"scale": b"9",
        b"charLength": b"0",
        b"byteLength": b"8",
        b"finalType": b"T",
    }
    assert batch[7] == pa.array([15329123456000], type=pa.int64())

    field = batch.schema.field(8)
    assert field == pa.field(
        name="TO_TIMESTAMP_TZ('2013-04-05 01:02:03.123456')",
        type=pa.struct(
            [
                pa.field("epoch", pa.int64(), nullable=False),
                pa.field("fraction", pa.int32(), nullable=False),
                pa.field("timezone", pa.int32(), nullable=False),
            ]
        ),
        nullable=False,
    )
    assert field.metadata == {
        b"logicalType": b"TIMESTAMP_TZ",
        b"precision": b"0",
        b"scale": b"9",
        b"charLength": b"0",
        b"byteLength": b"16",
        b"finalType": b"T",
    }
    assert batch[8] == pa.StructArray.from_arrays(
        arrays=[
            pa.array([1365123723], type=pa.int64()),
            pa.array([123456000], type=pa.int32()),
            pa.array([1440], type=pa.int32()),
        ],
        fields=[
            pa.field("epoch", nullable=False, type=pa.int64()),
            pa.field("fraction", nullable=False, type=pa.int32()),
            pa.field("timezone", nullable=False, type=pa.int32()),
        ],
    )

    field = batch.schema.field(9)
    assert field == pa.field(
        name="TO_TIMESTAMP_NTZ('2013-04-05 01:02:03.123456')",
        type=pa.struct(
            [pa.field("epoch", pa.int64(), nullable=False), pa.field("fraction", pa.int32(), nullable=False)]
        ),
        nullable=False,
    )
    assert field.metadata == {
        b"logicalType": b"TIMESTAMP_NTZ",
        b"precision": b"0",
        b"scale": b"9",
        b"charLength": b"0",
        b"byteLength": b"16",
        b"finalType": b"T",
    }
    assert batch[9] == pa.StructArray.from_arrays(
        arrays=[
            pa.array([1365123723], type=pa.int64()),
            pa.array([123456000], type=pa.int32()),
        ],
        fields=[
            pa.field("epoch", nullable=False, type=pa.int64()),
            pa.field("fraction", nullable=False, type=pa.int32()),
        ],
    )

    field = batch.schema.field(10)
    assert field == pa.field(
        name="X'41424320E29D84'",
        type=pa.binary(),
        nullable=False,
    )
    assert field.metadata == {
        b"logicalType": b"BINARY",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"7",
        b"byteLength": b"7",
        b"finalType": b"T",
    }

    field = batch.schema.field(11)
    assert field == pa.field(name="ARRAY_CONSTRUCT('FOO')", type=pa.string())
    assert field.metadata == {
        b"logicalType": b"ARRAY",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"16777216",
        b"byteLength": b"16777216",
        b"finalType": b"T",
    }

    field = batch.schema.field(12)
    assert field == pa.field(name="OBJECT_CONSTRUCT('K','V1')", type=pa.string())
    assert field.metadata == {
        b"logicalType": b"OBJECT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"16777216",
        b"byteLength": b"16777216",
        b"finalType": b"T",
    }

    field = batch.schema.field(13)
    assert field == pa.field(
        name="'VARY'::VARIANT",
        type=pa.string(),
        nullable=False,
    )
    assert field.metadata == {
        b"logicalType": b"VARIANT",
        b"precision": b"38",
        b"scale": b"0",
        b"charLength": b"16777216",
        b"byteLength": b"16777216",
        b"finalType": b"T",
    }

    field = batch.schema.field(14)
    assert field == pa.field(name="ARRAY_SIZE", type=pa.int32())
    assert field.metadata == {
        b"logicalType": b"FIXED",
        b"precision": b"9",
        b"scale": b"0",
        b"charLength": b"0",
        b"byteLength": b"4",
        b"finalType": b"T",
    }

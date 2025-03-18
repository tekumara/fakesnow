from __future__ import annotations

from typing import cast

import pyarrow as pa
import pyarrow.compute as pc

from fakesnow.rowtype import ColumnInfo


def to_sf_schema(schema: pa.Schema, rowtype: list[ColumnInfo]) -> pa.Schema:
    # expected by the snowflake connector
    # uses rowtype to populate metadata, rather than the arrow schema type, for consistency with
    # rowtype returned in the response

    assert len(schema) == len(rowtype), f"schema and rowtype must be same length but f{len(schema)=} f{len(rowtype)=}"

    # see https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowTableIterator.cpp#L32
    # and https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/SnowflakeType.cpp#L10

    def sf_field(field: pa.Field, c: ColumnInfo) -> pa.Field:
        if isinstance(field.type, pa.TimestampType):
            # snowflake uses a struct to represent timestamps, see timestamp_to_sf_struct
            fields = [pa.field("epoch", pa.int64(), nullable=False), pa.field("fraction", pa.int32(), nullable=False)]
            if field.type.tz:
                fields.append(pa.field("timezone", nullable=False, type=pa.int32()))
            field = field.with_type(pa.struct(fields))
        elif isinstance(field.type, pa.Time64Type):
            field = field.with_type(pa.int64())
        elif pa.types.is_uint64(field.type):
            # snowflake-python-connector expects signed ints
            # see https://github.com/snowflakedb/snowflake-connector-python/blob/5d7064c7f3f756792c1f6252bf5c9d807e4307e8/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowChunkIterator.cpp#L187
            field = field.with_type(pa.int64())

        return field.with_metadata(
            {
                "logicalType": c["type"].upper(),
                # required for FIXED type see
                # https://github.com/snowflakedb/snowflake-connector-python/blob/416ff57/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowChunkIterator.cpp#L147
                "precision": str(c["precision"] or 38),
                "scale": str(c["scale"] or 0),
                "charLength": str(c["length"] or 0),
            }
        )

    fms = [sf_field(schema.field(i), c) for i, c in enumerate(rowtype)]
    return pa.schema(fms)


def to_ipc(table: pa.Table) -> pa.Buffer:
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"{len(batches)} batches")
    batch = batches[0]

    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_batch(batch)

    return sink.getvalue()


def to_sf(table: pa.Table, rowtype: list[ColumnInfo]) -> pa.Table:
    def to_sf_col(col: pa.ChunkedArray) -> pa.Array | pa.ChunkedArray:
        if pa.types.is_timestamp(col.type):
            return timestamp_to_sf_struct(col)
        elif pa.types.is_time(col.type):
            # as nanoseconds
            return pc.multiply(col.cast(pa.int64()), 1000)  # type: ignore https://github.com/zen-xu/pyarrow-stubs/issues/44
        return col

    return pa.Table.from_arrays([to_sf_col(c) for c in table.columns], schema=to_sf_schema(table.schema, rowtype))


def timestamp_to_sf_struct(ts: pa.Array | pa.ChunkedArray) -> pa.Array:
    if isinstance(ts, pa.ChunkedArray):
        # combine because pa.StructArray.from_arrays doesn't support ChunkedArray
        ts = cast(pa.Array, ts.combine_chunks())  # see https://github.com/zen-xu/pyarrow-stubs/issues/46

    if not isinstance(ts.type, pa.TimestampType):
        raise ValueError(f"Expected TimestampArray, got {type(ts)}")

    # Round to seconds, ie: strip subseconds
    tsa_without_us = pc.floor_temporal(ts, unit="second")  # type: ignore https://github.com/zen-xu/pyarrow-stubs/issues/45
    epoch = pc.divide(tsa_without_us.cast(pa.int64()), 1_000_000)

    # Calculate fractional part as nanoseconds
    fraction = pc.multiply(pc.subsecond(ts), 1_000_000_000).cast(pa.int32())  # type: ignore

    if ts.type.tz:
        assert ts.type.tz == "UTC", f"Timezone {ts.type.tz} not yet supported"
        timezone = pa.array([1440] * len(ts), type=pa.int32())

        return pa.StructArray.from_arrays(
            arrays=[epoch, fraction, timezone],
            fields=[
                pa.field("epoch", nullable=False, type=pa.int64()),
                pa.field("fraction", nullable=False, type=pa.int32()),
                pa.field("timezone", nullable=False, type=pa.int32()),
            ],
        )
    else:
        return pa.StructArray.from_arrays(
            arrays=[epoch, fraction],
            fields=[
                pa.field("epoch", nullable=False, type=pa.int64()),
                pa.field("fraction", nullable=False, type=pa.int32()),
            ],
        )

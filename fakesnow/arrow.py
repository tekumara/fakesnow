import pyarrow as pa

from fakesnow.types import ColumnInfo


def with_sf_metadata(schema: pa.Schema, rowtype: list[ColumnInfo]) -> pa.Schema:
    # expected by the snowflake connector
    # uses rowtype to populate metadata, rather than the arrow schema type, for consistency with
    # rowtype returned in the response

    assert len(schema) == len(rowtype), f"schema and rowtype must be same length but f{len(schema)=} f{len(rowtype)=}"

    # see https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowTableIterator.cpp#L32
    # and https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/SnowflakeType.cpp#L10

    fms = [
        schema.field(i).with_metadata(
            {
                "logicalType": c["type"].upper(),
                # required for FIXED type see
                # https://github.com/snowflakedb/snowflake-connector-python/blob/416ff57/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowChunkIterator.cpp#L147
                "precision": str(c["precision"] or 38),
                "scale": str(c["scale"] or 0),
                "charLength": str(c["length"] or 0),
            }
        )
        for i, c in enumerate(rowtype)
    ]
    return pa.schema(fms)


def to_ipc(table: pa.Table, rowtype: list[ColumnInfo]) -> pa.Buffer:
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"{len(batches)} batches")
    batch = batches[0]

    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, with_sf_metadata(table.schema, rowtype)) as writer:
        writer.write_batch(batch)

    return sink.getvalue()

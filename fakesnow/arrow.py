import pyarrow as pa

arrow_type_to_sf = {pa.string(): "TEXT"}


def with_sf_metadata(schema: pa.Schema) -> pa.Schema:
    # see https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowTableIterator.cpp#L32
    # and https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/SnowflakeType.cpp#L10
    fms = []
    for i, t in enumerate(schema.types):
        f = schema.field(i)
        fm = f.with_metadata({"logicalType": arrow_type_to_sf[t]})
        fms.append(fm)
    return pa.schema(fms)


def to_ipc(b: pa.RecordBatch) -> bytes:
    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, with_sf_metadata(b.schema)) as writer:
        writer.write_batch(b)

    return sink.getvalue()

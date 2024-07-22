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


def to_ipc(table: pa.Table) -> bytes:
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"{len(batches)} batches")
    batch = batches[0]

    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, with_sf_metadata(batch.schema)) as writer:
        writer.write_batch(batch)

    return sink.getvalue()

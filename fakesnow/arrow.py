import pyarrow as pa


def with_sf_metadata(schema: pa.Schema) -> pa.Schema:
    # see https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowTableIterator.cpp#L32
    # and https://github.com/snowflakedb/snowflake-connector-python/blob/e9393a6/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/SnowflakeType.cpp#L10
    fms = []
    for i, t in enumerate(schema.types):
        f = schema.field(i)

        # TODO: precision, scale, charLength etc. for all types

        if t == pa.bool_():
            fm = f.with_metadata({"logicalType": "BOOLEAN"})
        elif t == pa.int64():
            # scale and precision required, see here
            # https://github.com/snowflakedb/snowflake-connector-python/blob/416ff57/src/snowflake/connector/nanoarrow_cpp/ArrowIterator/CArrowChunkIterator.cpp#L147
            fm = f.with_metadata({"logicalType": "FIXED", "precision": "38", "scale": "0"})
        elif t == pa.float64():
            fm = f.with_metadata({"logicalType": "REAL"})
        elif isinstance(t, pa.Decimal128Type):
            fm = f.with_metadata({"logicalType": "FIXED", "precision": str(t.precision), "scale": str(t.scale)})
        elif t == pa.string():
            fm = f.with_metadata({"logicalType": "TEXT"})
        else:
            raise NotImplementedError(f"Unsupported Arrow type: {t}")
        fms.append(fm)
    return pa.schema(fms)


def to_ipc(table: pa.Table) -> pa.Buffer:
    batches = table.to_batches()
    if len(batches) != 1:
        raise NotImplementedError(f"{len(batches)} batches")
    batch = batches[0]

    sink = pa.BufferOutputStream()

    with pa.ipc.new_stream(sink, with_sf_metadata(batch.schema)) as writer:
        writer.write_batch(batch)

    return sink.getvalue()

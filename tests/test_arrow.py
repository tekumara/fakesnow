import threading
from base64 import b64decode, b64encode
from collections.abc import Iterator
from time import sleep
from typing import Callable

import pandas as pd
import pyarrow as pa

from fakesnow.arrow import with_sf_metadata


def test_with_sf_metadata() -> None:
    assert with_sf_metadata(pa.schema([pa.field("'name'", pa.string())])) == pa.schema(
        [pa.field("'name'", pa.string(), metadata={"logicalType": "TEXT"})]
    )


def test_ipc() -> None:
    import pyarrow as pa
    import pyarrow.ipc as ipc

    # sf
    # schema:
    # 'HELLO WORLD': string
    #   -- field metadata --
    #   physicalType: 'LOB'
    #   byteLength: '44'
    #   logicalType: 'TEXT'
    #   charLength: '11'
    sf_rowset64 = "/////0ABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAABAAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAEAAAAYAAAAAAASABgAFAATABIADAAAAAgABAASAAAAFAAAAMQAAADIAAAAAAAFAcQAAAAEAAAAiAAAAFgAAAAsAAAABAAAAJD///8IAAAADAAAAAIAAAAxMQAACgAAAGNoYXJMZW5ndGgAALT///8IAAAAEAAAAAQAAABURVhUAAAAAAsAAABsb2dpY2FsVHlwZQDc////CAAAAAwAAAACAAAANDQAAAoAAABieXRlTGVuZ3RoAAAIAAwACAAEAAgAAAAIAAAADAAAAAMAAABMT0IADAAAAHBoeXNpY2FsVHlwZQAAAAAAAAAABAAEAAQAAAANAAAAJ0hFTExPIFdPUkxEJwAAAP////+YAAAAFAAAAAAAAAAMABYADgAVABAABAAMAAAAIAAAAAAAAAAAAAQAEAAAAAADCgAYAAwACAAEAAoAAAAUAAAASAAAAAEAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAAAIAAAAAAAAABAAAAAAAAAACwAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAALAAAAaGVsbG8gd29ybGQAAAAAAP////8AAAAA"
    # duckdb arrow table
    # schema: 'hello world': string
    rowset64 = "/////3gAAAAQAAAAAAAKAAwABgAFAAgACgAAAAABBAAMAAAACAAIAAAABAAIAAAABAAAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACQAAAAEAAAAAAAAAA0AAAAnaGVsbG8gd29ybGQnAAAABAAEAAQAAAD/////mAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADBAAYAAAAGAAAAAAAAAAAAAoAGAAMAAQACAAKAAAATAAAABAAAAABAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAsAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAsAAABoZWxsbyB3b3JsZAAAAAAA/////wAAAAA="
    # duckdb pandas
    # rowset64 = "/////3ACAAAQAAAAAAAKAA4ABgAFAAgACgAAAAABBAAQAAAAAAAKAAwAAAAEAAgACgAAAPgBAAAEAAAAAQAAAAwAAAAIAAwABAAIAAgAAADQAQAABAAAAMEBAAB7ImluZGV4X2NvbHVtbnMiOiBbeyJraW5kIjogInJhbmdlIiwgIm5hbWUiOiBudWxsLCAic3RhcnQiOiAwLCAic3RvcCI6IDEsICJzdGVwIjogMX1dLCAiY29sdW1uX2luZGV4ZXMiOiBbeyJuYW1lIjogbnVsbCwgImZpZWxkX25hbWUiOiBudWxsLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IHsiZW5jb2RpbmciOiAiVVRGLTgifX1dLCAiY29sdW1ucyI6IFt7Im5hbWUiOiAiJ2hlbGxvIHdvcmxkJyIsICJmaWVsZF9uYW1lIjogIidoZWxsbyB3b3JsZCciLCAicGFuZGFzX3R5cGUiOiAidW5pY29kZSIsICJudW1weV90eXBlIjogIm9iamVjdCIsICJtZXRhZGF0YSI6IG51bGx9XSwgImNyZWF0b3IiOiB7ImxpYnJhcnkiOiAicHlhcnJvdyIsICJ2ZXJzaW9uIjogIjE2LjEuMCJ9LCAicGFuZGFzX3ZlcnNpb24iOiAiMi4yLjIifQAAAAYAAABwYW5kYXMAAAEAAAAUAAAAEAAUAAgABgAHAAwAAAAQABAAAAAAAAEFEAAAACQAAAAEAAAAAAAAAA0AAAAnaGVsbG8gd29ybGQnAAAABAAEAAQAAAD/////mAAAABQAAAAAAAAADAAWAAYABQAIAAwADAAAAAADBAAYAAAAGAAAAAAAAAAAAAoAGAAMAAQACAAKAAAATAAAABAAAAABAAAAAAAAAAAAAAADAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAACAAAAAAAAAAIAAAAAAAAAAsAAAAAAAAAAAAAAAEAAAABAAAAAAAAAAAAAAAAAAAAAAAAAAsAAABoZWxsbyB3b3JsZAAAAAAA/////wAAAAA="

    f = b64decode(sf_rowset64)
    reader = ipc.open_stream(f)
    sf_batches = list(reader)
    print(sf_batches[0].schema)

    f = b64decode(rowset64)
    reader = ipc.open_stream(f)
    batches = list(reader)

    p = batches[0].to_pandas()

    print(p)
    print(batches[0].schema)
    pd.testing.assert_frame_equal(
        p,
        pd.DataFrame.from_dict(
            {
                "'HELLO WORLD'": ["hello world"],
            }
        ),
    )

    batch = pa.RecordBatch.from_pandas(p)
    sink = pa.BufferOutputStream()

    with ipc.new_stream(sink, batch.schema) as writer:
        writer.write_batch(batch)

    buf = sink.getvalue()
    print(b64encode(buf))

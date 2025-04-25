#!/usr/bin/env python
import base64
import sys

import pyarrow as pa


def dump_field_metadata(field: pa.Field, index: int) -> None:
    """Dump metadata for a single field."""
    print(f"Field {index}: {field.name}")
    print(f"  Type: {field.type}")
    print(f"  Nullable: {field.nullable}")
    print("  Metadata:")
    assert field.metadata
    for key, value in field.metadata.items():
        try:
            print(f"    {key.decode('utf-8')}: {value.decode('utf-8')}")
        except UnicodeDecodeError:  # noqa: PERF203
            print(f"    {key.decode('utf-8')}: <binary data>")
    print()


def main() -> None:
    if len(sys.argv) > 1:
        print("Usage: python dump_rowset_metadata.py < base64_encoded_file")
        print("   or: cat base64_encoded_file | python dump_rowset_metadata.py")
        print()
        print("Dump pyarrow metadata for a base64-encoded rowset.")
        sys.exit(1)

    # Read base64 input from stdin
    rowset_b64 = sys.stdin.read().strip()

    try:
        # Decode base64
        data = base64.b64decode(rowset_b64)

        # Parse with PyArrow
        reader = pa.ipc.open_stream(data)

    except Exception as e:
        full_class_name = f"{e.__module__}.{e.__class__.__name__}"
        print(f"Error processing rowset: {full_class_name} {e}")
        sys.exit(1)

    # Get the first batch
    batch = next(iter(reader))

    print(f"Total fields: {batch.num_columns}")
    print("=" * 50)

    # Dump metadata for each field
    for i, field in enumerate(batch.schema):
        dump_field_metadata(field, i)

        # Also print a sample of the array data
        print(f"  Batch data: {batch[i]}")
        print(f"  Batch data type: {type(batch[i])}")
        print("=" * 50)


if __name__ == "__main__":
    main()

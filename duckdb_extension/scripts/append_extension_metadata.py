from __future__ import annotations

import argparse
import shutil
from pathlib import Path

_FIELD_WIDTH = 32
_SIGNATURE_WIDTH = 256


def _start_signature() -> bytes:
    # This prefix matches DuckDB's extension-template-c metadata footer.
    return b"".join(
        [
            (0).to_bytes(1, byteorder="big"),
            (147).to_bytes(1, byteorder="big"),
            (4).to_bytes(1, byteorder="big"),
            (16).to_bytes(1, byteorder="big"),
            b"duckdb_signature",
            (128).to_bytes(1, byteorder="big"),
            (4).to_bytes(1, byteorder="big"),
        ]
    )


def _padded_byte_string(value: str) -> bytes:
    encoded = value.encode("ascii")
    return encoded + b"\x00" * (_FIELD_WIDTH - len(encoded))


def _read_text_file(path: str, description: str) -> str:
    value = Path(path).read_text().strip()
    if not value:
        raise ValueError(f"{description} file is empty: {path}")
    return value


def main() -> None:
    parser = argparse.ArgumentParser(description="Append extension metadata to loadable DuckDB extensions")
    parser.add_argument("-l", "--library-file", required=True, help="Path to the raw shared library")
    parser.add_argument("-n", "--extension-name", required=True, help="Extension name to use")
    parser.add_argument("-o", "--out-file", default="", help="Explicit path for the output file")
    parser.add_argument("-p", "--duckdb-platform", help="The DuckDB platform to encode")
    parser.add_argument("-pf", "--duckdb-platform-file", help="File containing the DuckDB platform to encode")
    parser.add_argument(
        "-dv",
        "--duckdb-version",
        required=True,
        help="DuckDB version or C API version to encode, depending on ABI type",
    )
    parser.add_argument("-ev", "--extension-version", help="The extension version to encode")
    parser.add_argument("-evf", "--extension-version-file", help="File containing the extension version to encode")
    parser.add_argument("--abi-type", default="C_STRUCT", help="The ABI type to encode")
    args = parser.parse_args()

    output_file = args.out_file or f"{args.extension_name}.duckdb_extension"
    output_file_tmp = f"{output_file}.tmp"

    if args.duckdb_platform:
        platform = args.duckdb_platform
    elif args.duckdb_platform_file:
        platform = _read_text_file(args.duckdb_platform_file, "Platform")
    else:
        raise ValueError("Specify either --duckdb-platform or --duckdb-platform-file")

    if args.extension_version:
        extension_version = args.extension_version
    elif args.extension_version_file:
        extension_version = _read_text_file(args.extension_version_file, "Extension version")
    else:
        raise ValueError("Specify either --extension-version or --extension-version-file")

    print("Creating extension binary:")
    print(f" - Input file: {args.library_file}")
    print(f" - Output file: {output_file}")
    shutil.copyfile(args.library_file, output_file_tmp)

    fields = [
        ("FIELD8 (unused)", ""),
        ("FIELD7 (unused)", ""),
        ("FIELD6 (unused)", ""),
        ("FIELD5 (abi_type)", args.abi_type),
        ("FIELD4 (extension_version)", extension_version),
        ("FIELD3 (duckdb_version)", args.duckdb_version),
        ("FIELD2 (duckdb_platform)", platform),
        ("FIELD1 (header signature)", "4"),
    ]

    print(" - Metadata:")
    with Path(output_file_tmp).open("ab") as file:
        file.write(_start_signature())
        for name, value in fields:
            print(f"   - {name:<27} = {value or 'EMPTY'}")
            file.write(_padded_byte_string(value))
        file.write(b"\x00" * _SIGNATURE_WIDTH)

    shutil.move(output_file_tmp, output_file)


if __name__ == "__main__":
    main()

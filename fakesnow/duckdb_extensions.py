from __future__ import annotations

from pathlib import Path

from duckdb import DuckDBPyConnection

_EXTENSION_NAME = "fakesnow.duckdb_extension"


def _extension_paths() -> list[Path]:
    package_dir = Path(__file__).resolve().parent
    repo_dir = package_dir.parent
    return [
        package_dir / "_duckdb_extension" / _EXTENSION_NAME,
        repo_dir / "duckdb_extension" / "build" / "release" / _EXTENSION_NAME,
        repo_dir / "duckdb_extension" / "build" / "debug" / _EXTENSION_NAME,
    ]


def load_fakesnow_extension(conn: DuckDBPyConnection) -> None:
    for path in _extension_paths():
        if path.exists():
            conn.execute(f"LOAD '{path}'")
            return

    paths = ", ".join(str(path) for path in _extension_paths())
    raise RuntimeError(
        "fakesnow DuckDB extension is not built. Run `make duckdb-extension` "
        f"or provide {_EXTENSION_NAME} at one of: {paths}"
    )

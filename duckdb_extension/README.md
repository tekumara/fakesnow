# fakesnow DuckDB extension

This extension uses DuckDB's stable C extension API to register fakesnow-specific functions.

Currently it registers `_fs_sum`, an aggregate used to emulate Snowflake's `SUM` typing for common scalar inputs:

- integer inputs return `BIGINT`
- `FLOAT`/`DOUBLE` inputs return `DOUBLE`
- `VARCHAR` inputs are parsed as `DOUBLE`, with invalid values ignored like `TRY_CAST(... AS DOUBLE)` inside `SUM`

Build from this directory:

```sh
make configure
make debug
```

The loadable extension is written to `build/debug/fakesnow.duckdb_extension`.

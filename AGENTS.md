# Agent instructions

## Snowflake-compatible result assertions

When a test compares JSON, `VARIANT`, `ARRAY`, or `OBJECT` results as strings, use the existing helpers in `tests.utils` so DuckDB output is normalised to Snowflake's formatting:

- Use `dindent(rows)` for `DictCursor` results.
- Use `indent(rows)` for tuple results.

Do not compare raw DuckDB JSON strings when the assertion is intended to match real Snowflake output.

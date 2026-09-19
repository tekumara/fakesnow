# Agent instructions

## Snowflake-compatible result assertions

When a test compares JSON, `VARIANT`, `ARRAY`, or `OBJECT` results as strings, use the existing helpers in `tests.utils` so DuckDB output is normalised to Snowflake's formatting:

- Use `dindent(rows)` for `DictCursor` results.
- Use `indent(rows)` for tuple results.

Do not compare raw DuckDB JSON strings when the assertion is intended to match real Snowflake output.

## Live behavior in tests

Tests should assert the live Snowflake behavior rather than the fake implementation's current limitation.

## Keep tests behavior-focused

Before adding or modifying a test, list the distinct behaviors it exercises. If more than one could fail independently, split the test unless the interaction between them is the explicit behavior under test.

## Keep behavior with its owner

Find where related rules already live and how similar cases are handled.
Prefer extending the existing implementation over spreading responsibility
across callers.

Before adding special handling to a caller, check whether the module
responsible for that behavior can handle it through its existing interface.
Prefer this over requiring callers to perform extra steps, even through
a shared helper.

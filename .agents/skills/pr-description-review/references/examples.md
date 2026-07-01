# PR Description Review Examples

Use these examples to calibrate review output quality or update the skill after observing accepted/rejected PR bodies.

## Happy Path: User Supplied Intent And Issue Link

User context:
- Intent: fixes DuckDB catalog error after `CREATE DATABASE`: `_fs_to_timestamp does not exist`.
- Intentional issue link: `Fixes #347`.

Acceptable body:

```markdown
### Why?

Fixes #347.

Fixes a DuckDB catalog error where Snowflake-compatible timestamp conversion can fail after creating a database with `CREATE DATABASE`:

`Scalar Function with name _fs_to_timestamp does not exist`

This change is needed so databases created during a fakesnow session support the same internal compatibility helpers as databases created at connection time.

### How?

Initialize newly created databases with fakesnow’s internal helper macros, so Snowflake SQL rewrites continue to work consistently after switching into a database created during the session.
```

Why it passes:
- Intent comes from the user-supplied error and issue reference.
- `How?` is high-level and short.
- No files, test plan, or speculative risks.

## Robust Variant: Intent Missing

User request:

```text
Create a PR description for this branch.
```

Required response:

```text
Before creating this PR, I need to understand the intent behind this change.

What problem does this solve, and why is this change needed?
```

Why it passes:
- The request describes the action but not the reason for the change.
- The agent stops instead of inventing motivation from the diff.

## Anti-Pattern And Correction

Bad body:

```markdown
## What

Updated `cursor.py` to call `macros.creation_sql(...)` and added `test_create_database_registers_macros` in `tests/test_connect.py`.

## Why

This improves macro handling.

## Test plan

- Ran pytest.
```

Violations:
- Lists files and narrates implementation.
- `Why` is vague and not a concrete user-supplied problem.
- Includes a forbidden test-plan section.

Corrected body when the user has supplied the catalog-error intent:

```markdown
### Why?

Fixes a DuckDB catalog error where Snowflake-compatible SQL can fail after creating and switching into a database during a fakesnow session.

### How?

Initialize newly created databases with fakesnow’s internal helper macros so rewritten Snowflake SQL has the compatibility helpers it expects.
```

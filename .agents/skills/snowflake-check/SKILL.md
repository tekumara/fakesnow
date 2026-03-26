---
name: snowflake-check
description: Inspect the Git diff from `main...HEAD` to identify tests it added or changed, validate the applicable runtime behavior against a real Snowflake instance, and produce a concise report of failures and behavioral differences. Use when a user asks to run tests from the current branch on real Snowflake, compare fake/local Snowflake behavior to actual Snowflake behavior, or verify whether newly added Snowflake-facing tests match live semantics.
---

# Snowflake Check

Identify the tests introduced by the current branch diff against `main`, separate live-applicable runtime checks from local-only unit tests, reproduce the runtime behavior against a real Snowflake account with isolated temporary objects, and report any divergence precisely.

Prefer reproducing the behavior in a small one-off script over patching the repo tests to force them through live credentials. Treat the repo tests as specifications to validate, not as the only executable surface.

## Workflow

1. Inspect the current branch diff.

- Run `git diff --stat --name-only main...HEAD` to identify touched files.
- Run `git diff main...HEAD -- <candidate test files>` to isolate the exact newly added test bodies.
- Note that the review scope is `main...HEAD` in the final report.

2. Classify each added test before running anything live.

- Read `tests/conftest.py` and any local fixtures to see whether the default path is fake Snowflake, an embedded server, or a real account.
- Mark tests as one of:
  - `live-runtime`: Executes SQL or connector behavior that can be reproduced against a real Snowflake account.
  - `local-only`: Pure transform/parser/unit tests that are not meaningful to run on Snowflake directly.
  - `mixed`: Has both a live-relevant behavior and a local-only assertion surface.
- Search for repo-specific live connection patterns in files like `tests/vcr/`, notebooks, or docs before inventing credentials or account parameters.

3. Discover the real Snowflake connection path that already exists.

- Check the current shell environment for `SNOWFLAKE_*` variables and any repo-local `.env` files.
- Search for hard-coded or example live connection dictionaries in the repo.
- Reuse the repo's known account, role, warehouse, database, schema, and authenticator values when possible.
- If the repo uses `externalbrowser`, expect sandboxed runs to fail before reaching Snowflake because the connector needs a local callback socket.

4. Reproduce the added runtime behavior against real Snowflake.

- Use the project venv if present, for example `.venv/bin/python`.
- Create isolated object names with `uuid` and prefer `temporary` tables or similarly disposable objects.
- Keep the live probe minimal and targeted to the behavior under test.
- Reproduce the exact query shape from the added test first. If that fails, keep the failure text.
- If the failure suggests a type restriction or Snowflake-specific syntax rule, run one focused follow-up probe to isolate the actual behavioral difference.
- Do not edit repo tests just to execute them against live credentials unless the user explicitly asks for that.

5. Handle sandbox and auth constraints explicitly.

- First attempt the live command in the sandbox.
- If the connector fails because of network restrictions, local callback binding, browser launch, or similar sandbox issues, rerun the same command with escalation.
- Keep the escalated command narrow: one script, one probe, no unrelated changes.

6. Produce the report.

- Report that the review scope was `main...HEAD` and the exact added tests reviewed.
- For each added test, state one of:
  - `passes on real Snowflake as written`
  - `fails on real Snowflake as written`
  - `not applicable to real Snowflake`
- For failures, include:
  - the test function name
  - the failing query or behavior
  - the exact or near-exact Snowflake error text
  - the inferred cause
  - the follow-up probe result, if one was needed
  - the behavioral difference relative to the fake/local test expectation
- Reference the relevant test file and line numbers in the final response.

## Command Pattern

Use this shape and adapt only the connection info and SQL:

```bash
.venv/bin/python - <<'PY'
import uuid
import snowflake.connector

conn_info = {
    "user": "...",
    "account": "...",
    "authenticator": "externalbrowser",
    "role": "...",
    "warehouse": "...",
    "database": "...",
    "schema": "...",
}

table = f"TMP_{uuid.uuid4().hex[:8].upper()}"
with snowflake.connector.connect(**conn_info) as conn:
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        cur.execute(f"create temporary table {table} (...)")
        cur.execute("...")
        print(cur.fetchall())
PY
```

## Heuristics

- A transform-only test, such as SQL rewrite assertions, is usually `not applicable to real Snowflake`.
- A fake-connector runtime test may still encode a live-relevant expectation; reproduce that expectation with a dedicated live query instead of trying to coerce pytest fixtures.
- When real Snowflake rejects the test's exact input, treat that as a real finding rather than silently adapting the test.
- Only run a second live probe when it sharpens the explanation of the failure.
- Prefer reporting one precise behavioral difference over a broad speculative explanation.

## Output Template

- `Diff scope:` `main...HEAD`
- `Added tests reviewed:` `<file:line>` entries
- `Live-applicable tests:` short list
- `Local-only tests:` short list
- `Findings:` one flat item per failure or confirmed behavior difference, each beginning with the test function name such as `test_object_agg:` or `test_object_agg_skips_nulls:`
- `Verification:` what command or one-off script was run and whether escalation was required

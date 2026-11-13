import datetime

import duckdb
import pytest

from fakesnow import macros


@pytest.mark.parametrize("input_sql", ["next_date()", "(select next_date())"])
def test_date_trunc_evaluates_input_once(input_sql: str):
    # Test DuckDB macro expansion directly: a volatile input must run only once,
    # including when a scalar subquery supplies the input.
    calls = 0

    def next_date() -> datetime.date:
        nonlocal calls
        calls += 1
        return datetime.date(2024, 5, 9)

    with duckdb.connect() as conn:
        conn.execute("attach ':memory:' as _fs_global")
        conn.execute(macros.fs_global_creation_sql())
        conn.create_function("next_date", next_date, return_type="DATE", side_effects=True)
        assert conn.execute(f"select _fs_global.main._fs_date_trunc('month', {input_sql})").fetchall() == [
            (datetime.date(2024, 5, 1),)
        ]
    assert calls == 1

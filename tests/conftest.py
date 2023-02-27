from typing import Iterator

import pytest
import snowflake.connector

pytest_plugins = ("fakesnow.fixtures",)


@pytest.fixture
def conn(_fake_snow: None) -> Iterator[snowflake.connector.SnowflakeConnection]:
    """
    Yield a snowflake connection once per session.
    """
    with snowflake.connector.connect(database="db1", schema="schema1") as c:
        yield c

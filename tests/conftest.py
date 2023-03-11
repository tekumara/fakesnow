from typing import Iterator

import pytest
import snowflake.connector

import fakesnow.fixtures

pytest_plugins = fakesnow.fixtures.__name__


@pytest.fixture
def conn(_fakesnow: None) -> Iterator[snowflake.connector.SnowflakeConnection]:
    """
    Yield a snowflake connection once per session.
    """
    with snowflake.connector.connect(database="db1", schema="schema1") as c:
        yield c

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


@pytest.fixture
def cur(conn: snowflake.connector.SnowflakeConnection) -> Iterator[snowflake.connector.cursor.SnowflakeCursor]:
    """
    Yield a snowflake cursor once per session.
    """
    with conn.cursor() as cur:
        yield cur

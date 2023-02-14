from typing import Iterator

import pytest
import snowflake.connector

import fakesnow


@pytest.fixture
def _fake_snow() -> Iterator[None]:
    with fakesnow.mock():
        yield


@pytest.fixture
def conn(_fake_snow: None) -> Iterator[snowflake.connector.SnowflakeConnection]:
    """
    Yield a snowflake connection once per session.
    """
    with snowflake.connector.connect(database="db1") as c:
        c.execute_string("CREATE SCHEMA IF NOT EXISTS schema1; USE SCHEMA schema1;")
        yield c

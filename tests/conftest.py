from typing import Iterator

import pytest
import snowflake.connector

import fakesnow


@pytest.fixture(scope="session")
def conn() -> Iterator[snowflake.connector.SnowflakeConnection]:
    """
    Yield a snowflake connection once per session.
    """
    with fakesnow.mock():
        with snowflake.connector.connect() as c:
            yield c

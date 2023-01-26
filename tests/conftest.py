from typing import Iterator

import pytest
import snowflake.connector

import mocksnow


@pytest.fixture(scope="session")
def conn() -> Iterator[snowflake.connector.SnowflakeConnection]:
    """
    Yield a snowflake connection once per session.
    """
    with mocksnow.mock():
        with snowflake.connector.connect() as conn:
            yield conn

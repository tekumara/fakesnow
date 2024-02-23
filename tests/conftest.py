from collections.abc import Iterator
from typing import cast

import pytest
import snowflake.connector
from sqlalchemy.engine import Engine, create_engine

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


@pytest.fixture
def dcur(conn: snowflake.connector.SnowflakeConnection) -> Iterator[snowflake.connector.cursor.DictCursor]:
    """
    Yield a snowflake cursor once per session.
    """
    with conn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        yield cast(snowflake.connector.cursor.DictCursor, cur)


@pytest.fixture
def snowflake_engine(_fakesnow: None) -> Engine:
    return create_engine("snowflake://user:password@account/db1/schema1")  # type: ignore

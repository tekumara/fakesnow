import os
import threading
from collections.abc import Iterator
from time import sleep
from typing import Callable, cast

import pytest
import snowflake.connector
import uvicorn
from sqlalchemy.engine import Engine, create_engine

import fakesnow.fixtures
import fakesnow.server

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
    return create_engine("snowflake://user:password@account/db1/schema1")


@pytest.fixture(scope="session")
def unused_port(unused_tcp_port_factory: Callable[[], int]) -> int:
    # unused_tcp_port_factory is from pytest-asyncio
    return unused_tcp_port_factory()


@pytest.fixture(scope="session")
def server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[dict]:
    port = unused_tcp_port_factory()
    server = uvicorn.Server(uvicorn.Config(fakesnow.server.app, port=port, log_level="info"))
    thread = threading.Thread(target=server.run, name="Server", daemon=True)
    thread.start()

    while not server.started:
        sleep(0.1)

    yield dict(
        user="fake",
        password="snow",
        account="fakesnow",
        host="localhost",
        port=port,
        protocol="http",
        # disable telemetry
        # isolate each session to a separate instance to avoid sharing tables between tests
        session_parameters={"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False, "FAKESNOW_DB_PATH": ":isolated:"},
    )

    server.should_exit = True
    # wait for server thread to end
    thread.join()


@pytest.fixture
def sconn(server: dict) -> Iterator[snowflake.connector.SnowflakeConnection]:
    with snowflake.connector.connect(
        **server,
        database="db1",
        schema="schema1",
        # disable infinite retries on error
        network_timeout=1,
    ) as c:
        yield c


@pytest.fixture
def scur(
    sconn: snowflake.connector.SnowflakeConnection,
) -> Iterator[snowflake.connector.cursor.SnowflakeCursor]:
    with sconn.cursor() as cur:
        yield cur


if os.getenv("TEST_SERVER"):
    # use server to run all tests
    conn = sconn  # type: ignore

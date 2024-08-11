import threading
from collections.abc import Iterator
from decimal import Decimal
from time import sleep
from typing import Callable

import pytest
import snowflake.connector
import uvicorn

import fakesnow.server


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
        session_parameters={"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False},
    )

    server.should_exit = True
    # wait for server thread to end
    thread.join()


def test_server_connect(server: dict) -> None:
    with (
        snowflake.connector.connect(
            **server,
            # disable infinite retries on error
            network_timeout=1,
        ) as conn1,
        conn1.cursor() as cur,
    ):
        cur.execute("select 'hello', to_decimal('12.3456', 10,2)")
        assert cur.fetchall() == [("hello", Decimal("12.35"))]


def test_server_abort_request(server: dict) -> None:
    with (
        snowflake.connector.connect(
            **server,
            # triggers an abort request
            network_timeout=0,
        ) as conn1,
        conn1.cursor() as cur,
    ):
        cur.execute("select 'will abort'")

import threading
from base64 import b64decode, b64encode
from collections.abc import Iterator
from time import sleep
from typing import Callable

import pandas as pd
import pytest
import snowflake.connector
import uvicorn

import fakesnow.server


@pytest.fixture(scope="session")
def unused_port(unused_tcp_port_factory: Callable[[], int]) -> int:
    # unused_tcp_port_factory is from pytest-asyncio
    return unused_tcp_port_factory()


@pytest.fixture(scope="session")
def server(unused_tcp_port_factory: Callable[[], int]) -> Iterator[int]:
    port = unused_tcp_port_factory()
    server = uvicorn.Server(uvicorn.Config(fakesnow.server.app, port=port, log_level="info"))
    thread = threading.Thread(target=server.run, name="Server", daemon=True)
    thread.start()

    while not server.started:
        sleep(0.1)
    yield port

    server.should_exit = True
    # wait for server thread to end
    thread.join()


def test_server_connect(server: int) -> None:
    with (
        snowflake.connector.connect(
            user="fake", password="snow", account="fakesnow", host="localhost", port=server, protocol="http"
        ) as conn1,
        conn1.cursor() as cur,
    ):
        cur.execute("select 'hello world'")
        assert cur.fetchall() == [("hello world",)]


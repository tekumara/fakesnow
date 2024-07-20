import asyncio
from typing import AsyncIterator, Callable, Iterator

import pytest
import snowflake.connector
import uvicorn


@pytest.fixture(scope="session")
async def unused_port(unused_tcp_port_factory: Callable[[], int]) -> int:
    # use pytest-asyncio.unused_tcp_port_factory
    return unused_tcp_port_factory()

@pytest.fixture(scope="session")
async def server(unused_tcp_port_factory: Callable[[], int]) -> AsyncIterator[int]:
    port = unused_tcp_port_factory()
    s = uvicorn.Server(uvicorn.Config("fakesnow.server:app", port=port, log_level="info"))
    _ = asyncio.create_task(s.serve())  # noqa: RUF006
    while not s.started:
        await asyncio.sleep(0.1)
    yield port
    s.should_exit = True
    await asyncio.sleep(0.1)

def test_server_connect(server: int) -> None:
    with (
        snowflake.connector.connect(
            user="fake", password="snow", account="fakesnow", host="localhost", port=server, protocol="http"
        ) as conn1,
        conn1.cursor() as cur,
    ):
        cur.execute("select 'hello world'")
        assert cur.fetchall() == [("hello world",)]

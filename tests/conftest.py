import os
from collections.abc import Iterator
from typing import cast

import boto3
import pytest
import snowflake.connector
from mypy_boto3_s3 import S3Client
from sqlalchemy.engine import Engine, create_engine

import fakesnow
import fakesnow.fixtures
import tests.fixtures.moto

pytest_plugins = fakesnow.fixtures.__name__, tests.fixtures.moto.__name__


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
def _server_conn_kwargs() -> Iterator[dict]:
    # isolate each session to a separate instance to avoid sharing tables between tests
    with fakesnow.server(
        session_parameters={
            "FAKESNOW_DB_PATH": ":isolated:",
        }
    ) as conn_kwargs:
        yield conn_kwargs


@pytest.fixture
def server(_server_conn_kwargs: dict) -> dict:
    # snowflake connector modifies session_parameters in place
    # so we yield a copy to avoid side effects between tests
    # see https://github.com/snowflakedb/snowflake-connector-python/issues/2716
    copy = _server_conn_kwargs.copy()
    copy["session_parameters"] = copy.get("session_parameters", {}).copy()
    return copy


@pytest.fixture
def sconn(server: dict) -> Iterator[snowflake.connector.SnowflakeConnection]:
    with snowflake.connector.connect(
        **server,
        database="db1",
        schema="schema1",
    ) as c:
        yield c


@pytest.fixture
def scur(
    sconn: snowflake.connector.SnowflakeConnection,
) -> Iterator[snowflake.connector.cursor.SnowflakeCursor]:
    with sconn.cursor() as cur:
        yield cur


@pytest.fixture
def sdcur(
    sconn: snowflake.connector.SnowflakeConnection,
) -> Iterator[snowflake.connector.cursor.DictCursor]:
    with sconn.cursor(snowflake.connector.cursor.DictCursor) as cur:
        yield cast(snowflake.connector.cursor.DictCursor, cur)


@pytest.fixture()
def s3_client(moto_session: boto3.Session, cur: snowflake.connector.cursor.SnowflakeCursor) -> S3Client:
    """Configures duckdb to use the moto session and returns an s3 client."""

    client = moto_session.client("s3")
    endpoint_url = client.meta.endpoint_url

    creds = moto_session.get_credentials()
    assert creds

    # NOTE: This is a duckdb query, not a transformed snowflake one!
    cur.execute(
        f"""
        CREATE SECRET s3_secret (
            TYPE s3,
            ENDPOINT '{endpoint_url.removeprefix("http://")}',
            KEY_ID '{creds.access_key}',
            SECRET '{creds.secret_key}',
            SESSION_TOKEN '{creds.token}',
            URL_STYLE 'path',
            USE_SSL false,
            REGION '{moto_session.region_name}'
        );
        """
    )

    return client


if os.getenv("TEST_SERVER"):
    # use server to run all tests
    conn = sconn

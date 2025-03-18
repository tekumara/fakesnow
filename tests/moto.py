from __future__ import annotations

import time
from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

import boto3
import pytest
import snowflake.connector
from moto.server import ThreadedMotoServer

if TYPE_CHECKING:
    from _pytest.scope import _ScopeName


@dataclass
class Session:
    session: boto3.Session
    endpoint_url: str
    access_key_id: str
    secret_access_key: str
    session_token: str
    region: str

    def client(self, **kwargs: Any):
        return self.session.client("s3", endpoint_url=self.endpoint_url, **kwargs)

    def resource(self, **kwargs: Any):
        return self.session.resource("s3", endpoint_url=self.endpoint_url, **kwargs)


def create_moto_fixture(region: str = "us-east-1"):
    """Create a moto fixture which relies on a test-wide inline moto server.

    Note each test generates a unique internal moto account in order to ensure
    isolation of data/state between tests.
    """

    @pytest.fixture()
    def _fixture(moto_server: str, conn: snowflake.connector.SnowflakeConnection) -> Generator[Session, None, None]:
        # Attempt at a cross-process way of generating unique 12-character integers.
        account_id = str(time.time_ns())[:12]

        sts = boto3.client(
            "sts",
            endpoint_url=moto_server,
            aws_access_key_id="test",
            aws_secret_access_key="test",
            region_name=region,
        )
        response = sts.assume_role(
            RoleArn=f"arn:aws:iam::{account_id}:role/my-role",
            RoleSessionName="test-session-name",
            ExternalId="test-external-id",
        )

        credentials = response["Credentials"]

        access_key_id = credentials["AccessKeyId"]
        secret_access_key = credentials["SecretAccessKey"]
        session_token = credentials["SessionToken"]
        with conn.cursor() as cursor:
            # NOTE: This is a duckdb query, not a transformed snowflake one!
            cursor.execute(
                f"""
                CREATE SECRET s3_secret (
                    TYPE s3,
                    ENDPOINT '{moto_server.removeprefix("http://")}',
                    KEY_ID '{access_key_id}',
                    SECRET '{secret_access_key}',
                    SESSION_TOKEN '{session_token}',
                    URL_STYLE 'path',
                    USE_SSL false,
                    REGION '{region}'
                );
                """
            )

        yield Session(
            boto3.Session(
                aws_access_key_id=access_key_id,
                aws_secret_access_key=secret_access_key,
                aws_session_token=session_token,
                region_name=region,
            ),
            endpoint_url=moto_server,
            access_key_id=access_key_id,
            secret_access_key=secret_access_key,
            session_token=session_token,
            region=region,
        )

    return _fixture


def create_moto_server(scope: _ScopeName = "session"):
    @pytest.fixture(scope=scope)
    def moto_server():
        server = ThreadedMotoServer(port=0)
        server.start()
        host, port = server.get_host_and_port()

        yield f"http://{host}:{port}"

        server.stop()

    return moto_server

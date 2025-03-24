import os
import time

import boto3
import pytest
from moto.server import ThreadedMotoServer


@pytest.fixture(scope="session")
def moto_server():
    server = ThreadedMotoServer(port=0)
    server.start()
    host, port = server.get_host_and_port()

    endpoint_url = f"http://{host}:{port}"

    # will be used implicitly by boto3 clients and resources
    os.environ["AWS_ENDPOINT_URL"] = endpoint_url
    yield endpoint_url

    server.stop()


@pytest.fixture()
def moto_session(moto_server: str) -> boto3.Session:
    """Create a boto session using the moto server.

    Note each session uses a unique mock moto AWS account in order to ensure
    isolation of data/state between tests.

    See https://docs.getmoto.org/en/latest/docs/multi_account.html
    """
    region = "us-east-1"

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

    return boto3.Session(
        aws_access_key_id=access_key_id,
        aws_secret_access_key=secret_access_key,
        aws_session_token=session_token,
        region_name=region,
    )

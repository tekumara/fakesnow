import datetime
import os
import subprocess
from collections.abc import Iterator
from pathlib import Path

import pytest
import snowflake.connector
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

REPO_ROOT = Path(__file__).parent.parent
PORT = 64616
IMAGE_TAG = "fakesnow-test:latest"


@pytest.fixture(scope="session")
def docker_image() -> Iterator[str]:
    # testcontainers-python uses docker-py, which does not expose BuildKit; use the Docker CLI instead.
    subprocess.run(
        ["docker", "build", "--tag", IMAGE_TAG, str(REPO_ROOT)],
        check=True,
        env={**os.environ, "DOCKER_BUILDKIT": "1"},
    )
    try:
        yield IMAGE_TAG
    finally:
        subprocess.run(["docker", "image", "rm", "--force", IMAGE_TAG], check=False)


@pytest.fixture(scope="session")
def docker_fakesnow(docker_image: str) -> Iterator[DockerContainer]:
    with (
        DockerContainer(docker_image)
        .with_exposed_ports(PORT)
        .waiting_for(LogMessageWaitStrategy("Application startup complete").with_startup_timeout(30)) as container
    ):
        yield container


def docker_connect(docker_fakesnow: DockerContainer) -> snowflake.connector.SnowflakeConnection:
    host = docker_fakesnow.get_container_host_ip()
    port = int(docker_fakesnow.get_exposed_port(PORT))

    return snowflake.connector.connect(
        user="fake",
        password="snow",
        account="fakesnow",
        host=host,
        port=port,
        protocol="http",
        session_parameters={"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False},
        network_timeout=5,
    )


def test_docker_select(docker_fakesnow: DockerContainer) -> None:
    with docker_connect(docker_fakesnow) as conn:
        cursor = conn.cursor().execute("SELECT 'Hello fake world!'")
        assert cursor is not None
        result = cursor.fetchone()
        assert result == ("Hello fake world!",)


def test_docker_put_wildcard_from_client_filesystem(docker_fakesnow: DockerContainer, tmp_path: Path) -> None:
    (tmp_path / "one.csv").write_text("1,2\n")
    (tmp_path / "two.csv").write_text("3,4\n")
    with docker_connect(docker_fakesnow) as conn, conn.cursor() as cur:
        cur.execute("CREATE DATABASE IF NOT EXISTS db1")
        cur.execute("USE DATABASE db1")
        cur.execute("CREATE SCHEMA IF NOT EXISTS schema1")
        cur.execute("USE SCHEMA schema1")
        cur.execute("CREATE STAGE files")
        cur.execute(f"PUT 'file://{tmp_path}/*.csv' @files")
        assert sorted(row[1] for row in cur.fetchall()) == ["one.csv.gz", "two.csv.gz"]

        cur.execute("LIST @files")
        assert sorted(row[0] for row in cur.fetchall()) == ["files/one.csv.gz", "files/two.csv.gz"]


def test_docker_timestamp_tz(docker_fakesnow: DockerContainer) -> None:
    with docker_connect(docker_fakesnow) as conn:
        cursor = conn.cursor().execute("SELECT to_timestamp_tz('2013-04-05 01:02:03.123456')")
        assert cursor is not None
        result = cursor.fetchone()
        assert result and isinstance(result[0], datetime.datetime)
        assert result[0].tzinfo is not None

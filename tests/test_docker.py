from collections.abc import Iterator
from pathlib import Path

import pytest
import snowflake.connector
from testcontainers.core.container import DockerContainer
from testcontainers.core.image import DockerImage
from testcontainers.core.wait_strategies import LogMessageWaitStrategy

REPO_ROOT = Path(__file__).parent.parent
PORT = 8080


@pytest.fixture(scope="session")
def docker_fakesnow() -> Iterator[DockerContainer]:
    with DockerImage(path=str(REPO_ROOT), tag="fakesnow-test:latest") as image:
        with (
            DockerContainer(str(image))
            .with_exposed_ports(PORT)
            .waiting_for(LogMessageWaitStrategy("Application startup complete").with_startup_timeout(30))
        ) as container:
            yield container


def test_docker_select(docker_fakesnow: DockerContainer) -> None:
    host = docker_fakesnow.get_container_host_ip()
    port = int(docker_fakesnow.get_exposed_port(PORT))

    with snowflake.connector.connect(
        user="fake",
        password="snow",
        account="fakesnow",
        host=host,
        port=port,
        protocol="http",
        session_parameters={"CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED": False},
        network_timeout=5,
    ) as conn:
        result = conn.cursor().execute("SELECT 'Hello fake world!'").fetchone()
        assert result == ("Hello fake world!",)

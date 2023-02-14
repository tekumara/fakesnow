from typing import Iterator

import pytest

import fakesnow


@pytest.fixture
def _fake_snow() -> Iterator[None]:
    with fakesnow.mock():
        yield


@pytest.fixture(scope="session")
def _fake_snow_session() -> Iterator[None]:
    with fakesnow.mock():
        yield

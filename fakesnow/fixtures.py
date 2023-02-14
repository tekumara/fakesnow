from typing import Iterator

import pytest

import fakesnow


@pytest.fixture
def _fake_snow() -> Iterator[fakesnow.FakeFunctions]:
    with fakesnow.mock() as fake_fns:
        yield fake_fns


@pytest.fixture(scope="session")
def _fake_snow_session() -> Iterator[fakesnow.FakeFunctions]:
    with fakesnow.mock() as fake_fns:
        yield fake_fns

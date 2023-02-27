from typing import Iterator

import pytest

import fakesnow


@pytest.fixture
def _fake_snow() -> Iterator[None]:
    with fakesnow.patch() as fake_fns:
        yield fake_fns


@pytest.fixture
def _fake_snow_no_auto_create() -> Iterator[None]:
    with fakesnow.patch(create_database_on_connect=False, create_schema_on_connect=False) as fake_fns:
        yield fake_fns


@pytest.fixture(scope="session")
def _fake_snow_session() -> Iterator[None]:
    with fakesnow.patch() as fake_fns:
        yield fake_fns

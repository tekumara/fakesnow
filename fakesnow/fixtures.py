from collections.abc import Iterator

import pytest

import fakesnow


@pytest.fixture
def _fakesnow() -> Iterator[None]:
    with fakesnow.patch():
        yield


@pytest.fixture
def _fakesnow_no_auto_create() -> Iterator[None]:
    with fakesnow.patch(create_database_on_connect=False, create_schema_on_connect=False):
        yield


@pytest.fixture(scope="session")
def _fakesnow_session() -> Iterator[None]:
    with fakesnow.patch():
        yield

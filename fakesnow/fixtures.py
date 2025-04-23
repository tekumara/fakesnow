from collections.abc import Iterator
from typing import Any

import pytest

import fakesnow


@pytest.fixture
def _fakesnow() -> Iterator[None]:
    with fakesnow.patch():
        yield


@pytest.fixture(scope="session")
def fakesnow_server() -> Iterator[dict[str, Any]]:
    with fakesnow.server() as conn_kwargs:
        yield conn_kwargs


@pytest.fixture
def _fakesnow_no_auto_create() -> Iterator[None]:
    with fakesnow.patch(create_database_on_connect=False, create_schema_on_connect=False):
        yield


@pytest.fixture(scope="session")
def _fakesnow_session() -> Iterator[None]:
    with fakesnow.patch():
        yield

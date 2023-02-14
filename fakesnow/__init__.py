from __future__ import annotations

import builtins
import contextlib
import sys
import types
import unittest.mock as mock
from contextlib import contextmanager
from dataclasses import dataclass
from typing import (
    Callable,
    Iterator,
    List,
    Mapping,
    Sequence,
)

import duckdb
import snowflake.connector
import snowflake.connector.pandas_tools
from duckdb import DuckDBPyConnection

import fakesnow.fakes as fakes


@contextmanager
def patch(extra_targets: str | Sequence[str] = []) -> Iterator[None]:
    """Patch snowflake targets with fakes.

    The standard targets are:
      - snowflake.connector.connect
      - snowflake.connector.pandas_tools.write_pandas

    Args:
        extra_targets (Sequence[types.ModuleType], optional): Extra targets to patch. Defaults to [].

        Allows extra targets beyond the standard snowflake.connector targets to be patched. Needed because we cannot
        patch definitions, only usages, see https://docs.python.org/3/library/unittest.mock.html#where-to-patch

    Yields:
        Iterator[None]: None.
    """

    # don't allow re-patching because the keys in the fake_fns dict will point to the fakes, and so we
    # won't be able to patch extra targets
    assert not isinstance(snowflake.connector.connect, mock.MagicMock), "Snowflake connector is already patched"

    duck_conn = duckdb.connect(database=":memory:")

    fake_fns = {
        # every time we connect, create a new cursor (ie: connection) so we can isolate each connection's
        # schema setting, see https://duckdb.org/docs/api/python/overview.html#startup--shutdown
        snowflake.connector.connect: lambda **kwargs: fakes.FakeSnowflakeConnection(duck_conn.cursor(), **kwargs),
        snowflake.connector.pandas_tools.write_pandas: fakes.write_pandas,
    }

    std_targets = ["snowflake.connector.connect", "snowflake.connector.pandas_tools.write_pandas"]

    stack = contextlib.ExitStack()

    for im in std_targets + list([extra_targets] if isinstance(extra_targets, str) else extra_targets):
        module_name = ".".join(im.split(".")[:-1])
        fn_name = im.split(".")[-1]
        module = sys.modules.get(module_name)
        assert module, f"No module {module}"
        fn = module.__dict__.get(fn_name)
        assert fn, f"No module var {im}"
        fake = fake_fns.get(fn)
        assert fake, f"Module var {im} is not one of {fake_fns.keys()}"

        p = mock.patch(im, side_effect=fake)
        stack.enter_context(p)

    yield None

    stack.close()

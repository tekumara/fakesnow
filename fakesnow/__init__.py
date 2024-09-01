from __future__ import annotations

import contextlib
import importlib
import os
import sys
import unittest.mock as mock
from collections.abc import Iterator, Sequence
from contextlib import contextmanager

import snowflake.connector
import snowflake.connector.pandas_tools

import fakesnow.fakes as fakes
from fakesnow.instance import FakeSnow


@contextmanager
def patch(
    extra_targets: str | Sequence[str] = [],
    create_database_on_connect: bool = True,
    create_schema_on_connect: bool = True,
    db_path: str | os.PathLike | None = None,
    nop_regexes: list[str] | None = None,
) -> Iterator[None]:
    """Patch snowflake targets with fakes.

    The standard targets are:
      - snowflake.connector.connect
      - snowflake.connector.pandas_tools.write_pandas

    Args:
        extra_targets (str | Sequence[str], optional): Extra targets to patch. Defaults to [].

        Allows extra targets beyond the standard snowflake.connector targets to be patched. Needed because we cannot
        patch definitions, only usages, see https://docs.python.org/3/library/unittest.mock.html#where-to-patch

        create_database_on_connect (bool, optional): Create database if provided in connection. Defaults to True.
        create_schema_on_connect (bool, optional): Create schema if provided in connection. Defaults to True.
        db_path (str | os.PathLike | None, optional): Use existing database files from this path
            or create them here if they don't already exist. If None databases are in-memory. Defaults to None.
        nop_regexes (list[str] | None, optional): SQL statements matching these regexes (case-insensitive) will return
            the success response without being run. Useful to skip over SQL commands that aren't implemented yet.
            Defaults to None.

    Yields:
        Iterator[None]: None.
    """

    # don't allow re-patching because the keys in the fake_fns dict will point to the fakes, and so we
    # won't be able to patch extra targets
    assert not isinstance(snowflake.connector.connect, mock.MagicMock), "Snowflake connector is already patched"

    fs = FakeSnow(
        create_database_on_connect=create_database_on_connect,
        create_schema_on_connect=create_schema_on_connect,
        db_path=db_path,
        nop_regexes=nop_regexes,
    )

    fake_fns = {
        snowflake.connector.connect: fs.connect,
        snowflake.connector.pandas_tools.write_pandas: fakes.write_pandas,
    }

    std_targets = ["snowflake.connector.connect", "snowflake.connector.pandas_tools.write_pandas"]

    stack = contextlib.ExitStack()

    for im in std_targets + list([extra_targets] if isinstance(extra_targets, str) else extra_targets):
        module_name = ".".join(im.split(".")[:-1])
        fn_name = im.split(".")[-1]
        # get module or try to import it if not loaded yet
        module = sys.modules.get(module_name) or importlib.import_module(module_name)
        fn = module.__dict__.get(fn_name)
        assert fn, f"No module var {im}"

        # if we imported the module above, it'll already be mocked because
        # it'll reference the standard targets which are mocked first
        if isinstance(fn, mock.MagicMock):
            continue

        fake = fake_fns.get(fn)
        assert fake, f"Module var {im} is {fn} and not one of {fake_fns.keys()}"

        p = mock.patch(im, side_effect=fake)
        stack.enter_context(p)

    try:
        yield None
    finally:
        stack.close()
        fs.duck_conn.close()

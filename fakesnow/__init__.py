from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import create_autospec, patch

import snowflake.connector


def mock_execute(
    command: str,
    **kwargs: dict[str, Any],  # noqa: ARG001 catch remaining args
) -> snowflake.connector.cursor.SnowflakeCursor | None:
    print(command)


@contextmanager
def mock() -> Iterator[None]:
    with patch.object(snowflake.connector, "connect", autospec=True) as connect:
        mock_cursor = create_autospec(snowflake.connector.cursor.SnowflakeCursor)
        mock_cursor.execute.side_effect = mock_execute


        mock_connection = create_autospec(snowflake.connector.SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        connect.return_value = mock_connection

        # allow mocks to be used as context managers
        mock_connection.__enter__.return_value = mock_connection
        mock_cursor.__enter__.return_value = mock_cursor

        yield

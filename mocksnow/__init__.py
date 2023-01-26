from __future__ import annotations

from contextlib import contextmanager
from typing import Any, Iterator
from unittest.mock import Mock, create_autospec, patch

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
        mock_connection = create_autospec(snowflake.connector.SnowflakeConnection)
        mock_connection.cursor.return_value = mock_cursor
        mock_cursor.execute.side_effect = mock_execute
        connect.return_value = mock_connection

        # when used as a context manager return itself
        mock_connection.__enter__.return_value = mock_connection
        mock_cursor.__enter__.return_value = mock_cursor

        yield

from unittest.mock import MagicMock

import snowflake.connector
import snowflake.connector.pandas_tools
from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import builtins

def test_mock_snowflake_connector_connect(_fake_snow: None) -> None:
    assert isinstance(snowflake.connector.connect, MagicMock), "snowflake.connector.connect is not mocked"


def test_mock_snowflake_connector_pandas_tools_write_pandas(_fake_snow: None) -> None:
    assert isinstance(
        snowflake.connector.pandas_tools.write_pandas, MagicMock
    ), "snowflake.connector.pandas_tools.write_pandas is not mocked"


def test_mock_connect(_fake_snow: None) -> None:
    assert isinstance(connect, MagicMock), "connect is not mocked"


def test_mock_write_pandas(_fake_snow: None) -> None:
    assert isinstance(write_pandas, MagicMock), "write_pandas is not mocked"

# ruff: noqa: SLF001
import datetime

import snowflake.connector.converter

from fakesnow.converter import from_binary, from_boolean, from_date, from_datetime, from_time

converter = snowflake.connector.converter.SnowflakeConverter()
date_converter = converter._DATE_to_python({})


def test_from_binary() -> None:
    value = b"Jenny"
    assert from_binary(converter._bytes_to_snowflake_bindings(..., value)) == value


def test_from_boolean() -> None:
    value = True
    assert from_boolean(converter._bool_to_snowflake_bindings(..., value)) == value


def test_from_date() -> None:
    value = datetime.date(2023, 1, 2)
    assert from_date(converter._date_to_snowflake_bindings(..., value)) == value


def test_from_time() -> None:
    value = datetime.time(12, 30, 45, 123456)
    assert from_time(converter._time_to_snowflake_bindings(..., value)) == value


def test_from_datetime() -> None:
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)
    assert from_datetime(converter._datetime_to_snowflake_bindings("TIMESTAMP_NTZ", value)) == value

# ruff: noqa: SLF001
import datetime

import snowflake.connector.converter

from fakesnow.converter import (
    from_binary,
    from_binding,
    from_boolean,
    from_date,
    from_datetime,
    from_datetime_tz,
    from_time,
)

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


def test_from_binding_timestamps() -> None:
    # the JDBC driver binds setTimestamp as TIMESTAMP_LTZ, the python connector as TIMESTAMP_NTZ
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.timezone.utc)
    for type_ in ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ"]:
        binding = {"type": type_, "value": converter._datetime_to_snowflake_bindings(type_, value)}
        assert from_binding(binding) == value, type_


def test_from_datetime_tz() -> None:
    # TIMESTAMP_TZ bindings carry the offset, as "<epoch nanoseconds> <offset minutes + 1440>"
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
    converted = from_datetime_tz(converter._datetime_to_snowflake_bindings("TIMESTAMP_TZ", value))
    assert converted == value
    assert converted.utcoffset() == datetime.timedelta(hours=9)

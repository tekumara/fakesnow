# ruff: noqa: SLF001
import datetime
import importlib
from typing import Any

from fakesnow.converter import (
    from_binary,
    from_binding,
    from_boolean,
    from_date,
    from_datetime,
    from_datetime_tz,
    from_time,
)

try:  # connector 5.x
    _converter = importlib.import_module("snowflake.connector._internal.binding_converters").JsonBindingConverter

    def binding(value: Any) -> str:  # noqa: ANN401
        """The value the connector binds for ``value``."""
        return _converter._convert_value(value)[1]

except ModuleNotFoundError:  # connector 4.x
    _converter = importlib.import_module("snowflake.connector.converter").SnowflakeConverter()

    def binding(value: Any) -> str:  # noqa: ANN401
        """The value the connector binds for ``value``."""
        to_binding = getattr(_converter, f"_{type(value).__name__.lower()}_to_snowflake_bindings")
        return to_binding("TIMESTAMP_NTZ", value)


def test_from_binary() -> None:
    value = b"Jenny"
    assert from_binary(binding(value)) == value


def test_from_boolean() -> None:
    value = True
    assert from_boolean(binding(value)) == value


def test_from_date() -> None:
    value = datetime.date(2023, 1, 2)
    assert from_date(binding(value)) == value


def test_from_time() -> None:
    value = datetime.time(12, 30, 45, 123456)
    assert from_time(binding(value)) == value


def test_from_datetime() -> None:
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    assert from_datetime(binding(value)) == value


def test_from_binding_timestamps() -> None:
    # the JDBC driver binds setTimestamp as TIMESTAMP_LTZ, the python connector as TIMESTAMP_NTZ
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    for type_ in ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ"]:
        assert from_binding({"type": type_, "value": binding(value)}) == value, type_


def test_from_binding_timestamp_tz() -> None:
    # TIMESTAMP_TZ bindings carry the offset, as "<epoch nanoseconds> <offset minutes + 1440>"
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    assert from_binding({"type": "TIMESTAMP_TZ", "value": "1672662645123456000 1440"}) == value


def test_from_datetime_tz() -> None:
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
    converted = from_datetime_tz("1672630245123456000 1980")
    assert converted == value
    assert converted.utcoffset() == datetime.timedelta(hours=9)

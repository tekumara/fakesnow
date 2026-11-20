import datetime

import pytest

from fakesnow.converter import from_binding


def test_from_binding_binary() -> None:
    assert from_binding({"type": "BINARY", "value": "4a656e6e79"}) == b"Jenny"


def test_from_binding_boolean() -> None:
    assert from_binding({"type": "BOOLEAN", "value": "true"}) is True


def test_from_binding_date() -> None:
    assert from_binding({"type": "DATE", "value": "1672617600000"}) == datetime.date(2023, 1, 2)


def test_from_binding_time() -> None:
    assert from_binding({"type": "TIME", "value": "45045123456000"}) == datetime.time(12, 30, 45, 123456)


@pytest.mark.parametrize("type_", ["TIMESTAMP_NTZ", "TIMESTAMP_LTZ"])
def test_from_binding_timestamp(type_: str) -> None:
    # JDBC binds setTimestamp as TIMESTAMP_LTZ, and the Python connector as TIMESTAMP_NTZ.
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    assert from_binding({"type": type_, "value": "1672662645123456000"}) == value


def test_from_binding_timestamp_tz() -> None:
    # TIMESTAMP_TZ bindings carry the offset, as "<epoch nanoseconds> <offset minutes + 1440>".
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.UTC)
    assert from_binding({"type": "TIMESTAMP_TZ", "value": "1672662645123456000 1440"}) == value


def test_from_binding_timestamp_tz_offset() -> None:
    value = datetime.datetime(2023, 1, 2, 12, 30, 45, 123456, tzinfo=datetime.timezone(datetime.timedelta(hours=9)))
    converted = from_binding({"type": "TIMESTAMP_TZ", "value": "1672630245123456000 1980"})
    assert isinstance(converted, datetime.datetime)
    assert converted == value
    assert converted.utcoffset() == datetime.timedelta(hours=9)

from __future__ import annotations

import binascii
import datetime
from datetime import date, time, timezone

# convert bindings provided as strings to the server into python types


def from_binding(binding: dict[str, str]) -> int | bytes | bool | date | time | datetime.datetime | str:
    typ = binding["type"]
    value = binding["value"]
    if typ == "FIXED":
        return int(value)
    elif typ == "BINARY":
        return from_binary(value)
    # TODO: not strictly needed
    elif typ == "BOOLEAN":
        return value.lower() == "true"
    elif typ == "DATE":
        return from_date(value)
    elif typ == "TIME":
        return from_time(value)
    elif typ == "TIMESTAMP_NTZ":
        return from_datetime(value)
    else:
        # For other types, return str
        return value


def from_binary(s: str) -> bytes:
    return binascii.unhexlify(s)


def from_boolean(s: str) -> bool:
    return s.lower() == "true"


def from_date(s: str) -> date:
    milliseconds = int(s)
    seconds = milliseconds / 1000
    return datetime.datetime.fromtimestamp(seconds, timezone.utc).date()


def from_time(s: str) -> time:
    nanoseconds = int(s)
    microseconds = nanoseconds / 1000
    return (
        datetime.datetime.fromtimestamp(microseconds / 1_000_000, timezone.utc)
        .replace(microsecond=int(microseconds % 1_000_000))
        .time()
    )


def from_datetime(s: str) -> datetime.datetime:
    nanoseconds = int(s)
    microseconds = nanoseconds / 1000
    return datetime.datetime.fromtimestamp(microseconds / 1_000_000, timezone.utc).replace(
        microsecond=int(microseconds % 1_000_000)
    )

# pyright: reportOptionalMemberAccess=false

import datetime

import snowflake.connector

# see https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
# snowflake returns naive timestamps (ie: no timezone)


def test_int_seconds_to_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    # to_timestamp, to_timestamp_ntz, ::timestamp and ::timestamp_ntz are all the same
    assert cur.execute("select 1748179630::timestamp").fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]
    assert cur.execute("select 1748179630::timestamp_ntz").fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]
    assert cur.execute("select to_timestamp(1748179630)").fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]
    assert cur.execute("select to_timestamp_ntz(1748179630)").fetchall() == [
        (datetime.datetime(2025, 5, 25, 13, 27, 10),)
    ]

    cur.execute("create table example1 (ts int)")
    cur.execute("insert into example1 values (1748179630)")
    assert cur.execute(
        "select ts::timestamp as 1, ts::timestamp_ntz as 2,"
        "to_timestamp(ts) as 3, to_timestamp_ntz(ts) as 4 from example1"
    ).fetchall() == [
        (
            datetime.datetime(2025, 5, 25, 13, 27, 10),
            datetime.datetime(2025, 5, 25, 13, 27, 10),
            datetime.datetime(2025, 5, 25, 13, 27, 10),
            datetime.datetime(2025, 5, 25, 13, 27, 10),
        )
    ]


def test_milliseconds_to_timestamp_with_scale(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute("select to_timestamp(1748179630122, 3)").fetchall() == [
        (datetime.datetime(2025, 5, 25, 13, 27, 10, microsecond=122000),)
    ]


def test_microseconds_to_timestamp_with_scale(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute("select to_timestamp_ntz(1748179630212333, 6)").fetchall() == [
        (datetime.datetime(2025, 5, 25, 13, 27, 10, microsecond=212333),)
    ]


def test_string_seconds_to_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    assert cur.execute("select '1748179630'::timestamp").fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]


def test_json_field_seconds_to_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    query = "select parse_json('{\"ts\": 1748179630}'):ts::timestamp_ntz"
    assert cur.execute(query).fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]
    query = "select to_timestamp_ntz(parse_json('{\"ts\": 1748179630}'):ts)"
    assert cur.execute(query).fetchall() == [(datetime.datetime(2025, 5, 25, 13, 27, 10),)]


def test_string_datetime_to_timestamp(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT to_timestamp('2013-04-05 01:02:03')")
    assert cur.fetchall() == [(datetime.datetime(2013, 4, 5, 1, 2, 3),)]

    cur.execute("SELECT to_timestamp_ntz('2013-04-05 01:02:03')")
    assert cur.fetchall() == [(datetime.datetime(2013, 4, 5, 1, 2, 3),)]

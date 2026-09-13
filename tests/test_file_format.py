import pytest
import snowflake.connector.cursor


def test_create_file_format(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

    assert str(excinfo.value) == "002002 (42710): SQL compilation error:\nObject 'MY_FMT' already exists."


def test_create_file_format_or_replace(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

    dcur.execute("CREATE OR REPLACE FILE FORMAT my_fmt TYPE='CSV' SKIP_HEADER=1")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]


def test_create_file_format_if_not_exists(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT IF NOT EXISTS my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]

    dcur.execute("CREATE FILE FORMAT IF NOT EXISTS my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "MY_FMT already exists, statement succeeded."}]


def test_create_file_format_fully_qualified(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE DATABASE db2")
    dcur.execute("CREATE SCHEMA db2.schema2")

    dcur.execute("CREATE FILE FORMAT db2.schema2.my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]

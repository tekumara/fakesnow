# ruff: noqa: E501
# ruff: noqa: RUF012
from __future__ import annotations

import uuid

import pytest
import snowflake.connector.cursor
import sqlglot
from mypy_boto3_s3 import S3Client

from fakesnow.transforms.copy_into import copy_into


def create_table(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE SCHEMA IF NOT EXISTS schema1")
    dcur.execute("CREATE TABLE schema1.table1 (a INT, b INT)")


def upload_file(s3_client: S3Client, data: str, key: str = "foo.txt") -> str:
    bucket = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket


class TestCSV:
    sql = """
    COPY INTO schema1.table1
    FROM 's3://{bucket}/'
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV')
    FORCE = TRUE;
    """
    expect_sql = "INSERT INTO schema1.table1 SELECT * FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)"
    csv_data = "1,2\n"
    expected_data = [{"A": 1, "B": 2}]

    def test_transform(self) -> None:
        sql = self.sql.format(bucket="bucket")
        assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
            dialect="duckdb"
        ) == self.expect_sql.format(bucket="bucket")

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
        create_table(dcur)
        bucket = upload_file(s3_client, self.csv_data)
        dcur.execute(self.sql.format(bucket=bucket))
        assert dcur.fetchall() == [
            {
                "file": f"s3://{bucket}/foo.txt",
                "status": "LOADED",
                "rows_parsed": 1,
                "rows_loaded": 1,
                "error_limit": 1,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        dcur.execute("SELECT * FROM schema1.table1")
        assert dcur.fetchall() == self.expected_data


class TestCSVPipeDelimiter:
    sql = """
    COPY INTO schema1.table1 (a, b)
    FROM 's3://{bucket}/'
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
    FORCE = TRUE;
    """
    expect_sql = "INSERT INTO schema1.table1 (a, b) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE, sep = '|')"
    csv_data = "1|2\n"
    expected_data = [{"A": 1, "B": 2}]

    def test_transform(self) -> None:
        sql = self.sql.format(bucket="bucket")
        assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
            dialect="duckdb"
        ) == self.expect_sql.format(bucket="bucket")

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
        create_table(dcur)
        bucket = upload_file(s3_client, self.csv_data)
        dcur.execute(self.sql.format(bucket=bucket))
        assert dcur.fetchall() == [
            {
                "file": f"s3://{bucket}/foo.txt",
                "status": "LOADED",
                "rows_parsed": 1,
                "rows_loaded": 1,
                "error_limit": 1,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        dcur.execute("SELECT * FROM schema1.table1")
        assert dcur.fetchall() == self.expected_data


class TestCSVSkipHeader:
    sql = """
    COPY INTO schema1.table1 (a, b)
    FROM 's3://{bucket}/'
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    FORCE = TRUE;
    """
    expect_sql = "INSERT INTO schema1.table1 (a, b) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE, skip = 1)"
    csv_data = "a,b\n1,2\n"
    expected_data = [{"A": 1, "B": 2}]

    def test_transform(self) -> None:
        sql = self.sql.format(bucket="bucket")
        assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
            dialect="duckdb"
        ) == self.expect_sql.format(bucket="bucket")

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
        create_table(dcur)
        bucket = upload_file(s3_client, self.csv_data)
        dcur.execute(self.sql.format(bucket=bucket))
        assert dcur.fetchall() == [
            {
                "file": f"s3://{bucket}/foo.txt",
                "status": "LOADED",
                "rows_parsed": 1,  # Snowflake doesn't count header row if SKIP_HEADER is used
                "rows_loaded": 1,
                "error_limit": 1,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        dcur.execute("SELECT * FROM schema1.table1")
        assert dcur.fetchall() == self.expected_data


class TestCSVSelectColumn:
    sql = """
    COPY INTO schema1.table1 (b)
    FROM 's3://{bucket}/'
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV')
    FORCE = TRUE;
    """
    expect_sql = "INSERT INTO schema1.table1 (b) SELECT column0 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)"
    csv_data = "1,2\n"
    expected_data = [{"A": None, "B": 1}]

    def test_transform(self) -> None:
        sql = self.sql.format(bucket="bucket")
        assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
            dialect="duckdb"
        ) == self.expect_sql.format(bucket="bucket")

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
        create_table(dcur)
        bucket = upload_file(s3_client, self.csv_data)
        dcur.execute(self.sql.format(bucket=bucket))
        assert dcur.fetchall() == [
            {
                "file": f"s3://{bucket}/foo.txt",
                "status": "LOADED",
                "rows_parsed": 1,
                "rows_loaded": 1,
                "error_limit": 1,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        dcur.execute("SELECT * FROM schema1.table1")
        assert dcur.fetchall() == self.expected_data


class TestCSVNoFileFormat:
    sql = """
    COPY INTO schema1.table1
    FROM 's3://{bucket}/'
    FILES=('foo.txt')
    FORCE = TRUE;
    """
    expect_sql = "INSERT INTO schema1.table1 SELECT * FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)"
    csv_data = "1,2\n"
    expected_data = [{"A": 1, "B": 2}]

    def test_transform(self) -> None:
        sql = self.sql.format(bucket="bucket")
        assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
            dialect="duckdb"
        ) == self.expect_sql.format(bucket="bucket")

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
        create_table(dcur)
        bucket = upload_file(s3_client, self.csv_data)
        dcur.execute(self.sql.format(bucket=bucket))
        assert dcur.fetchall() == [
            {
                "file": f"s3://{bucket}/foo.txt",
                "status": "LOADED",
                "rows_parsed": 1,
                "rows_loaded": 1,
                "error_limit": 1,
                "errors_seen": 0,
                "first_error": None,
                "first_error_line": None,
                "first_error_character": None,
                "first_error_column_name": None,
            }
        ]
        dcur.execute("SELECT * FROM schema1.table1")
        assert dcur.fetchall() == self.expected_data


def test_errors(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO schema1.table1 FROM 'invalid_source' FILES=('foobar.csv') FORCE=true")

    assert str(excinfo.value) == "001011 (42601): SQL compilation error:\ninvalid URL prefix found in: 'invalid_source'"

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO schema1.table1 FROM 's3://invalid_source' FILES=('foobar.csv') FORCE=true")

    assert "091016 (22000)" in str(excinfo.value)
    assert "invalid_source" in str(excinfo.value)
    assert "foobar.csv" in str(excinfo.value)

    # file has header but the skip header file format option is not used
    sql = """
    COPY INTO schema1.table1
    FROM 's3://{bucket}/'
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV')
    FORCE = TRUE;
    """
    csv_data = "a,b\n1,2\n"

    bucket = upload_file(s3_client, csv_data)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute(sql.format(bucket=bucket))

    assert "100038 (22018)" in str(excinfo.value)

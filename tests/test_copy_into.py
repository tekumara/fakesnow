# ruff: noqa: E501
from __future__ import annotations

import uuid
from typing import NamedTuple

import pytest
import snowflake.connector.cursor
import sqlglot
from mypy_boto3_s3 import S3Client
from sqlglot import exp

from fakesnow.transforms.copy_into import _params, copy_into


class Case(NamedTuple):
    sql: str
    expect_sql: str
    csv_data: str
    expected_data: list[dict[str, int | None]]


cases = [
    pytest.param(
        Case(
            sql="""
            COPY INTO table1
            FROM 's3://{bucket}/'
            FILES=('foo.txt')
            FILE_FORMAT = (TYPE = 'CSV')
            FORCE = TRUE;
            """,
            expect_sql="INSERT INTO table1 SELECT * FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)",
            csv_data="1,2\n",
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="csv",
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO schema1.table1 (b)
            FROM 's3://{bucket}/'
            STORAGE_INTEGRATION = some_name
            FILES=('foo.txt')
            FILE_FORMAT = (TYPE = 'CSV')
            FORCE = TRUE;
            """,
            expect_sql="INSERT INTO schema1.table1 (b) SELECT column0 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)",
            csv_data="1,2\n",
            expected_data=[{"A": None, "B": 1}],
        ),
        id="select column",
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO table1 (a, b)
            FROM 's3://{bucket}/'
            STORAGE_INTEGRATION = some_name
            FILES=('foo.txt')
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
            FORCE = TRUE;
            """,
            expect_sql="INSERT INTO table1 (a, b) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE, skip = 1)",
            csv_data="a,b\n1,2\n",
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="skip header",
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO table1
            FROM 's3://{bucket}/'
            FILES=('foo.txt')
            FORCE = TRUE;
            """,
            expect_sql="INSERT INTO table1 SELECT * FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE)",
            csv_data="1,2\n",
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="no file format",  # will default to csv
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO table1 (a, b)
            FROM 's3://{bucket}/'
            STORAGE_INTEGRATION = some_name
            FILES=('foo.txt')
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
            FORCE = TRUE;
            """,
            expect_sql="INSERT INTO table1 (a, b) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.txt', header = FALSE, sep = '|')",
            csv_data="1|2\n",
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="pipe delimiter",
    ),
]


@pytest.mark.parametrize("case", cases)
def test_transform(case: Case) -> None:
    sql = case.sql.format(bucket="bucket")
    assert sqlglot.parse_one(sql, read="snowflake").transform(copy_into).sql(
        dialect="duckdb"
    ) == case.expect_sql.format(bucket="bucket")


@pytest.mark.parametrize("case", cases)
def test_execute(case: Case, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)
    bucket = upload_file(s3_client, case.csv_data)
    dcur.execute(case.sql.format(bucket=bucket))

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
    assert dcur.fetchall() == case.expected_data


def test_errors(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO table1 FROM 'invalid_source' FILES=('foobar.csv') FORCE=true")

    assert str(excinfo.value) == "001011 (42601): SQL compilation error:\ninvalid URL prefix found in: 'invalid_source'"

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO table1 FROM 's3://invalid_source' FILES=('foobar.csv') FORCE=true")

    assert "091016 (22000)" in str(excinfo.value)
    assert "invalid_source" in str(excinfo.value)
    assert "foobar.csv" in str(excinfo.value)

    # file has header but the skip header file format option is not used
    sql = """
    COPY INTO table1
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


def create_table(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE TABLE schema1.table1 (a INT, b INT)")


def upload_file(s3_client: S3Client, data: str, key: str = "foo.txt") -> str:
    bucket = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket


def test_param_files_single():
    sql = """
    COPY INTO table1 (a, b)
    FROM @mystage
    files=('file1.csv')
    FORCE=true
    """
    expr = sqlglot.parse_one(sql, read="snowflake")
    assert isinstance(expr, exp.Copy)
    assert _params(expr).files == ["file1.csv"]


def test_param_files_multiple():
    sql = """
    COPY INTO table1 (a, b)
    FROM @mystage
    FILES=('file1.csv', 'file2.csv')
    FORCE=true
    """
    expr = sqlglot.parse_one(sql, read="snowflake")
    assert isinstance(expr, exp.Copy)
    assert _params(expr).files == ["file1.csv", "file2.csv"]

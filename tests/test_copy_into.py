# ruff: noqa: E501
from __future__ import annotations

import uuid
from datetime import timezone
from typing import NamedTuple
from unittest.mock import MagicMock, patch

import pytest
import snowflake.connector.cursor
import sqlglot
from dirty_equals import IsNow
from mypy_boto3_s3 import S3Client
from sqlglot import exp

from fakesnow import logger
from fakesnow.copy_into import Params, _params, _source_urls


class Case(NamedTuple):
    sql: str
    expected_inserts: list[str]
    csv_data: str
    expected_rows_loaded: int
    expected_data: list[dict[str, int | None]]


cases = [
    pytest.param(
        Case(
            sql="""
            COPY INTO table1
            FROM 's3://{bucket}/'
            FILES=('foo.csv')
            FILE_FORMAT = (TYPE = 'CSV')
            FORCE = TRUE;
            """,
            expected_inserts=["INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"],
            csv_data="1,2\n3,4",
            expected_rows_loaded=2,
            expected_data=[{"A": 1, "B": 2}, {"A": 3, "B": 4}],
        ),
        id="csv",
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO schema1.table1 (b)
            FROM 's3://{bucket}/'
            STORAGE_INTEGRATION = some_name
            FILES=('foo.csv')
            FILE_FORMAT = (TYPE = 'CSV')
            FORCE = TRUE;
            """,
            expected_inserts=[
                "INSERT INTO SCHEMA1.TABLE1 (B) SELECT column0 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"
            ],
            csv_data="1,2\n",
            expected_rows_loaded=1,
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
            FILES=('foo.csv')
            FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
            FORCE = TRUE;
            """,
            expected_inserts=[
                "INSERT INTO TABLE1 (A, B) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE, skip = 1)"
            ],
            csv_data="a,b\n1,2\n",
            expected_rows_loaded=1,
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="skip header",
    ),
    pytest.param(
        Case(
            sql="""
            COPY INTO table1
            FROM 's3://{bucket}/'
            FILES=('foo.csv')
            FORCE = TRUE;
            """,
            expected_inserts=["INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"],
            csv_data="1,2\n",
            expected_rows_loaded=1,
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
            FILES=('foo.csv')
            FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
            FORCE = TRUE;
            """,
            expected_inserts=[
                "INSERT INTO TABLE1 (A, B) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE, sep = '|')"
            ],
            csv_data="1|2\n",
            expected_rows_loaded=1,
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="pipe delimiter",
    ),
]


def captured_inserts(mock_log_sql: MagicMock) -> list[str]:
    """
    Return the captured SQL inserts from mock_log_sql.
    """
    return [call[0][0] for call in mock_log_sql.call_args_list if call[0][0].startswith("INSERT INTO")]


@pytest.mark.parametrize("case", cases)
@patch("fakesnow.copy_into.logger.log_sql", side_effect=logger.log_sql)
def test_execute(
    mock_log_sql: MagicMock, case: Case, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client
) -> None:
    create_table(dcur)
    bucket = upload_file(s3_client, case.csv_data)
    dcur.execute(case.sql.format(bucket=bucket))

    assert captured_inserts(mock_log_sql) == [i.format(bucket=bucket) for i in case.expected_inserts]
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
            "status": "LOADED",
            "rows_parsed": case.expected_rows_loaded,
            "rows_loaded": case.expected_rows_loaded,
            "error_limit": 1,
            "errors_seen": 0,
            "first_error": None,
            "first_error_line": None,
            "first_error_character": None,
            "first_error_column_name": None,
        }
    ]
    assert dcur.description
    assert dcur.rowcount == 1, "rowcount should be length of results not number of rows loaded"
    dcur.execute("SELECT * FROM schema1.table1")
    assert dcur.fetchall() == case.expected_data


@patch("fakesnow.copy_into.logger.log_sql", side_effect=logger.log_sql)
def test_execute_two_files(
    mock_log_sql: MagicMock, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client
) -> None:
    create_table(dcur)
    bucket = str(uuid.uuid4())
    upload_file(s3_client, "1,2\n3,4", bucket=bucket, key="foo.csv")
    upload_file(s3_client, "5,6\n7,8", bucket=bucket, key="bar.csv")

    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FILES=('foo.csv', 'bar.csv')
    FORCE = TRUE;
    """

    dcur.execute(sql.format(bucket=bucket))

    assert captured_inserts(mock_log_sql) == [
        f"INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)",
        f"INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/bar.csv', header = FALSE)",
    ]
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
            "status": "LOADED",
            "rows_parsed": 2,
            "rows_loaded": 2,
            "error_limit": 1,
            "errors_seen": 0,
            "first_error": None,
            "first_error_line": None,
            "first_error_character": None,
            "first_error_column_name": None,
        },
        {
            "file": f"s3://{bucket}/bar.csv",
            "status": "LOADED",
            "rows_parsed": 2,
            "rows_loaded": 2,
            "error_limit": 1,
            "errors_seen": 0,
            "first_error": None,
            "first_error_line": None,
            "first_error_character": None,
            "first_error_column_name": None,
        },
    ]
    assert dcur.description
    assert dcur.rowcount == 2, "rowcount should be length of results not number of rows loaded"
    dcur.execute("SELECT * FROM schema1.table1")
    assert dcur.fetchall() == [
        {"A": 1, "B": 2},  # from foo.csv
        {"A": 3, "B": 4},  # from foo.csv
        {"A": 5, "B": 6},  # from bar.csv
        {"A": 7, "B": 8},  # from bar.csv
    ]


def test_load_history(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)
    bucket = str(uuid.uuid4())
    upload_file(s3_client, "1,2\n3,4", bucket=bucket, key="foo.csv")

    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FILES=('foo.csv')
    FORCE = TRUE;
    """

    dcur.execute(sql.format(bucket=bucket))
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
            "status": "LOADED",
            "rows_parsed": 2,
            "rows_loaded": 2,
            "error_limit": 1,
            "errors_seen": 0,
            "first_error": None,
            "first_error_line": None,
            "first_error_character": None,
            "first_error_column_name": None,
        }
    ]
    dcur.execute("SELECT * FROM information_schema.load_history")
    assert dcur.fetchall() == [
        {
            "SCHEMA_NAME": "SCHEMA1",
            "FILE_NAME": f"s3://{bucket}/foo.csv",
            "TABLE_NAME": "TABLE1",
            "LAST_LOAD_TIME": IsNow(tz=timezone.utc),
            "STATUS": "LOADED",
            "ROW_COUNT": 2,  # number of rows loaded
            "ROW_PARSED": 2,
            "FIRST_ERROR_MESSAGE": None,
            "FIRST_ERROR_LINE_NUMBER": None,
            "FIRST_ERROR_CHARACTER_POSITION": None,
            "FIRST_ERROR_COL_NAME": None,
            "ERROR_COUNT": 0,
            "ERROR_LIMIT": 1,
        }
    ]


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
    FILES=('foo.csv')
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


def upload_file(s3_client: S3Client, data: str, bucket: str | None = None, key: str = "foo.csv") -> str:
    if not bucket:
        bucket = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket


def test_param_files_single():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket/'
    files=('file1.csv')
    FORCE=true
    """)
    assert params.files == ["file1.csv"]
    assert _source_urls(expr, params.files) == ["s3://mybucket/file1.csv"]


def test_param_files_prefix():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket/pre'
    FILES=('file1.csv')
    FORCE=true
    """)
    assert params.files == ["file1.csv"]
    assert _source_urls(expr, params.files) == ["s3://mybucket/prefile1.csv"]


def test_param_files_prefix_query():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket/pre?query=1'
    FILES=('file1.csv')
    FORCE=true
    """)
    assert params.files == ["file1.csv"]
    assert _source_urls(expr, params.files) == ["s3://mybucket/pre?query=1file1.csv"]


def test_param_files_host_without_trailing_slash():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket'
    FILES=('file1.csv')
    FORCE=true
    """)
    assert params.files == ["file1.csv"]
    assert _source_urls(expr, params.files) == ["s3://mybucket/file1.csv"]


def test_params_files_multiple():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket/data/'
    FILES=('file1.csv', 'file2.csv')
    FORCE=true
    """)

    assert params.files == ["file1.csv", "file2.csv"]
    assert _source_urls(expr, params.files) == ["s3://mybucket/data/file1.csv", "s3://mybucket/data/file2.csv"]


def test_param_files_none():
    expr, params = parse("""
    COPY INTO table1 (a, b)
    FROM 's3://mybucket/myfile.csv'
    FORCE=true
    """)
    assert params.files == []
    assert _source_urls(expr, params.files) == ["s3://mybucket/myfile.csv"]


def parse(sql: str) -> tuple[exp.Copy, Params]:
    expr = sqlglot.parse_one(sql, read="snowflake")
    assert isinstance(expr, exp.Copy)
    return expr, _params(expr)

# ruff: noqa: E501
from __future__ import annotations

import os
import re
import tempfile
import uuid
from datetime import timezone
from typing import NamedTuple, cast
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import snowflake.connector.cursor
import sqlglot
from dirty_equals import IsNow
from mypy_boto3_s3 import S3Client
from sqlglot import exp

from fakesnow import logger
from fakesnow.copy_into import CopyParams, _from_source, _params, _source_urls, _strip_json_extract


class Case(NamedTuple):
    sql: str
    expected_inserts: list[str]
    data: str | bytes
    expected_rows_loaded: int
    expected_data: list[dict[str, int | None]]
    stage: str | None = None


cases = [
    pytest.param(
        Case(
            sql="""
            COPY INTO table1
            FROM 's3://{bucket}/'
            FILES=('foo.csv')
            FILE_FORMAT = (TYPE = 'CSV')
            """,
            expected_inserts=["INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"],
            data="1,2\n3,4",
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
            """,
            expected_inserts=[
                "INSERT INTO SCHEMA1.TABLE1 (B) SELECT column0 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"
            ],
            data="1,2\n",
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
            """,
            expected_inserts=[
                "INSERT INTO TABLE1 (A, B) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE, skip = 1)"
            ],
            data="a,b\n1,2\n",
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
            """,
            expected_inserts=["INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)"],
            data="1,2\n",
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
            """,
            expected_inserts=[
                "INSERT INTO TABLE1 (A, B) SELECT column0, column1 FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE, sep = '|')"
            ],
            data="1|2\n",
            expected_rows_loaded=1,
            expected_data=[{"A": 1, "B": 2}],
        ),
        id="pipe delimiter",
    ),
    pytest.param(
        Case(
            stage="create stage stage1 url='s3://{bucket}/'",
            sql="""
            COPY INTO table1
            FROM (SELECT $1:B::integer, $1:A::integer FROM @stage1)
            FILES=('foo.parquet')
            FILE_FORMAT = (TYPE = 'PARQUET')
            """,
            expected_inserts=[
                "INSERT INTO TABLE1 SELECT CAST((B) AS BIGINT), CAST((A) AS BIGINT) FROM READ_PARQUET('s3://{bucket}/foo.parquet')"
            ],
            data=pd.DataFrame({"A": [1, 2], "B": [11, 12]}).to_parquet(),
            expected_rows_loaded=2,
            # columns are swapped to test the order of the select
            expected_data=[{"A": 11, "B": 1}, {"A": 12, "B": 2}],
        ),
        id="parquet",
    ),
]


@pytest.mark.parametrize("case", cases)
@patch("fakesnow.copy_into.logger.log_sql", side_effect=logger.log_sql)
def test_execute(
    mock_log_sql: MagicMock, case: Case, dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client
) -> None:
    create_table(dcur)
    match = re.search(r"FILES=\('([^']+)'\)", case.sql)
    assert match
    key = match.group(1)
    bucket = upload_file(s3_client, case.data, key=key)
    if case.stage:
        dcur.execute(case.stage.format(bucket=bucket))
    dcur.execute(case.sql.format(bucket=bucket))

    assert captured_inserts(mock_log_sql) == [i.format(bucket=bucket) for i in case.expected_inserts]
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/{key}",
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


def test_execute_internal_stage_server(sdcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    dcur = sdcur

    create_table(dcur)
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        # use internal stage
        dcur.execute("CREATE STAGE stage3")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage3")

        dcur.execute("LIST @stage3")
        results = dcur.fetchall()
        assert len(results) == 1

        sql = """
        COPY INTO table1
        FROM @stage3
        PURGE = TRUE
        """

        dcur.execute(sql)
        assert dcur.fetchall() == [
            {
                "file": f"stage3/{temp_file_basename}.gz",
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

        # check that the file has been removed from the stage because PURGE was set to TRUE
        dcur.execute("LIST @stage3")
        results = dcur.fetchall()
        assert len(results) == 0


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
    FILES=('bar.csv', 'foo.csv')
    """

    dcur.execute(sql.format(bucket=bucket))

    excepted_inserts = [
        f"INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/bar.csv', header = FALSE)",
        f"INSERT INTO TABLE1 SELECT * FROM READ_CSV('s3://{bucket}/foo.csv', header = FALSE)",
    ]
    assert captured_inserts(mock_log_sql) == excepted_inserts
    mock_log_sql.reset_mock()

    expected_results = [
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
    ]
    assert dcur.fetchall() == expected_results
    assert dcur.description
    assert dcur.rowcount == 2, "rowcount should be length of results not number of rows loaded"
    dcur.execute("SELECT * FROM schema1.table1")
    assert dcur.fetchall() == [
        {"A": 5, "B": 6},  # from bar.csv
        {"A": 7, "B": 8},  # from bar.csv
        {"A": 1, "B": 2},  # from foo.csv
        {"A": 3, "B": 4},  # from foo.csv
    ]

    # without the FILES clause should load all files in the bucket
    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FORCE = TRUE
    """

    dcur.execute(sql.format(bucket=bucket))
    assert captured_inserts(mock_log_sql) == excepted_inserts
    assert dcur.fetchall() == expected_results


def test_execute_no_files(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    bucket = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket)

    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    """

    dcur.execute(sql.format(bucket=bucket))
    assert dcur.fetchall() == [{"status": "Copy executed with 0 files processed."}]


def test_errors(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO table1 FROM 'invalid_source' FILES=('foobar.csv')")

    assert str(excinfo.value) == "001011 (42601): SQL compilation error:\ninvalid URL prefix found in: 'invalid_source'"

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("COPY INTO table1 FROM 's3://invalid_source' FILES=('foobar.csv')")

    assert "091016 (22000)" in str(excinfo.value)
    assert "invalid_source" in str(excinfo.value)
    assert "foobar.csv" in str(excinfo.value)

    # external locations cannot be used in a copy transformation ie: a FROM (SELECT ...) clause
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute(
            """
            COPY INTO table1
            FROM (SELECT * FROM 's3://bucket/foo.csv')
        """
        )
    assert (
        str(excinfo.value)
        == "001011 (42601): SQL compilation error:\ninvalid URL prefix found in: 's3://bucket/foo.csv'"
    )

    # file has header but the skip header file format option is not used
    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FILES=('foo.csv')
    FILE_FORMAT = (TYPE = 'CSV')
    """
    csv_data = "a,b\n1,2\n"

    bucket = upload_file(s3_client, csv_data)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute(sql.format(bucket=bucket))

    assert "100038 (22018)" in str(excinfo.value)


def test_errors_abort_statement(_fakesnow: None) -> None:
    with (
        snowflake.connector.connect(database="db1", schema="schema1", paramstyle="qmark") as conn,
        cast(snowflake.connector.cursor.DictCursor, conn.cursor(snowflake.connector.cursor.DictCursor)) as dcur,
    ):
        create_table(dcur)
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            dcur.execute(
                """
                COPY INTO table1
                FROM 's3://foobar/'
                FILES=('foo.csv')
                ON_ERROR = ABORT_STATEMENT
                """
            )
        assert "091016 (22000)" in str(excinfo.value)

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            dcur.execute(
                """
                COPY INTO table1
                FROM 's3://foobar/'
                FILES=('foo.csv')
                ON_ERROR = ?
                """,
                ("abort_statement",),
            )
        assert "091016 (22000)" in str(excinfo.value)


def test_errors_parquet(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute(
            """
            COPY INTO table1
            FROM 's3://bucket/'
            FILES=('foo.parquet')
            FILE_FORMAT = (TYPE = 'PARQUET')
        """
        )
    assert (
        str(excinfo.value)
        == "002019 (0A000): SQL compilation error:\nPARQUET file format can produce one and only one column of type variant, object, or array. Load data into separate columns using the MATCH_BY_COLUMN_NAME copy option or copy with transformation."
    )


def test_force(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)
    bucket = str(uuid.uuid4())
    upload_file(s3_client, "1,2", bucket=bucket, key="foo.csv")

    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FILES=('foo.csv')
    """

    dcur.execute(sql.format(bucket=bucket))
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
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

    # reloading the same file should skip it
    dcur.execute(sql.format(bucket=bucket))
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
            "status": "LOAD_SKIPPED",
            "rows_parsed": 0,
            "rows_loaded": 0,
            "error_limit": None,
            "errors_seen": 1,
            "first_error": "File was loaded before.",
            "first_error_line": None,
            "first_error_character": None,
            "first_error_column_name": None,
        }
    ]

    # reloading the same file with force will load it again
    dcur.execute(f"{sql} FORCE = TRUE".format(bucket=bucket))
    assert dcur.fetchall() == [
        {
            "file": f"s3://{bucket}/foo.csv",
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
    assert dcur.fetchall() == [
        {"A": 1, "B": 2},  # first load
        {"A": 1, "B": 2},  # reload with force = true
    ]

    # two rows, for the first load and the reload with force = true
    dcur.execute("SELECT * FROM information_schema.load_history")
    assert len(dcur.fetchall()) == 2

    # TODO: TRUNCATE TABLE should reset the load history


def test_load_history(dcur: snowflake.connector.cursor.DictCursor, s3_client: S3Client) -> None:
    create_table(dcur)
    bucket = str(uuid.uuid4())
    upload_file(s3_client, "1,2\n3,4", bucket=bucket, key="foo.csv")

    sql = """
    COPY INTO table1
    FROM 's3://{bucket}/'
    FILES=('foo.csv')
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


def test_param_files_single():
    expr, params = parse("""
    COPY INTO table1
    FROM 's3://mybucket/'
    files=('file1.csv')
    """)
    assert params.files == ["file1.csv"]
    from_source = _from_source(expr)
    assert _source_urls(from_source, params.files) == ["s3://mybucket/file1.csv"]


def test_param_files_prefix():
    expr, params = parse("""
    COPY INTO table1
    FROM 's3://mybucket/pre'
    FILES=('file1.csv')
    """)
    assert params.files == ["file1.csv"]
    from_source = _from_source(expr)
    assert _source_urls(from_source, params.files) == ["s3://mybucket/prefile1.csv"]


def test_param_files_prefix_query():
    expr, params = parse("""
    COPY INTO table1
    FROM 's3://mybucket/pre?query=1'
    FILES=('file1.csv')
    """)
    assert params.files == ["file1.csv"]
    from_source = _from_source(expr)
    assert _source_urls(from_source, params.files) == ["s3://mybucket/pre?query=1file1.csv"]


def test_param_files_host_without_trailing_slash():
    expr, params = parse("""
    COPY INTO table1
    FROM 's3://mybucket'
    FILES=('file1.csv')
    """)
    assert params.files == ["file1.csv"]
    from_source = _from_source(expr)
    assert _source_urls(from_source, params.files) == ["s3://mybucket/file1.csv"]


def test_params_files_multiple():
    expr, params = parse("""
    COPY INTO table1
    FROM 's3://mybucket/data/'
    FILES=('file1.csv', 'file2.csv')
    """)

    assert params.files == ["file1.csv", "file2.csv"]
    from_source = _from_source(expr)
    assert _source_urls(from_source, params.files) == ["s3://mybucket/data/file1.csv", "s3://mybucket/data/file2.csv"]


def test__strip_json_extract():
    sql = 'SELECT $1:"A"::integer, $1:"B"::integer FROM @stage1'

    assert (
        sqlglot.parse_one(
            sql,
            read="snowflake",
        )
        .transform(_strip_json_extract)
        .sql()
        # TODO: quoted????
        == "SELECT CAST(A AS INT), CAST(B AS INT) FROM @stage1"
    )

    # after all our transforms sql becomes
    sql = "SELECT CAST((JSON_EXTRACT_PATH_TEXT($1, 'A')) AS BIGINT), CAST((JSON_EXTRACT_PATH_TEXT($1, 'B')) AS BIGINT) FROM @stage1"

    assert (
        sqlglot.parse_one(
            sql,
            read="snowflake",
        )
        .transform(_strip_json_extract)
        .sql()
        == "SELECT CAST((A) AS BIGINT), CAST((B) AS BIGINT) FROM @stage1"
    )


def captured_inserts(mock_log_sql: MagicMock) -> list[str]:
    """
    Return the captured SQL inserts from mock_log_sql.
    """
    return [
        call[0][0]
        for call in mock_log_sql.call_args_list
        # filter out create stage inserts
        if call[0][0].startswith("INSERT INTO") and "_fs_stages" not in call[0][0]
    ]


def create_table(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE TABLE schema1.table1 (a INT, b INT)")


def upload_file(s3_client: S3Client, data: str | bytes, bucket: str | None = None, key: str = "foo.csv") -> str:
    if not bucket:
        bucket = str(uuid.uuid4())
    s3_client.create_bucket(Bucket=bucket)
    s3_client.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket


def parse(sql: str) -> tuple[exp.Copy, CopyParams]:
    expr = sqlglot.parse_one(sql, read="snowflake")
    assert isinstance(expr, exp.Copy)
    cparams = _params(expr)
    return expr, cparams

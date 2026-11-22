# ruff: noqa: E501

import bz2
import datetime
import gzip
import os
import tempfile
from base64 import b64decode
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal
from pathlib import Path
from threading import Barrier
from typing import Protocol

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
import pytz
import requests
import snowflake.connector
from dirty_equals import IsDatetime, IsInt, IsStr, IsUUID
from pandas.testing import assert_frame_equal
from snowflake.connector.constants import QueryStatus
from snowflake.connector.cursor import ResultMetadata

from tests.utils import indent


def test_server_abort_request(server: dict) -> None:
    # network_timeout is set to 0 to trigger an abort
    with snowflake.connector.connect(**server | {"network_timeout": 0}) as conn1, conn1.cursor() as cur:
        cur.execute("select 'will abort'")


def test_server_binding_qmark(server: dict):
    with (
        snowflake.connector.connect(**server, database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
    ):
        cur.execute(
            # test most important types from
            # https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-api#data-type-mappings-for-qmark-and-numeric-bindings
            """
            create or replace table example (
                XINT INT, XDECIMAL DECIMAL(10,2), XFLOAT REAL,
                XSTR TEXT, XUNICODE TEXT, XBYTES BINARY, XBYTEARRAY BINARY, XBOOL BOOLEAN,
                XDATE DATE, XTIME TIME, XDATETIME TIMESTAMP_NTZ
            )
            """
        )
        cur.execute(
            "insert into example values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                1,
                10.23,
                1.23456,
                "Jenny",
                "Jenny",
                b"Jenny",
                bytearray(b"Jenny"),
                True,
                datetime.date(2023, 1, 1),
                datetime.time(1, 2, 3, 123456),
                datetime.datetime(2023, 1, 2, 3, 4, 5, 123456),
            ),
        )
        cur.execute("select * from example")
        assert cur.fetchall() == [
            (
                1,
                Decimal("10.23"),
                1.23456,
                "Jenny",
                "Jenny",
                bytearray(b"Jenny"),
                bytearray(b"Jenny"),
                True,
                datetime.date(2023, 1, 1),
                datetime.time(1, 2, 3, 123456),
                datetime.datetime(2023, 1, 2, 3, 4, 5, 123456),
            )
        ]

        cur.execute("select * from example where xint = ?", (1,))


def test_server_binding_timestamp_ltz(server: dict) -> None:
    with (
        snowflake.connector.connect(**server, database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
    ):
        cur.execute("create or replace table example (value timestamp_ntz)")
        timestamp = datetime.datetime(2026, 1, 1, 10)

        cur.execute("insert into example values (?)", (("TIMESTAMP_LTZ", timestamp),))

        cur.execute("select value from example")
        assert cur.fetchone() == (timestamp,)


class ServerQuery(Protocol):
    def __call__(self, sql: str, bindings: dict | None = None, *, user_agent: str | None = None) -> dict: ...


@pytest.fixture
def server_query(server: dict) -> Iterator[ServerQuery]:
    """Exercise the Snowflake HTTP protocol directly for contracts used by other drivers."""
    base_url = f"http://{server['host']}:{server['port']}"
    login = requests.post(
        f"{base_url}/session/v1/login-request",
        params={"databaseName": "db1", "schemaName": "schema1"},
        json={"data": {"SESSION_PARAMETERS": server["session_parameters"]}},
        timeout=5,
    )
    login.raise_for_status()
    headers = {"Authorization": 'Snowflake Token="' + login.json()["data"]["token"] + '"'}

    def query(sql: str, bindings: dict | None = None, *, user_agent: str | None = None) -> dict:
        response = requests.post(
            f"{base_url}/queries/v1/query-request",
            headers=headers | ({"User-Agent": user_agent} if user_agent else {}),
            json={"sqlText": sql, "bindings": bindings},
            timeout=5,
        )
        response.raise_for_status()
        return response.json()

    yield query
    requests.post(f"{base_url}/session?delete=true", headers=headers, timeout=5).raise_for_status()


def test_server_binding_named(server_query: ServerQuery) -> None:
    # Named server-side bindings are used by the .NET connector, not Python's pyformat style.
    assert server_query("create table customers (id int, name text)")["success"]
    assert server_query("insert into customers values (1, 'Jenny'), (2, 'Jasper')")["success"]
    result = server_query(
        "select * from customers where id = :myid and name = :myname",
        {"myid": {"type": "FIXED", "value": "2"}, "myname": {"type": "TEXT", "value": "Jasper"}},
    )
    assert result["success"] is True
    assert result["data"]["total"] == 1


def test_server_connect(sconn: snowflake.connector.SnowflakeConnection) -> None:
    conn = sconn

    assert conn.database == "DB1"
    assert conn.schema == "SCHEMA1"

    conn.execute_string("create database db2; use database db2; create schema schema2; use schema schema2;")
    assert conn.database == "DB2"
    assert conn.schema == "SCHEMA2"


def test_server_connect_autocommit_false(server: dict) -> None:
    # connect with autocommit=False
    with (
        snowflake.connector.connect(**server | {"autocommit": False}, database="db1", schema="schema1") as conn,
        conn.cursor() as cur,
    ):
        cur.execute("create table test_commit (i int)")
        cur.execute("insert into test_commit values (1)")

        # verify row is present in current transaction
        cur.execute("select count(*) from test_commit")
        row = cur.fetchone()
        assert row and row[0] == 1

        # rollback
        conn.rollback()

        # verify row is gone
        cur.execute("select count(*) from test_commit")
        row = cur.fetchone()
        assert row and row[0] == 0

        # insert again
        cur.execute("insert into test_commit values (1)")
        conn.commit()

        # verify row is present
        cur.execute("select count(*) from test_commit")
        row = cur.fetchone()
        assert row and row[0] == 1


def test_server_client_session_keep_alive(server: dict) -> None:
    with snowflake.connector.connect(**server | {"client_session_keep_alive": True}):
        # shouldn't error
        pass


def test_server_login_reports_server_version(server: dict) -> None:
    # the jdbc driver reads data.serverVersion from the login response and serves it as
    # DatabaseMetaData.getDatabaseProductVersion(). the python connector ignores it, so assert
    # on the response itself.
    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/v1/login-request",
        json={"data": {"ACCOUNT_NAME": "fakesnow", "LOGIN_NAME": "fake", "SESSION_PARAMETERS": {}}},
        timeout=5,
    )

    assert response.status_code == 200
    assert response.json()["data"]["serverVersion"] == "10.0.0"


def test_server_executemany_qmark(server: dict) -> None:
    with (
        snowflake.connector.connect(**server, database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
    ):
        cur.execute("create or replace table example (id int, name varchar)")
        cur.executemany("insert into example values (?, ?)", [(1, "one"), (2, "two"), (3, "three")])

        cur.execute("select id, name from example order by id")
        assert cur.fetchall() == [
            (1, "one"),
            (2, "two"),
            (3, "three"),
        ]


def test_server_array_binding_returns_one_count_row(server_query: ServerQuery) -> None:
    assert server_query("create table example (id int)")["success"]
    result = server_query("insert into example values (?)", {"1": {"type": "FIXED", "value": ["1", "2", "3"]}})

    # Snowflake's response has one result row containing the affected-row count.
    assert result["data"]["rowset"] == [["3"]]
    assert result["data"]["total"] == 1


def test_server_result_scan_of_array_binding_returns_aggregate_count(server_query: ServerQuery) -> None:
    assert server_query("create temporary table batch_scan (id int)")["success"]
    original = server_query("insert into batch_scan values (?)", {"1": {"type": "FIXED", "value": ["1", "2", "3"]}})
    assert original["success"]

    scan = server_query(f"select * from table(result_scan('{original['data']['queryId']}'))")
    assert scan["success"]
    table = pa.ipc.open_stream(b64decode(scan["data"]["rowsetBase64"])).read_all()
    assert table.to_pylist() == [{"number of rows inserted": 3}]


def test_server_merge_response_total(server_query: ServerQuery) -> None:
    assert server_query("create table target (id int, name varchar)")["success"]
    assert server_query("insert into target values (1, 'a')")["success"]
    result = server_query(
        """
        merge into target using (select 1 as id, 'a' as name) source on target.id = source.id
        when matched and target.name is distinct from source.name then update set name = source.name
        when not matched then insert (id, name) values (source.id, source.name)
        """
    )

    # A zero-effect MERGE still returns one row of operation counts.
    assert result["data"]["total"] == 1


def test_server_explicit_begin(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # clients that manage transactions themselves send BEGIN rather than relying on the
    # connector, eg: database/sql in Go calls it from Tx.Begin
    scur.execute("create table test_begin (i int)")

    scur.execute("BEGIN TRANSACTION")
    assert scur.fetchall() == [("Statement executed successfully.",)]

    scur.execute("insert into test_begin values (1)")
    scur.execute("ROLLBACK")

    scur.execute("select count(*) from test_begin")
    assert scur.fetchall() == [(0,)]


def test_server_close(server: dict) -> None:
    with snowflake.connector.connect(**server) as conn:
        # conn.close() ignores errors so we call the endpoint directly
        assert conn.rest and conn.rest.token
        response = requests.post(
            f"http://{server['host']}:{server['port']}/session?delete=true",
            headers={"Authorization": f'Snowflake Token="{conn.rest.token}"'},
            timeout=5,
            json={},
        )
        assert response.status_code == 200
        assert response.json()["success"]


def test_server_errors(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur = scur
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("select * from this_table_does_not_exist")

    assert excinfo.value.errno == 2003
    assert excinfo.value.sqlstate == "42S02"
    assert excinfo.value.msg
    assert "THIS_TABLE_DOES_NOT_EXIST" in excinfo.value.msg


def test_server_fetch_pandas_all(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur = scur

    cur.execute("select * from values (1, 'Salted'), (2, 'Caramel') as t(id, flavour)")

    expected_df = pd.DataFrame(
        [
            # TODO: snowflake returns int8
            (np.int32(1), "Salted"),
            (np.int32(2), "Caramel"),
        ],
        columns=["ID", "FLAVOUR"],
    )

    assert_frame_equal(cur.fetch_pandas_all(), expected_df)


def test_server_no_gzip(server: dict) -> None:
    # mimic the go snowflake connector which does not gzip requests
    headers = {
        "Accept": "application/snowflake",
        "Content-Type": "application/json",
        "Accept-Encoding": "gzip",
        "User-Agent": "Go/2.4.0 (darwin-arm64) gc/go1.27.4",
        "Client_App_Id": "Go",
        "Client_App_Version": "2.4.0",
    }

    login_payload = {
        "data": {
            "CLIENT_APP_ID": "Go",
            "CLIENT_APP_VERSION": "2.4.0",
            "SVN_REVISION": "",
            "ACCOUNT_NAME": "fakesnow",
            "LOGIN_NAME": "fake",
            "PASSWORD": "snow",
            "SESSION_PARAMETERS": {"CLIENT_VALIDATE_DEFAULT_PARAMETERS": True},
            "CLIENT_ENVIRONMENT": {
                "APPLICATION": "Go",
                "OS": "darwin",
                "OS_VERSION": "gc-arm64",
                "OCSP_MODE": "FAIL_OPEN",
                "GO_VERSION": "go1.27.4",
            },
        }
    }

    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/v1/login-request",
        headers=headers,
        json=login_payload,
        timeout=5,
    )
    assert response.status_code == 200
    assert response.json()["success"]
    token = response.json()["data"]["token"]

    payload = {
        "sqlText": "SELECT current_timestamp() as TIME, current_user() as USER, current_role() as ROLE;",
        "asyncExec": False,
        "sequenceId": 1,
        "isInternal": False,
        "queryContextDTO": {},
    }

    response = requests.post(
        f"http://{server['host']}:{server['port']}/queries/v1/query-request?requestId=uuid1&request_guid=uuid2",
        headers=headers | {"Authorization": f'Snowflake Token="{token}"'},
        json=payload,
        timeout=5,
    )
    assert response.status_code == 200
    assert response.json()["success"]


def test_server_session_heartbeat(server: dict) -> None:
    headers = {
        "Accept": "application/snowflake",
        "Content-Type": "application/json",
    }
    # Let the connector issue a complete login request, as it does for a real Snowflake session.
    with snowflake.connector.connect(**server) as conn:
        assert conn.rest
        token = conn.rest.token
        assert token
        auth_headers = headers | {"Authorization": f'Snowflake Token="{token}"'}

        response = requests.post(
            f"http://{server['host']}:{server['port']}/session/heartbeat",
            headers=auth_headers,
            timeout=5,
        )
        assert response.status_code == 200
        assert response.json()["success"] is True


def test_server_session_heartbeat_missing_authorization(server: dict) -> None:
    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/heartbeat",
        timeout=5,
    )

    assert response.status_code == 401
    assert response.json()["success"] is False
    assert response.json()["code"] == "390103"
    assert response.json()["message"] == "Session token not found in the request data."


def test_server_session_heartbeat_invalid_authorization(server: dict) -> None:
    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/heartbeat",
        headers={"Authorization": 'Snowflake Token="invalid"'},
        timeout=5,
    )

    assert response.status_code == 401
    assert response.json()["success"] is False
    assert response.json()["code"] == "390104"
    assert response.json()["message"] == "User must login again to access the service."


def test_server_nop_regexes(server: dict) -> None:
    with snowflake.connector.connect(**server | {"session_parameters": {"nop_regexes": ["^CALL.*"]}}) as conn:
        cur = conn.cursor()
        cur.execute("call this_procedure_does_not_exist('foo', 'bar');")
        assert cur.fetchall() == [("Statement executed successfully.",)]


def test_server_put_list(sdcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur = sdcur

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        dcur.execute("CREATE STAGE stage1")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage1")
        put_results = dcur.fetchall()
        assert put_results == [
            {
                "source": temp_file_basename,
                "target": f"{temp_file_basename}.gz",
                "source_size": len(data),
                # Snowflake client-side encryption can make internal-stage targets
                # larger; fakesnow stores plain local files.
                "target_size": IsInt(ge=len(data)),
                "source_compression": "NONE",
                "target_compression": "GZIP",
                "status": "UPLOADED",
                "message": "",
            }
        ]

        dcur.execute("LIST @stage1")
        results = dcur.fetchall()
        assert len(results) == 1
        assert results[0] == {
            "name": f"stage1/{temp_file_basename}.gz",
            "size": put_results[0]["target_size"],
            "md5": IsStr(regex=r"^[0-9a-f]{32}$"),
            "last_modified": IsDatetime(
                # string in RFC 7231 date format (e.g. 'Sat, 31 May 2025 08:50:51 GMT')
                format_string="%a, %d %b %Y %H:%M:%S GMT"
            ),
        }

        # fully qualified stage name quoted
        dcur.execute('CREATE STAGE db1.schema1."stage2"')
        dcur.execute(f"PUT 'file://{temp_file_path}' @db1.schema1.\"stage2\"")


@pytest.mark.parametrize("auto_compress", [True, False])
@pytest.mark.parametrize("pattern", ["*.csv", "???.csv"], ids=["star", "question"])
def test_server_put_wildcard_uploads_each_file(
    sdcur: snowflake.connector.cursor.DictCursor, tmp_path: Path, auto_compress: bool, pattern: str
) -> None:
    (tmp_path / "one.csv").write_text("1,2\n")
    (tmp_path / "two.csv").write_text("3,4\n")
    (tmp_path / "ignored.txt").write_text("ignored\n")
    sdcur.execute("CREATE OR REPLACE STAGE wildcard_stage")

    sdcur.execute(
        f"PUT 'file://{tmp_path}/{pattern}' @wildcard_stage/nested AUTO_COMPRESS={str(auto_compress).upper()}"
    )
    suffix = ".gz" if auto_compress else ""
    assert sorted((row["source"], row["target"], row["status"]) for row in sdcur.fetchall()) == [
        ("one.csv", f"one.csv{suffix}", "UPLOADED"),
        ("two.csv", f"two.csv{suffix}", "UPLOADED"),
    ]

    sdcur.execute("LIST @wildcard_stage")
    assert sorted(row["name"] for row in sdcur.fetchall()) == [
        f"wildcard_stage/nested/one.csv{suffix}",
        f"wildcard_stage/nested/two.csv{suffix}",
    ]


@pytest.mark.parametrize("filename", ["data.csv.bz2", "data.csv"], ids=["extension", "magic-bytes"])
def test_server_put_preserves_bzip2_filename(
    sdcur: snowflake.connector.cursor.DictCursor, tmp_path: Path, filename: str
) -> None:
    source = tmp_path / filename
    source.write_bytes(bz2.compress(b"1,2\n"))
    sdcur.execute("CREATE OR REPLACE STAGE compressed_stage")

    sdcur.execute(f"PUT 'file://{source}' @compressed_stage")
    result = sdcur.fetchall()
    assert [(row["source"], row["target"], row["source_compression"], row["target_compression"]) for row in result] == [
        (filename, filename, "BZIP2", "BZIP2")
    ]

    sdcur.execute("LIST @compressed_stage")
    assert [row["name"] for row in sdcur.fetchall()] == [f"compressed_stage/{filename}"]


@pytest.mark.parametrize(
    ("user_agent", "filename"),
    [
        ("JDBC/4.4.1", "data.csv.gz"),
        ("JDBC/4.4.1", "data[final].csv.gz"),
        ("Go/2.4.0", "data.csv"),
        ("Go/2.4.0", "data.csv.gz"),
    ],
    ids=["jdbc-plain", "jdbc-brackets", "go-uncompressed", "go-compressed"],
)
def test_server_put_presigned_url(server_query: ServerQuery, user_agent: str, filename: str) -> None:
    # JDBC and Go request another URL using the final upload filename.
    assert server_query("CREATE OR REPLACE STAGE presigned_stage")["success"]
    response = server_query(f"PUT 'file://{filename}' @presigned_stage", user_agent=user_agent)
    url = response["data"]["stageInfo"]["presignedUrl"]
    data = gzip.compress(b"1,2\n") if filename.endswith(".gz") else b"1,2\n"
    uploaded = requests.put(url, data=data, timeout=5)
    assert uploaded.status_code == 200

    listed = server_query("LIST @presigned_stage")
    table = pa.ipc.open_stream(b64decode(listed["data"]["rowsetBase64"])).read_all()
    assert [row["name"] for row in table.to_pylist()] == [f"presigned_stage/{filename}"]


@pytest.mark.parametrize(
    "source",
    [
        "/tmp/*[ab].csv",
        "/tmp/multi-*[ab].csv",
        "/tmp/*.csv",
        "/tmp/???.csv",
        "/tmp/[ab].csv",
        "/tmp/dir*/input.csv",
    ],
    ids=["collision", "mixed", "star", "question", "character-class", "directory-glob"],
)
def test_server_put_go_wildcard_rejected_before_upload(server_query: ServerQuery, source: str) -> None:
    # Go can issue indistinguishable presigned URL requests for different local files.
    assert server_query("CREATE OR REPLACE STAGE go_stage")["success"]
    result = server_query(f"PUT 'file://{source}' @go_stage", user_agent="Go/2.4.0")
    assert result["success"] is False
    assert "wildcard" in result["message"].lower()

    listed = server_query("LIST @go_stage")
    assert listed["success"]
    assert listed["data"]["total"] == 0


def test_server_put_jdbc_stage_info_has_endpoint(server_query: ServerQuery) -> None:
    assert server_query("CREATE OR REPLACE STAGE jdbc_stage")["success"]
    result = server_query("PUT 'file:///tmp/data.csv' @jdbc_stage", user_agent="JDBC/4.4.1")
    assert result["data"]["stageInfo"]["endPoint"] is None


def test_server_concurrent_presigned_requests_share_session(server_query: ServerQuery) -> None:
    # JDBC requests URLs for multiple files concurrently on the same connection.
    assert server_query("CREATE OR REPLACE STAGE concurrent_stage")["success"]
    barrier = Barrier(12)

    def request_url(index: int) -> dict:
        barrier.wait(timeout=10)
        return server_query(f"PUT 'file://file{index}.csv.gz' @concurrent_stage", user_agent="JDBC/4.4.1")

    with ThreadPoolExecutor(max_workers=12) as executor:
        results = list(executor.map(request_url, range(12)))

    assert all(result["success"] for result in results)
    assert len({result["data"]["stageInfo"]["presignedUrl"] for result in results}) == 12


def test_server_put_source_with_hash(sdcur: snowflake.connector.cursor.DictCursor, tmp_path: Path) -> None:
    source = tmp_path / "input#tag.csv"
    source.write_text("1,2\n")
    sdcur.execute("CREATE OR REPLACE STAGE hash_source_stage")
    sdcur.execute(f"PUT 'file://{source}' @hash_source_stage AUTO_COMPRESS=FALSE")
    assert [row["status"] for row in sdcur.fetchall()] == ["UPLOADED"]

    sdcur.execute("LIST @hash_source_stage")
    assert [row["name"] for row in sdcur.fetchall()] == ["hash_source_stage/input#tag.csv"]


def test_server_put_relative_source(sdcur: snowflake.connector.cursor.DictCursor) -> None:
    with tempfile.TemporaryDirectory(dir=".") as directory:
        source = os.path.join(directory, "input.csv")
        with open(source, "wb") as file:
            file.write(b"1,2\n")

        sdcur.execute("CREATE STAGE relative_stage")
        sdcur.execute(f"PUT 'file://{os.path.relpath(source)}' @relative_stage AUTO_COMPRESS=FALSE")
        assert sdcur.fetchall()[0]["status"] == "UPLOADED"

        sdcur.execute("LIST @relative_stage")
        assert [row["name"] for row in sdcur.fetchall()] == ["relative_stage/input.csv"]


@pytest.mark.parametrize("stage_name", ['"bound_stage"', 'db1.schema1."qualified_bound_stage"'])
def test_server_put_qmark_quoted(server: dict, stage_name: str) -> None:
    with (
        snowflake.connector.connect(
            **server,
            database="db1",
            schema="schema1",
            paramstyle="qmark",
        ) as conn,
        conn.cursor(snowflake.connector.cursor.DictCursor) as dcur,
        tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file,
    ):
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        # Quote stage names as clients such as write_pandas do.
        dcur.execute("CREATE STAGE identifier(?)", (stage_name,))
        dcur.execute(f"PUT 'file://{temp_file_path}' ?", (f"@{stage_name}",))
        assert [row["status"] for row in dcur.fetchall()] == ["UPLOADED"]

        dcur.execute(f"LIST @{stage_name}")
        stage_prefix = stage_name.rsplit(".", 1)[-1].strip('"')
        assert [row["name"] for row in dcur.fetchall()] == [f"{stage_prefix}/{temp_file_basename}.gz"]


def test_server_put_non_existent_stage(sdcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur = sdcur

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        temp_file_path = temp_file.name

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            dcur.execute(f"PUT 'file://{temp_file_path}' @foobar")

        assert excinfo.value.errno == 2003
        assert excinfo.value.sqlstate == "02000"
        assert "Stage 'DB1.SCHEMA1.FOOBAR' does not exist or not authorized." in str(excinfo.value)


def test_server_put_rejects_path_outside_stage(server: dict) -> None:
    with (
        snowflake.connector.connect(**server | {"network_timeout": 1}, database="db1", schema="schema1") as conn,
        conn.cursor() as cur,
        tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file,
    ):
        temp_file.write("1,2\n")
        temp_file.flush()
        cur.execute("CREATE STAGE source_stage")
        cur.execute("CREATE STAGE other_stage")

        # The connector maps query-error responses to ProgrammingError, even when
        # the server's underlying PUT error is an OperationalError.
        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur.execute(f"PUT 'file://{temp_file.name}' @source_stage/../OTHER_STAGE")

        assert excinfo.value.errno == 253003
        assert "While putting file(s) there was an error:" in str(excinfo.value)


def test_server_bucket_upload_rejects_path_outside_bucket(server: dict) -> None:
    response = requests.put(f"http://{server['host']}:{server['port']}/fs_bucket//etc/passwd", data=b"x", timeout=5)

    assert response.status_code == 400


def test_server_response_params(server: dict) -> None:
    # mimic the jdbc driver
    headers = {
        "client_app_id": "JDBC",
        "client_app_version": "3.22.0",
        "accept": "application/json",
        "accept-encoding": "",
        "user-agent": "JDBC/3.22.0 (Mac OS X 15.3.1) JAVA/21.0.5",
    }

    login_payload = {
        "data": {
            "ACCOUNT_NAME": "127",
            "CLIENT_APP_ID": "JDBC",
            "CLIENT_APP_VERSION": "3.22.0",
            "CLIENT_ENVIRONMENT": {
                "tracing": "INFO",
                "OS": "Mac OS X",
                "OCSP_MODE": "FAIL_OPEN",
                "JAVA_VM": "OpenJDK 64-Bit Server VM",
                "APPLICATION": "DBeaver_DBeaver",
                "JDBC_JAR_NAME": "snowflake-jdbc-3.22.0",
                "password": "****",
                "database": "TEST_DB",
                "application": "DBeaver_DBeaver",
                "OS_VERSION": "15.3.1",
                "serverURL": "http://127.0.0.1:8000/",
                "JAVA_VERSION": "21.0.5",
                "user": "fake",
                "account": "127",
                "JAVA_RUNTIME": "OpenJDK Runtime Environment",
            },
            "EXT_AUTHN_DUO_METHOD": "push",
            "LOGIN_NAME": "fake",
            "PASSWORD": "snow",
            "SESSION_PARAMETERS": {},
        },
        "inFlightCtx": None,
    }

    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/v1/login-request",
        headers=headers,
        json=login_payload,
        timeout=5,
    )
    assert response.status_code == 200
    assert response.json()["success"]

    # expected by the JDBC driver
    assert {"name": "AUTOCOMMIT", "value": True} in response.json()["data"]["parameters"]

    # expected by the .NET connector
    assert "sessionInfo" in response.json()["data"]

    # set autocommit=False
    response = requests.post(
        f"http://{server['host']}:{server['port']}/session/v1/login-request",
        headers=headers,
        json=login_payload | {"data": {"SESSION_PARAMETERS": {"AUTOCOMMIT": False}}},
        timeout=5,
    )
    assert response.status_code == 200
    assert response.json()["success"]

    assert {"name": "AUTOCOMMIT", "value": False} in response.json()["data"]["parameters"]


def test_server_rowcount(scur: snowflake.connector.cursor.SnowflakeCursor):
    cur = scur

    cur.execute("select * from values ('Salted'), ('Caramel')")
    assert cur.rowcount == 2


@pytest.mark.parametrize(
    "sql",
    [
        "use role foobar",
        "use database db1",
        "use schema schema1",
        "use warehouse foobar",
    ],
)
def test_server_query_response_has_use_statement_type_id(server_query: ServerQuery, sql: str) -> None:
    assert server_query(sql)["data"]["statementTypeId"] == 0x4300


def test_server_sfid(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur = scur
    assert not cur.sfqid
    cur.execute("select 1")
    assert cur.sfqid == IsUUID()


def test_server_types_no_result_set(sconn: snowflake.connector.SnowflakeConnection) -> None:
    cur = sconn.cursor()
    cur.execute(
        """
        create or replace table example (
            XBOOLEAN BOOLEAN, XINT INT, XFLOAT FLOAT, XDECIMAL DECIMAL(10,2),
            XVARCHAR VARCHAR,
            XDATE DATE, XTIME TIME, XTIMESTAMP TIMESTAMP_TZ, XTIMESTAMP_NTZ TIMESTAMP_NTZ,
            XBINARY BINARY, /* XARRAY ARRAY, XOBJECT OBJECT, */ XVARIANT VARIANT
        )
        """
    )
    cur.execute("select * from example")
    # fmt: off
    assert cur.description == [
        ResultMetadata(name='XBOOLEAN', type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        # TODO: is_nullable should be False
        ResultMetadata(name='XINT', type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True),
        ResultMetadata(name='XFLOAT', type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name="XDECIMAL", type_code=0, display_size=None, internal_size=None, precision=10, scale=2, is_nullable=True),
        ResultMetadata(name="XVARCHAR", type_code=2, display_size=16777216, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XDATE', type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XTIME', type_code=12, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP', type_code=7, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XTIMESTAMP_NTZ', type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True),
        ResultMetadata(name='XBINARY', type_code=11, display_size=None, internal_size=8388608, precision=None, scale=None, is_nullable=True),
        # TODO: handle ARRAY and OBJECT see https://github.com/tekumara/fakesnow/issues/26
        # ResultMetadata(name='XARRAY', type_code=10, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        # ResultMetadata(name='XOBJECT', type_code=9, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True),
        ResultMetadata(name='XVARIANT', type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True)

    ]
    # fmt: on


def test_server_types(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur = scur
    cur.execute(
        # TODO: match columns names without using AS
        """
        select
            true, 1::int, 2.0::float, to_decimal('12.3456', 10,2),
            'hello', 'hello'::varchar(20),
            to_date('2018-04-15'), to_time('04:15:29.123456'), to_timestamp_tz('2013-04-05 01:02:03.123456'), to_timestamp_ntz('2013-04-05 01:02:03.123456'),
            /* X'41424320E29D84', ARRAY_CONSTRUCT('foo'), */ OBJECT_CONSTRUCT('k','v1'), 1.23::VARIANT
            ,array_size(parse_json('["a","b"]')) /* duckdb uint64 */
        """
    )
    assert indent(cur.fetchall()) == [
        (
            True,
            1,
            2.0,
            Decimal("12.35"),
            "hello",
            "hello",
            datetime.date(2018, 4, 15),
            datetime.time(4, 15, 29, 123456),
            datetime.datetime(2013, 4, 5, 1, 2, 3, 123456, tzinfo=pytz.utc),
            datetime.datetime(2013, 4, 5, 1, 2, 3, 123456),
            # TODO
            # bytearray(b"ABC \xe2\x9d\x84"),
            # '[\n  "foo"\n]',
            '{\n  "k": "v1"\n}',
            "1.23",
            2,
        )
    ]


@pytest.mark.parametrize("type_", ["timestamp_ntz", "timestamp_ltz", "timestamp_tz"])
def test_server_null_timestamp(scur: snowflake.connector.cursor.SnowflakeCursor, type_: str) -> None:
    scur.execute(f"select null::{type_}")
    assert scur.fetchall() == [(None,)]


def test_server_retrieves_async_query_result(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    scur.execute_async("select 42 as answer, 'hello world' as message")
    sfqid = scur.sfqid
    assert sfqid

    scur.get_results_from_sfqid(sfqid)
    assert scur.fetchall() == [(42, "hello world")]


def test_server_retrieves_stored_query_result_after_source_is_dropped(
    scur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    scur.execute("create table example as select 42 as answer")
    scur.execute("select answer from example")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute("drop table example")
    scur.get_results_from_sfqid(sfqid)
    assert scur.fetchall() == [(42,)]


def test_server_result_scan_after_source_table_is_dropped(
    scur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    scur.execute("create temporary table result_scan_source as select 42 as answer")
    scur.execute("select answer from result_scan_source")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute("drop table result_scan_source")
    scur.execute(f"select * from table(result_scan('{sfqid}'))")
    assert scur.fetchall() == [(42,)]


def test_server_result_scan_variant_after_source_is_dropped(
    scur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    scur.execute("create temporary table result_scan_variant as select parse_json('{\"a\":1}') as value")
    scur.execute("select value from result_scan_variant")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute("drop table result_scan_variant")
    scur.execute(f"select * from table(result_scan('{sfqid}'))")
    assert indent(scur.fetchall()) == [('{\n  "a": 1\n}',)]


def test_server_result_scan_empty_variant_after_source_is_dropped(
    scur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    scur.execute("create temporary table result_scan_variant as select parse_json('{\"a\":1}') as value")
    scur.execute("select value from result_scan_variant where false")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute("drop table result_scan_variant")
    scur.execute(f"select * from table(result_scan('{sfqid}'))")
    assert scur.fetchall() == []
    assert scur.description
    assert [(column.name, column.type_code) for column in scur.description] == [("VALUE", 5)]


def test_server_result_scan_has_own_query_id(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    scur.execute("select 42 as answer")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute(f"select * from table(result_scan('{sfqid}'))")
    assert scur.sfqid and scur.sfqid != sfqid


def test_server_retrieves_dml_and_result_scan_with_independent_rowcounts(
    scur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    scur.execute("create temporary table result_scan_dml (value int)")
    scur.execute("insert into result_scan_dml values (1), (2)")
    sfqid = scur.sfqid
    assert sfqid

    scur.execute(f"select * from table(result_scan('{sfqid}'))")
    scan_sfqid = scur.sfqid
    assert scan_sfqid
    scur.get_results_from_sfqid(scan_sfqid)
    scur.fetchall()
    assert scur.rowcount == 1

    scur.get_results_from_sfqid(sfqid)
    scur.fetchall()
    assert scur.rowcount == 2


def test_server_retrieves_stored_query_result_from_another_connection(server: dict) -> None:
    # Two connections to the same server instance use the same query result cache.
    server["session_parameters"].pop("FAKESNOW_DB_PATH")
    with snowflake.connector.connect(**server) as conn, conn.cursor() as cur:
        cur.execute("select 42")
        sfqid = cur.sfqid
        assert sfqid

    with snowflake.connector.connect(**server) as conn, conn.cursor() as cur:
        cur.get_results_from_sfqid(sfqid)
        assert cur.fetchall() == [(42,)]


def test_server_query_status_for_unknown_id(sconn: snowflake.connector.SnowflakeConnection) -> None:
    assert sconn.get_query_status("00000000-0000-0000-0000-000000000000") == QueryStatus.NO_DATA


def test_server_describe_only(server: dict) -> None:
    # clients describe a statement before executing it, eg: the JDBC driver before a batch
    # insert, and expect the result columns back without the statement having run
    with (
        snowflake.connector.connect(**server, database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
    ):
        cur.execute("create or replace table example (id int, name varchar)")
        cur.execute("insert into example values (1, 'old')")

        def describe(sql: str) -> list[ResultMetadata]:
            metadata = cur.describe(sql)
            assert metadata is not None
            return metadata

        assert [(m.name, m.type_code) for m in describe("select id, name from example where id = ?")] == [
            ("ID", 0),
            ("NAME", 2),
        ]
        assert [m.name for m in describe("insert into example values (?, ?)")] == ["number of rows inserted"]
        assert [m.name for m in describe("update example set name = ? where id = ?")] == [
            "number of rows updated",
            "number of multi-joined rows updated",
        ]
        assert [m.name for m in describe("delete from example where id = ?")] == ["number of rows deleted"]

        merge = """
            merge into example t using (select 1 as id, 'new' as name) s on t.id = s.id
            when matched then update set t.name = s.name
            when not matched then insert values (s.id, s.name)
            """
        assert [m.name for m in describe(merge)] == [
            "number of rows inserted",
            "number of rows updated",
        ]
        assert [
            m.name
            for m in describe("merge into example t using (select 1 as id) s on t.id = s.id when matched then delete")
        ] == ["number of rows deleted"]

        assert [m.name for m in describe("create table side_effect (id int)")] == ["status"]
        cur.execute("select count(*) from information_schema.tables where table_name = 'SIDE_EFFECT'")
        assert cur.fetchall() == [(0,)]

        assert [m.name for m in describe("drop table example")] == ["status"]
        assert [m.name for m in describe("alter table example add column w int")] == ["status"]
        assert [m.name for m in describe("truncate table example")] == ["status"]

        # nothing ran: the original table is untouched
        cur.execute("select * from example")
        assert cur.fetchall() == [(1, "old")]


def test_server_fetch_multiple_arrow_batches(scur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # with more than one thread duckdb 2 returns large results as several arrow batches
    scur.execute(
        """SELECT ID,
            IFF(MOD(ID, 2) = 0, NULL, 'héllo') AS LABEL,
            12.34::NUMBER(10, 2) AS AMOUNT,
            '2024-01-02 03:04:05.123456'::TIMESTAMP_NTZ AS CREATED_AT
        FROM (SELECT SEQ4() AS ID FROM TABLE(GENERATOR(ROWCOUNT => 5000)))
        ORDER BY ID"""
    )

    timestamp = datetime.datetime(2024, 1, 2, 3, 4, 5, 123456)
    assert scur.fetchall() == [(i, None if i % 2 == 0 else "héllo", Decimal("12.34"), timestamp) for i in range(5000)]

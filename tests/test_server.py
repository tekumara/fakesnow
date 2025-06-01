# ruff: noqa: E501

import datetime
import os
import tempfile
from decimal import Decimal

import numpy as np
import pandas as pd
import pytest
import pytz
import requests
import snowflake.connector
import snowflake.connector.pandas_tools
from dirty_equals import IsUUID
from pandas.testing import assert_frame_equal
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


def test_server_client_session_keep_alive(server: dict) -> None:
    with snowflake.connector.connect(**server | {"client_session_keep_alive": True}):
        # shouldn't error
        pass


def test_server_close(server: dict) -> None:
    conn = snowflake.connector.connect(**server)

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
        "User-Agent": "Go/1.13.0 (darwin-arm64) gc/go1.23.6",
        "Client_App_Id": "Go",
        "Client_App_Version": "1.13.0",
    }

    login_payload = {
        "data": {
            "CLIENT_APP_ID": "Go",
            "CLIENT_APP_VERSION": "1.13.0",
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
                "GO_VERSION": "go1.23.6",
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


def test_server_put(sdcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur = sdcur

    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        dcur.execute("CREATE STAGE stage1")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage1")
        assert dcur.fetchall() == [
            {
                "source": temp_file_basename,
                "target": f"{temp_file_basename}.gz",
                "source_size": len(data),
                "target_size": 42,  # GZIP compressed size
                "source_compression": "NONE",
                "target_compression": "GZIP",
                "status": "UPLOADED",
                "message": "",
            }
        ]


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


def test_server_rowcount(scur: snowflake.connector.cursor.SnowflakeCursor):
    cur = scur

    cur.execute("select * from values ('Salted'), ('Caramel')")
    assert cur.rowcount == 2


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
            XVARCHAR VARCHAR, XVARCHAR20 VARCHAR(20),
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
        ResultMetadata(name="XVARCHAR", type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
        # TODO: internal_size matches column size, ie: 20
        ResultMetadata(name='XVARCHAR20', type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True),
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

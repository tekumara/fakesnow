from __future__ import annotations

import gzip
import json
import logging
import secrets
from base64 import b64encode
from dataclasses import dataclass
from typing import Any

import snowflake.connector.errors
from sqlglot import parse_one
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from fakesnow.arrow import to_ipc, to_sf
from fakesnow.converter import from_binding
from fakesnow.expr import normalise_ident
from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from fakesnow.rowtype import describe_as_rowtype

logger = logging.getLogger("fakesnow.server")
# use same format as uvicorn
logger.handlers = logging.getLogger("uvicorn").handlers
logger.setLevel(logging.INFO)

shared_fs = FakeSnow()
sessions: dict[str, FakeSnowflakeConnection] = {}


@dataclass
class ServerError(Exception):
    status_code: int
    code: str
    message: str


async def login_request(request: Request) -> JSONResponse:
    database = (d := request.query_params.get("databaseName")) and normalise_ident(d)
    schema = (s := request.query_params.get("schemaName")) and normalise_ident(s)
    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    body_json = json.loads(body)
    session_params: dict[str, Any] = body_json["data"]["SESSION_PARAMETERS"]
    nop_regexes = session_params.get("nop_regexes")

    if db_path := session_params.get("FAKESNOW_DB_PATH"):
        # isolated creates a new in-memory database, rather than using the shared in-memory database
        # so this connection won't share any tables with other connections
        fs = FakeSnow() if db_path == ":isolated:" else FakeSnow(db_path=db_path)
    else:
        # share the in-memory database across connections
        fs = shared_fs
    token = secrets.token_urlsafe(32)
    logger.info(f"Session login {database=} {schema=} {nop_regexes=}")
    sessions[token] = fs.connect(database, schema, nop_regexes=nop_regexes)
    return JSONResponse(
        {
            "data": {
                "token": token,
                "parameters": [
                    {"name": "AUTOCOMMIT", "value": True},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
                ],
                "sessionInfo": {
                    "databaseName": database,
                    "schemaName": schema,
                },
            },
            "success": True,
        }
    )


async def query_request(request: Request) -> JSONResponse:
    try:
        conn = to_conn(to_token(request))

        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        body_json = json.loads(body)

        sql_text = body_json["sqlText"]

        if bindings := body_json.get("bindings"):
            # Convert parameters like {'1': {'type': 'FIXED', 'value': '10'}, ...} to tuple (10, ...)
            params = tuple(from_binding(bindings[str(pos)]) for pos in range(1, len(bindings) + 1))
            logger.debug(f"Bindings: {params}")
        else:
            params = None

        expr = parse_one(sql_text, read="snowflake")

        try:
            # only a single sql statement is sent at a time by the python snowflake connector
            cur = await run_in_threadpool(conn.cursor().execute, sql_text, binding_params=params, server=True)
            rowtype = describe_as_rowtype(cur._describe_last_sql())  # noqa: SLF001

            expr = cur._last_transformed  # noqa: SLF001
            assert expr
            if put_stage_data := expr.args.get("put_stage_data"):
                # this is a PUT command, so return the stage data
                return JSONResponse(
                    {
                        "data": put_stage_data,
                        "success": True,
                    }
                )

        except snowflake.connector.errors.ProgrammingError as e:
            logger.info(f"{sql_text=} ProgrammingError {e}")
            code = f"{e.errno:06d}"
            return JSONResponse(
                {
                    "data": {
                        "errorCode": code,
                        "sqlState": e.sqlstate,
                    },
                    "code": code,
                    "message": e.msg,
                    "success": False,
                }
            )
        except Exception as e:
            # we have a bug or use of an unsupported feature
            msg = f"{sql_text=} {params=} Unhandled exception"
            logger.error(msg, exc_info=e)
            # my guess at mimicking a 500 error as per https://docs.snowflake.com/en/developer-guide/sql-api/reference
            # and https://github.com/snowflakedb/gosnowflake/blob/8ed4c75ffd707dd712ad843f40189843ace683c4/restful.go#L318
            raise ServerError(status_code=500, code="261000", message=msg) from None

        if cur._arrow_table:  # noqa: SLF001
            batch_bytes = to_ipc(to_sf(cur._arrow_table, rowtype))  # noqa: SLF001
            rowset_b64 = b64encode(batch_bytes).decode("utf-8")
        else:
            rowset_b64 = ""

        return JSONResponse(
            {
                "data": {
                    "parameters": [
                        {"name": "TIMEZONE", "value": "Etc/UTC"},
                    ],
                    "rowtype": rowtype,
                    "rowsetBase64": rowset_b64,
                    "total": cur._rowcount,  # noqa: SLF001
                    "queryId": cur.sfqid,
                    "queryResultFormat": "arrow",
                    "finalDatabaseName": conn.database,
                    "finalSchemaName": conn.schema,
                },
                "success": True,
            }
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def to_token(request: Request) -> str:
    if not (auth := request.headers.get("Authorization")):
        raise ServerError(status_code=401, code="390101", message="Authorization header not found in the request data.")

    return auth[17:-1]


def to_conn(token: str) -> FakeSnowflakeConnection:
    if not (conn := sessions.get(token)):
        raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

    return conn


async def session(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        _ = to_conn(token)

        if bool(request.query_params.get("delete")):
            del sessions[token]

        return JSONResponse(
            {"data": None, "code": None, "message": None, "success": True},
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def monitoring_query(request: Request) -> JSONResponse:
    token = to_token(request)
    conn = to_conn(token)

    sfqid = request.path_params["sfqid"]
    if not conn.results_cache.get(sfqid):
        return JSONResponse({"data": {"queries": []}, "success": True})

    return JSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})


routes = [
    Route(
        "/session/v1/login-request",
        login_request,
        methods=["POST"],
    ),
    Route("/session", session, methods=["POST"]),
    Route(
        "/queries/v1/query-request",
        query_request,
        methods=["POST"],
    ),
    Route("/queries/v1/abort-request", lambda _: JSONResponse({"success": True}), methods=["POST"]),
    Route("/monitoring/queries/{sfqid}", monitoring_query, methods=["GET"]),
]

app = Starlette(debug=True, routes=routes)

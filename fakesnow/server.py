from __future__ import annotations

import gzip
import json
import secrets
from dataclasses import dataclass

from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow

fs = FakeSnow()
sessions = {}


@dataclass
class ServerError(Exception):
    status_code: int
    code: str
    message: str


def login_request(request: Request) -> JSONResponse:
    database = request.query_params.get("databaseName")
    schema = request.query_params.get("schemaName")
    token = secrets.token_urlsafe(32)
    sessions[token] = fs.connect(database, schema)
    return JSONResponse({"data": {"token": token}, "success": True})


async def query_request(request: Request) -> JSONResponse:
    try:
        conn = to_conn(request)

        body = await request.body()
        body_json = json.loads(gzip.decompress(body))

        sql_text = body_json["sqlText"]

        # only a single sql statement is sent at a time by the python snowflake connector
        cur = run_in_threadpool(conn.cursor().execute, sql_text)

        # TODO:
        # a = cur._arrow_table

        return JSONResponse(
            {
                "data": {
                    "rowtype": [
                        {
                            "name": "'HELLO WORLD'",
                            "database": "",
                            "schema": "",
                            "table": "",
                            "nullable": False,
                            "byteLength": 44,
                            "type": "text",
                            "length": 11,
                            "scale": None,
                            "precision": None,
                            "collation": None,
                        }
                    ],
                    "rowsetBase64": "/////0ABAAAQAAAAAAAKAA4ABgANAAgACgAAAAAABAAQAAAAAAEKAAwAAAAIAAQACgAAAAgAAAAIAAAAAAAAAAEAAAAYAAAAAAASABgAFAATABIADAAAAAgABAASAAAAFAAAAMQAAADIAAAAAAAFAcQAAAAEAAAAiAAAAFgAAAAsAAAABAAAAJD///8IAAAADAAAAAIAAAAxMQAACgAAAGNoYXJMZW5ndGgAALT///8IAAAAEAAAAAQAAABURVhUAAAAAAsAAABsb2dpY2FsVHlwZQDc////CAAAAAwAAAACAAAANDQAAAoAAABieXRlTGVuZ3RoAAAIAAwACAAEAAgAAAAIAAAADAAAAAMAAABMT0IADAAAAHBoeXNpY2FsVHlwZQAAAAAAAAAABAAEAAQAAAANAAAAJ0hFTExPIFdPUkxEJwAAAP////+YAAAAFAAAAAAAAAAMABYADgAVABAABAAMAAAAIAAAAAAAAAAAAAQAEAAAAAADCgAYAAwACAAEAAoAAAAUAAAASAAAAAEAAAAAAAAAAAAAAAMAAAAAAAAAAAAAAAEAAAAAAAAACAAAAAAAAAAIAAAAAAAAABAAAAAAAAAACwAAAAAAAAAAAAAAAQAAAAEAAAAAAAAAAAAAAAAAAAABAAAAAAAAAAAAAAALAAAAaGVsbG8gd29ybGQAAAAAAP////8AAAAA",
                    "queryResultFormat": "arrow",
                },
                "success": True,
            }
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def to_conn(request: Request) -> FakeSnowflakeConnection:
    if not (auth := request.headers.get("Authorization")):
        raise ServerError(status_code=401, code="390103", message="Session token not found in the request data.")

    token = auth[17:-1]

    if not (conn := sessions.get(token)):
        raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

    return conn


routes = [
    Route(
        "/session/v1/login-request",
        login_request,
        methods=["POST"],
    ),
    Route(
        "/queries/v1/query-request",
        query_request,
        methods=["POST"],
    ),
]

app = Starlette(debug=True, routes=routes)

from __future__ import annotations

import gzip
import json
import logging
import secrets
from base64 import b64encode
from dataclasses import dataclass
from typing import Any

import snowflake.connector.errors
from sqlglot import Expr, exp, parse_one
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from fakesnow import statement_type
from fakesnow.arrow import to_ipc, to_sf
from fakesnow.converter import from_binding
from fakesnow.cursor import FakeSnowflakeCursor
from fakesnow.expr import normalise_ident
from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from fakesnow.rowtype import ColumnInfo, describe_as_rowtype
from fakesnow.statement_type import DML_TYPE_IDS, statement_type_id

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
    autocommit = session_params.get("AUTOCOMMIT", True)

    if db_path := session_params.get("FAKESNOW_DB_PATH"):
        # isolated creates a new in-memory database, rather than using the shared in-memory database
        # so this connection won't share any tables with other connections
        fs = FakeSnow() if db_path == ":isolated:" else FakeSnow(db_path=db_path)
    else:
        # share the in-memory database across connections
        fs = shared_fs
    token = secrets.token_urlsafe(32)
    logger.info(f"Session login {database=} {schema=} {nop_regexes=}")
    sessions[token] = fs.connect(database, schema, nop_regexes=nop_regexes, autocommit=autocommit)
    return JSONResponse(
        {
            "data": {
                "token": token,
                "parameters": [
                    {"name": "AUTOCOMMIT", "value": autocommit},
                    {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
                    # clients read this to decide whether to bind an array inline or stage it
                    # first. 0 keeps them inline, ie: no PUT, which we don't support well yet.
                    {"name": "CLIENT_STAGE_ARRAY_BINDING_THRESHOLD", "value": 0},
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

        params: Any = None
        # rows of params, when the client sends an array binding
        batch: list[tuple[Any, ...]] | None = None

        if bindings := body_json.get("bindings"):
            if not all(k.isdigit() for k in bindings):
                # named bindings: {'myid': {'type': 'FIXED', 'value': '10'}, ...} -> dict {'myid': 10, ...}
                params = {name: from_binding(b) for name, b in bindings.items()}
            elif any(isinstance(b["value"], list) for b in bindings.values()):
                # array bindings hold a list of values per placeholder, one per row, and are sent
                # when executing a batch, eg: cursor.executemany. transpose them into rows, ie:
                # {'1': {'type': 'FIXED', 'value': ['1', '2']}, ...} -> [(1, ...), (2, ...)]
                columns = [
                    [from_binding({**b, "value": v}) for v in b["value"]]
                    for b in (bindings[str(pos)] for pos in range(1, len(bindings) + 1))
                ]
                batch = list(zip(*columns, strict=True))
            else:
                # positional bindings: {'1': {'type': 'FIXED', 'value': '10'}, ...} -> tuple (10, ...)
                params = tuple(from_binding(bindings[str(pos)]) for pos in range(1, len(bindings) + 1))
            logger.debug(f"Bindings: {batch if batch is not None else params}")

        expr = parse_one(sql_text, read="snowflake")
        type_id = statement_type_id(expr)

        if body_json.get("describeOnly"):
            cur = conn.cursor()
            if (described := await run_in_threadpool(cur._describe_only, sql_text)) is not None:  # noqa: SLF001
                return describe_only_response(conn, cur, describe_as_rowtype(described), expr, type_id)
            # we can only describe this statement by running it, which is what we've always done

        batch_rowcount = 0

        try:
            # only a single sql statement is sent at a time by the python snowflake connector
            cur = conn.cursor()
            if batch is None:
                await run_in_threadpool(cur.execute, sql_text, binding_params=params, server=True)
            else:
                # snowflake runs an array binding as one statement and reports the rows affected
                # across the whole batch, so accumulate them as we execute row by row
                for row in batch:
                    await run_in_threadpool(cur.execute, sql_text, binding_params=row, server=True)
                    batch_rowcount += cur.rowcount or 0
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

        arrow_table = cur._arrow_table  # noqa: SLF001

        if batch is not None and arrow_table and type_id in DML_TYPE_IDS:
            # the batch ran as multiple statements, but the client sent one, so report the total
            result: dict[str, Any] = {"queryResultFormat": "json", "rowset": [[str(batch_rowcount)]]}
        elif arrow_table and type_id in DML_TYPE_IDS:
            # DML results are returned as json, because clients read the affected row counts
            # from rowset, eg: SnowflakeCursor._init_result_and_meta
            result = {
                "queryResultFormat": "json",
                "rowset": [
                    [None if v is None else str(v) for v in row]
                    for row in zip(*[c.to_pylist() for c in arrow_table.columns], strict=True)
                ],
            }
        elif arrow_table:
            batch_bytes = to_ipc(to_sf(arrow_table, rowtype))
            result = {"queryResultFormat": "arrow", "rowsetBase64": b64encode(batch_bytes).decode("utf-8")}
        else:
            result = {"queryResultFormat": "arrow", "rowsetBase64": ""}

        return JSONResponse(
            {
                "data": {
                    "parameters": [
                        {"name": "TIMEZONE", "value": "Etc/UTC"},
                    ],
                    "rowtype": rowtype,
                    "total": 1 if batch is not None else arrow_table.num_rows if arrow_table else 0,
                    "queryId": cur.sfqid,
                    "statementTypeId": type_id,
                    "finalDatabaseName": conn.database,
                    "finalSchemaName": conn.schema,
                    **result,
                },
                "success": True,
            }
        )

    except ServerError as e:
        return JSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def describe_only_response(
    conn: FakeSnowflakeConnection,
    cur: FakeSnowflakeCursor,
    rowtype: list[ColumnInfo],
    expr: Expr,
    type_id: int,
) -> JSONResponse:
    """Describe the statement's result without any rows, as snowflake does for describeOnly."""

    # snowflake returns json for everything but a query
    result = (
        {"queryResultFormat": "arrow", "rowsetBase64": ""}
        if type_id == statement_type.SELECT
        else {"queryResultFormat": "json", "rowset": []}
    )
    return JSONResponse(
        {
            "data": {
                "rowtype": rowtype,
                "total": 0,
                "returned": 0,
                "queryId": cur.sfqid,
                "statementTypeId": type_id,
                "numberOfBinds": len(list(expr.find_all(exp.Placeholder))),
                # clients read this to decide whether they can send the batch as an array binding.
                # snowflake only supports it for inserts.
                "arrayBindSupported": type_id == statement_type.INSERT,
                "finalDatabaseName": conn.database,
                "finalSchemaName": conn.schema,
                **result,
            },
            "success": True,
        }
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

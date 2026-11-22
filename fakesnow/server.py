from __future__ import annotations

import asyncio
import glob
import gzip
import json
import logging
import os
import secrets
from base64 import b64encode
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import quote

import snowflake.connector.errors
from sqlglot import Expr, exp
from starlette.applications import Starlette
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from fakesnow import statement_type
from fakesnow.arrow import to_ipc, to_sf
from fakesnow.converter import from_binding
from fakesnow.cursor import CachedQueryResult, FakeSnowflakeCursor, QueryResultMetadata
from fakesnow.dialect import parse_one
from fakesnow.expr import normalise_ident
from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from fakesnow.rowtype import ColumnInfo, describe_as_rowtype
from fakesnow.statement_type import DML_TYPE_IDS, statement_type_id
from fakesnow.transforms import SERVER_VERSION, stage

logger = logging.getLogger("fakesnow.server")
# use same format as uvicorn
logger.handlers = logging.getLogger("uvicorn").handlers
logger.setLevel(logging.INFO)


@dataclass
class ServerSession:
    conn: FakeSnowflakeConnection
    # JDBC can request upload URLs concurrently on one connection. DuckDB cannot
    # execute multiple statements concurrently on that connection.
    query_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


shared_fs = FakeSnow()
sessions: dict[str, ServerSession] = {}


@dataclass
class ServerError(Exception):
    status_code: int
    code: str
    message: str

    def response(self) -> JSONResponse:
        return JSONResponse(
            {"data": None, "code": self.code, "message": self.message, "success": False, "headers": None},
            status_code=self.status_code,
        )


async def login_request(request: Request) -> JSONResponse:
    database = (d := request.query_params.get("databaseName")) and normalise_ident(d)
    schema = (s := request.query_params.get("schemaName")) and normalise_ident(s)
    body = await request.body()
    if request.headers.get("Content-Encoding") == "gzip":
        body = gzip.decompress(body)
    body_json = json.loads(body)
    session_params = {name.upper(): value for name, value in body_json["data"]["SESSION_PARAMETERS"].items()}
    # Connector 5 sends session parameter values as strings.
    nop_regexes = session_params.get("NOP_REGEXES")
    if isinstance(nop_regexes, str):
        nop_regexes = json.loads(nop_regexes)
    autocommit = session_params.get("AUTOCOMMIT", True)
    if isinstance(autocommit, str):
        autocommit = autocommit.lower() == "true"

    if db_path := session_params.get("FAKESNOW_DB_PATH"):
        # isolated creates a new in-memory database, rather than using the shared in-memory database
        # so this connection won't share any tables with other connections
        fs = FakeSnow() if db_path == ":isolated:" else FakeSnow(db_path=db_path)
    else:
        # share the in-memory database across connections
        fs = shared_fs
    token = secrets.token_urlsafe(32)
    logger.info(f"Session login {database=} {schema=} {nop_regexes=}")
    sessions[token] = ServerSession(fs.connect(database, schema, nop_regexes=nop_regexes, autocommit=autocommit))
    return JSONResponse(
        {
            "data": {
                "token": token,
                # connector 5.x refuses a login response without a master token
                "masterToken": token,
                "validityInSeconds": 3600,
                "masterValidityInSeconds": 14400,
                "sessionId": len(sessions),
                # clients serve this as the jdbc database product version, and some refuse to run
                # against a version they consider too old
                "serverVersion": SERVER_VERSION,
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
        session = to_session(to_token(request))
        async with session.query_lock:
            return await _query_request(request, session.conn)
    except ServerError as e:
        return e.response()


async def _query_request(request: Request, conn: FakeSnowflakeConnection) -> JSONResponse:
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

    batch_rowcount = 0

    try:
        expr = parse_one(sql_text)
        type_id = statement_type_id(expr)
        cur = conn.cursor()

        if (
            body_json.get("describeOnly")
            and (described := await run_in_threadpool(cur._describe_only, sql_text)) is not None  # noqa: SLF001
        ):
            return describe_only_response(conn, cur, describe_as_rowtype(described), expr, type_id)
        # Statements without a describe-only shortcut are described by executing them.

        # only a single sql statement is sent at a time by the python snowflake connector
        if batch is None:
            await run_in_threadpool(cur.execute, sql_text, binding_params=params, server=True)
        else:
            # An array binding is one statement; the cursor retains its aggregate result.
            await run_in_threadpool(cur.executemany, sql_text, batch, server=True)
            batch_rowcount = cur.rowcount or 0
        transformed = cur._last_transformed  # noqa: SLF001
        assert transformed
        if put_stage_data := transformed.args.get("put_stage_data"):
            return upload_response(request, put_stage_data)

        sfqid = cur.sfqid
        assert sfqid
        cached = conn.results_cache[sfqid]
        metadata = QueryResultMetadata(type_id, batch_rowcount if batch is not None else None)
        response = query_result_response(conn, sfqid, cached, metadata)

    except (snowflake.connector.errors.ProgrammingError, snowflake.connector.errors.OperationalError) as e:
        logger.info(f"{sql_text=} {type(e).__name__} {e}")
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

    cached.metadata = metadata
    return response


def query_result_response(
    conn: FakeSnowflakeConnection, sfqid: str, cached: CachedQueryResult, metadata: QueryResultMetadata
) -> JSONResponse:
    arrow_table = cached.table
    rowtype = cached.rowtype()
    batch_rowcount = metadata.batch_rowcount
    if batch_rowcount is not None and arrow_table and metadata.type_id in DML_TYPE_IDS:
        # the batch ran as multiple statements, but the client sent one, so report the total
        result: dict[str, Any] = {"queryResultFormat": "json", "rowset": [[str(batch_rowcount)]]}
    elif arrow_table and metadata.type_id in DML_TYPE_IDS:
        # DML results are returned as json, because clients read the affected row counts from rowset.
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
                "parameters": [{"name": "TIMEZONE", "value": "Etc/UTC"}],
                "rowtype": rowtype,
                "total": 1 if batch_rowcount is not None else arrow_table.num_rows if arrow_table else 0,
                "queryId": sfqid,
                "statementTypeId": metadata.type_id,
                "finalDatabaseName": conn.database,
                "finalSchemaName": conn.schema,
                **result,
            },
            "success": True,
        }
    )


def query_result(request: Request) -> JSONResponse:
    try:
        conn = to_conn(to_token(request))
        sfqid = request.path_params["sfqid"]
        cached = conn.results_cache.get(sfqid)
        if cached is None or cached.metadata is None:
            raise ServerError(status_code=404, code="000709", message=f"Statement {sfqid} not found")
        return query_result_response(conn, sfqid, cached, cached.metadata)
    except ServerError as e:
        return e.response()


def upload_response(request: Request, put: stage.PutStage) -> JSONResponse:
    """Send uploads over HTTP, even if the client and server don't share a filesystem."""
    client, _, version = request.headers.get("User-Agent", "").partition("/")
    major = version.partition(".")[0]
    if client == "Go" and glob.has_magic(put["source"]):
        # Go's presigned URL refresh can send identical requests for different files.
        # The upload itself does not contain the filename, so guessing risks data loss.
        raise snowflake.connector.errors.ProgrammingError(
            msg="Go wildcard PUT is not supported over HTTP; upload files using paths without *, ? or [.",
            errno=1003,
            sqlstate="0A000",
        )
    if client == "PythonConnector" and major.isdecimal() and int(major) >= 5:
        # Connector 5 normally reuses one presigned URL for all files, so it would misroute
        # wildcard uploads. A GCS-style endpoint lets it append each actual target filename.
        directory = os.path.relpath(put["directory"], stage.LOCAL_BUCKET_PATH).replace(os.sep, "/")
        stage_info = {
            "locationType": "GCS",
            "location": f"fs_bucket/{directory}/",
            "region": "us-east1",
            "endPoint": str(request.base_url).rstrip("/"),
            "creds": {"GCS_ACCESS_TOKEN": "fakesnow"},
        }
    else:
        # Python connector 4.x, Go and JDBC request presigned URLs per file. Their GCS
        # custom endpoints force HTTPS, by giving them a presigned URL we can make them
        # upload to our HTTP server.
        filename = os.path.basename(put["source"])
        target = os.path.join(put["directory"], filename)
        path = quote(os.path.relpath(target, stage.LOCAL_BUCKET_PATH).replace(os.sep, "/"))
        signed_headers = quote("host;x-goog-if-generation-match;x-goog-meta-sfc-digest")
        stage_info = {
            "locationType": "GCS",
            "location": f"{put['stage_name']}/",
            "region": "us-east1",
            "endPoint": None,
            "creds": {},
            "presignedUrl": f"{request.base_url}fs_bucket/{path}?X-Goog-SignedHeaders={signed_headers}",
        }
    return JSONResponse(
        {
            "data": {
                "stageInfo": stage_info,
                "src_locations": [put["source"]],
                "parallel": 4,
                "autoCompress": put["auto_compress"],
                "sourceCompression": "auto_detect",
                "overwrite": False,
                "command": "UPLOAD",
            },
            "success": True,
        }
    )


def _write_bucket_file(target: str, body: bytes, overwrite: bool) -> None:
    os.makedirs(os.path.dirname(target), exist_ok=True)
    with open(target, "wb" if overwrite else "xb") as f:
        f.write(body)


async def bucket_upload(request: Request) -> Response:
    """Store or check an internal-stage file in the local bucket."""
    # normpath is lexical and does not access the filesystem.
    target = os.path.normpath(os.path.join(stage.LOCAL_BUCKET_PATH, request.path_params["path"]))  # noqa: ASYNC240
    if not target.startswith(stage.LOCAL_BUCKET_PATH + os.sep):
        return Response(status_code=400)
    if request.method == "HEAD":
        return Response(status_code=200 if await run_in_threadpool(os.path.isfile, target) else 404)

    body = await request.body()
    overwrite = request.headers.get("x-goog-if-generation-match") != "0"
    try:
        await run_in_threadpool(_write_bucket_file, target, body, overwrite)
    except FileExistsError:
        return Response(status_code=412)
    logger.info(f"Uploaded {len(body)} bytes to {target}")
    return Response()


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
        raise ServerError(status_code=401, code="390103", message="Session token not found in the request data.")

    return auth[17:-1]


def to_session(token: str) -> ServerSession:
    if (session := sessions.get(token)) is None:
        raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

    return session


def to_conn(token: str) -> FakeSnowflakeConnection:
    return to_session(token).conn


async def session(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        to_conn(token)

        if bool(request.query_params.get("delete")):
            del sessions[token]

        return JSONResponse(
            {"data": None, "code": None, "message": None, "success": True},
        )

    except ServerError as e:
        return e.response()


def monitoring_query(request: Request) -> JSONResponse:
    try:
        conn = to_conn(to_token(request))
    except ServerError as e:
        return e.response()

    sfqid = request.path_params["sfqid"]
    if sfqid not in conn.results_cache:
        return JSONResponse({"data": {"queries": []}, "success": True})

    return JSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})


routes = [
    Route(
        "/session/v1/login-request",
        login_request,
        methods=["POST"],
    ),
    Route("/session", session, methods=["POST"]),
    Route("/session/heartbeat", session, methods=["POST"]),
    Route(
        "/queries/v1/query-request",
        query_request,
        methods=["POST"],
    ),
    Route("/queries/v1/abort-request", lambda _: JSONResponse({"success": True}), methods=["POST"]),
    Route("/fs_bucket/{path:path}", bucket_upload, methods=["HEAD", "PUT"]),
    Route("/monitoring/queries/{sfqid}", monitoring_query, methods=["GET"]),
    # Connector 5's status request includes a second slash.
    Route("/monitoring/queries//{sfqid}", monitoring_query, methods=["GET"]),
    Route("/queries/{sfqid}/result", query_result, methods=["GET"]),
]

app = Starlette(debug=True, routes=routes)

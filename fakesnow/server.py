from __future__ import annotations

import gzip
import json
import logging
import os
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


class SafeJSONResponse(JSONResponse):
    """JSONResponse that handles non-serializable types like datetime."""

    def render(self, content: Any) -> bytes:
        return json.dumps(content, default=str).encode("utf-8")

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

    # Session parameters take precedence over environment variable for db path, this allow you to have some sessions
    # share a database and others use isolated databases
    db_path = session_params.get("FAKESNOW_DB_PATH") or os.environ.get("FAKESNOW_DB_PATH")
    if db_path is None:
        # Use the shared in-memory database. This is shared across all sessions and is cleared when the server restarts.
        # Useful for sharing data between sessions without needing to manage database files.
        fs = shared_fs
    elif db_path == ":isolated:":
        # Explicitly setting FAKESNOW_DB_PATH = ":isolated:", creates a new isolated database in memory for every login.
        # Connection close is triggered by the context manager when hitting FakeSnowflakeConnection.__exit__()
        # If used outside of a context manager, users will need to manually close the connection when they're done with
        # it to release resources.
        fs = FakeSnow()
    else:
        # Use the set value for db_path. This instructs fakesnow to persist databases to the filesystem, making it
        # persistent across server restarts.
        fs = FakeSnow(db_path=db_path)
    token = secrets.token_urlsafe(32)
    logger.info(f"[LOGIN] database={database} schema={schema} autocommit={autocommit} nop_regexes={nop_regexes}")
    sessions[token] = fs.connect(database, schema, nop_regexes=nop_regexes, autocommit=autocommit)
    response = {
        "data": {
            "token": token,
            "parameters": [
                {"name": "AUTOCOMMIT", "value": autocommit},
                {"name": "CLIENT_SESSION_KEEP_ALIVE_HEARTBEAT_FREQUENCY", "value": 3600},
            ],
            "sessionInfo": {
                "databaseName": database,
                "schemaName": schema,
            },
        },
        "success": True,
    }
    return SafeJSONResponse(response)


async def query_request(request: Request) -> JSONResponse:
    try:
        conn = to_conn(to_token(request))
        request_id = request.query_params.get("requestId", "unknown")
        logger.debug(f"[QUERY_REQUEST] START requestId={request_id} host={request.client.host if request.client else 'unknown'}")

        body = await request.body()
        if request.headers.get("Content-Encoding") == "gzip":
            body = gzip.decompress(body)

        body_json = json.loads(body)

        sql_text = body_json["sqlText"]
        logger.debug(f"[QUERY_REQUEST] SQL: {sql_text}")

        if bindings := body_json.get("bindings"):
            # Convert parameters like {'1': {'type': 'FIXED', 'value': '10'}, ...} to tuple (10, ...)
            params = tuple(from_binding(bindings[str(pos)]) for pos in range(1, len(bindings) + 1))
            logger.debug(f"[QUERY_REQUEST] Bindings: {params}")
        else:
            params = None

        expr = parse_one(sql_text, read="snowflake")

        try:
            # only a single sql statement is sent at a time by the python snowflake connector
            logger.debug(f"[QUERY_REQUEST] Executing SQL with params={params}")
            cur = await run_in_threadpool(conn.cursor().execute, sql_text, binding_params=params, server=True)
            logger.info(f"[QUERY_REQUEST] SQL execution completed, queryId={cur.sfqid}, rowcount={cur._rowcount}")  # noqa: SLF001
            
            rowtype = describe_as_rowtype(cur._describe_last_sql())  # noqa: SLF001

            expr = cur._last_transformed  # noqa: SLF001
            assert expr
            if put_stage_data := expr.args.get("put_stage_data"):
                # this is a PUT command, so return the stage data
                logger.info(f"[QUERY_REQUEST] PUT command detected, returning stage data")
                return SafeJSONResponse(
                    {
                        "data": put_stage_data,
                        "success": True,
                    }
                )

        except snowflake.connector.errors.ProgrammingError as e:
            logger.error(f"[QUERY_REQUEST] ProgrammingError: {sql_text=} errno={e.errno} {e.msg}")
            code = f"{e.errno:06d}"
            response = {
                "data": {
                    "errorCode": code,
                    "sqlState": e.sqlstate,
                },
                "code": code,
                "message": e.msg,
                "success": False,
            }
            logger.error(f"[QUERY_REQUEST] Returning error response: code={code}")
            return SafeJSONResponse(response)
        except Exception as e:
            # we have a bug or use of an unsupported feature
            msg = f"{sql_text=} {params=} Unhandled exception"
            logger.error(f"[QUERY_REQUEST] {msg}", exc_info=e)
            # my guess at mimicking a 500 error as per https://docs.snowflake.com/en/developer-guide/sql-api/reference
            # and https://github.com/snowflakedb/gosnowflake/blob/8ed4c75ffd707dd712ad843f40189843ace683c4/restful.go#L318
            raise ServerError(status_code=500, code="261000", message=msg) from None

        if cur._arrow_table:  # noqa: SLF001
            batch_bytes = to_ipc(to_sf(cur._arrow_table, rowtype))  # noqa: SLF001
            rowset_b64 = b64encode(batch_bytes).decode("utf-8")
            # Convert arrow table to array of arrays for Node.js SDK
            # SDK expects [[val1, val2], [val1, val2], ...], not [{col: val}, ...]
            rowset_json = [list(row.values()) for row in cur._arrow_table.to_pylist()]  # noqa: SLF001
            logger.debug(f"[QUERY_REQUEST] Arrow table: {len(rowset_json)} rows, rowset_b64 length={len(rowset_b64)}")
        else:
            rowset_b64 = ""
            rowset_json = []
            logger.debug(f"[QUERY_REQUEST] No arrow table, empty result")

        # Cache the result data (limit to 50 most recent)
        cache_data = {
            "parameters": [
                {"name": "TIMEZONE", "value": "Etc/UTC"},
            ],
            "rowtype": rowtype,
            "rowsetBase64": rowset_b64,  # For Python SDK
            "rowset": rowset_json,  # For Node.js SDK
            "total": cur._rowcount,  # noqa: SLF001
            "returned": cur._rowcount,  # noqa: SLF001  # Node.js SDK needs this
            "queryId": cur.sfqid,
            "queryResultFormat": "arrow",
            "version": 1,  # Node.js SDK requires version field
            "chunks": [],  # Node.js SDK expects chunks field (empty list, not None)
            "finalDatabaseName": conn.database,
            "finalSchemaName": conn.schema,
        }
        
        # Store in cache, maintaining max 50 entries (LRU)
        # Store internal tuple format expected by result_scan() and get_results_from_sfqid()
        sfqid = cur.sfqid
        if sfqid is None:
            raise ServerError(status_code=500, code="261001", message="Missing query id after execution")
        conn.results_cache[sfqid] = (
            cur._arrow_table,  # noqa: SLF001
            cur._rowcount,  # noqa: SLF001
            cur._last_sql,  # noqa: SLF001
            cur._last_params,  # noqa: SLF001
            cur._last_transformed,  # noqa: SLF001
            rowtype,  # rowtype needed for get_cached_query_result()
        )
        if len(conn.results_cache) > 50:
            conn.results_cache.popitem(last=False)  # Remove oldest item
        logger.debug(f"[QUERY_REQUEST] Cached result for queryId={cur.sfqid}, cache size={len(conn.results_cache)}")

        # Return cache_data with both rowset and rowsetBase64
        response = {
            "data": cache_data,
            "code": "0",  # 0 = Success, results ready immediately
            "success": True,
        }
        logger.debug(f"[QUERY_REQUEST] END requestId={request_id} queryId={cur.sfqid} rows={cur._rowcount} status=success code=0")  # noqa: SLF001
        return SafeJSONResponse(response)

    except ServerError as e:
        logger.error(f"[QUERY_REQUEST] ServerError: code={e.code} message={e.message}")
        return SafeJSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )

async def get_cached_query_result(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        conn = to_conn(token)
        
        # Extract query_id from path: /queries/{query_id}/result
        query_id = request.path_params.get("query_id")
        request_guid = request.query_params.get("request_guid", "unknown")
        disable_offline_chunks = request.query_params.get("disableOfflineChunks", "unknown")
        
        logger.info(f"[GET_RESULT] START query_id={query_id} request_guid={request_guid} disableOfflineChunks={disable_offline_chunks} client={request.client.host if request.client else 'unknown'}")
        
        if not query_id:
            logger.error(f"[GET_RESULT] Missing query_id in request path")
            raise ServerError(status_code=400, code="002003", message="Missing query_id in request path")
        
        # Retrieve from cache
        cached_tuple = conn.results_cache.get(query_id)

        if not cached_tuple:
            logger.error(f"[GET_RESULT] Query results not found for query_id={query_id}, cache keys: {list(conn.results_cache.keys())}")
            raise ServerError(status_code=404, code="000604", message=f"Query results not found for query_id: {query_id}")

        # Unpack the cached tuple format (arrow_table, rowcount, last_sql, last_params, last_transformed, rowtype)
        arrow_table, rowcount, _, _, _, rowtype = cached_tuple

        # Reconstruct response data from cached tuple
        if arrow_table:
            batch_bytes = to_ipc(to_sf(arrow_table, rowtype))
            rowset_b64 = b64encode(batch_bytes).decode("utf-8")
            rowset_json = [list(row.values()) for row in arrow_table.to_pylist()]
        else:
            rowtype = []
            rowset_b64 = ""
            rowset_json = []

        has_rowset_b64 = bool(rowset_b64)
        rowset_count = len(rowset_json)

        logger.debug(f"[GET_RESULT] Found cached result: rowtype={rowtype}, rows={rowset_count}/{rowcount}, has_rowset_b64={has_rowset_b64}")

        cached_result = {
            "parameters": [
                {"name": "TIMEZONE", "value": "Etc/UTC"},
            ],
            "rowtype": rowtype,
            "rowsetBase64": rowset_b64,
            "rowset": rowset_json,
            "total": rowcount,
            "returned": rowcount,
            "queryId": query_id,
            "queryResultFormat": "arrow",
            "version": 1,
            "chunks": [],
            "finalDatabaseName": conn.database,
            "finalSchemaName": conn.schema,
        }

        # For GET /queries/{query_id}/result endpoint:
        # Return cached result with both rowset (JSON) and rowsetBase64 (Arrow)
        response = {
            "data": cached_result,
            "code": "0",  # 0 = Success, results ready immediately
            "success": True,
        }
        logger.debug(f"[GET_RESULT] END query_id={query_id} status=success code=0 rows={rowset_count}")
        return SafeJSONResponse(response)
        
    except ServerError as e:
        logger.error(f"[GET_RESULT] ServerError: code={e.code} message={e.message}")
        return SafeJSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def to_token(request: Request) -> str:
    if not (auth := request.headers.get("Authorization")):
        logger.error(f"[AUTH] Authorization header not found")
        raise ServerError(status_code=401, code="390101", message="Authorization header not found in the request data.")

    token = auth[17:-1]
    logger.debug(f"[AUTH] Token extracted from Authorization header")
    return token


def to_conn(token: str) -> FakeSnowflakeConnection:
    if not (conn := sessions.get(token)):
        logger.error(f"[AUTH] Session not found for token, available sessions: {len(sessions)}")
        raise ServerError(status_code=401, code="390104", message="User must login again to access the service.")

    logger.debug(f"[AUTH] Session found, database={conn.database} schema={conn.schema}")
    return conn


async def session(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        _ = to_conn(token)

        if bool(request.query_params.get("delete")):
            logger.info(f"[SESSION] DELETE session")
            sessions[token]._duck_conn.close()  # Close the duckdb connection to release resources
            del sessions[token]
        else:
            logger.debug(f"[SESSION] HEARTBEAT")

        return SafeJSONResponse(
            {"data": None, "code": None, "message": None, "success": True},
        )

    except ServerError as e:
        logger.error(f"[SESSION] ServerError: code={e.code} message={e.message}")
        return SafeJSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


def monitoring_query(request: Request) -> JSONResponse:
    try:
        token = to_token(request)
        conn = to_conn(token)

        sfqid = request.path_params["sfqid"]
        if not conn.results_cache.get(sfqid):
            logger.debug(f"[MONITORING] query {sfqid} not found in cache")
            return SafeJSONResponse({"data": {"queries": []}, "success": True})

        logger.debug(f"[MONITORING] query {sfqid} status=SUCCESS")
        return SafeJSONResponse({"data": {"queries": [{"status": "SUCCESS"}]}, "success": True})
    except ServerError as e:
        logger.error(f"[MONITORING] ServerError: code={e.code} message={e.message}")
        return SafeJSONResponse(
            {"data": None, "code": e.code, "message": e.message, "success": False, "headers": None},
            status_code=e.status_code,
        )


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
    Route(
        "/queries/{query_id}/result",
        get_cached_query_result,
        methods=["GET"],
    ),
    Route("/queries/v1/abort-request", lambda _: SafeJSONResponse({"success": True}), methods=["POST"]),
    Route("/monitoring/queries/{sfqid}", monitoring_query, methods=["GET"]),
]

app = Starlette(debug=True, routes=routes)

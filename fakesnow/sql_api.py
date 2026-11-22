"""Snowflake SQL API v2, backed by the same FakeSnow instance as the connector API."""

from __future__ import annotations

import asyncio
import gzip
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal
from typing import Any

import pyarrow as pa
import snowflake.connector.errors
from starlette.concurrency import run_in_threadpool
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Route

from fakesnow.converter import from_binding
from fakesnow.dialect import parse
from fakesnow.fakes import FakeSnowflakeConnection
from fakesnow.instance import FakeSnow
from fakesnow.rowtype import ColumnInfo

logger = logging.getLogger("fakesnow.sql_api")

# Snowflake chooses partition sizes itself; this limit prevents large first responses.
PARTITION_ROWS = 10_000
SYNC_WAIT_SECONDS = 45
_IDENTIFIER = re.compile(r"[A-Za-z_][A-Za-z0-9_$]*\Z")
_UNITS_PER_SECOND = {"s": 1, "ms": 1_000, "us": 1_000_000, "ns": 1_000_000_000}


@dataclass
class Statement:
    handle: str = field(default_factory=lambda: str(uuid.uuid4()))
    created_on: int = field(default_factory=lambda: int(time.time() * 1000))
    task: asyncio.Task[None] | None = None
    response: dict[str, Any] | None = None
    status_code: int = 202
    connection: FakeSnowflakeConnection | None = None
    cancelled: bool = False
    timed_out: bool = False

    @property
    def status_url(self) -> str:
        return f"/api/v2/statements/{self.handle}"

    def status(self, code: str, message: str, sqlstate: str = "00000") -> dict[str, Any]:
        return {
            "code": code,
            "sqlState": sqlstate,
            "message": message,
            "statementHandle": self.handle,
            "statementStatusUrl": self.status_url,
            "createdOn": self.created_on,
        }

    def interrupt(self) -> None:
        self.cancelled = True
        if connection := self.connection:
            connection._duck_conn.interrupt()  # noqa: SLF001


class SqlApi:
    def __init__(self, fs: FakeSnow) -> None:
        self.fs = fs
        self.statements: dict[str, Statement] = {}
        self.requests: dict[str, Statement] = {}
        self.routes = [
            Route("/api/v2/statements", self.submit, methods=["POST"]),
            Route("/api/v2/statements/{handle}", self.get, methods=["GET"]),
            Route("/api/v2/statements/{handle}/cancel", self.cancel, methods=["POST"]),
        ]

    @staticmethod
    def _authorize(request: Request) -> JSONResponse | None:
        # Like the connector login endpoint, Fakesnow does not authenticate credentials.
        auth = request.headers.get("authorization", "")
        scheme, _, credential = auth.partition(" ")
        if scheme.lower() != "bearer" or not credential.strip():
            return JSONResponse(
                {"code": "390146", "message": "Bearer token is missing in the HTTP request authorization header."},
                status_code=401,
            )
        return None

    async def submit(self, request: Request) -> Response:
        if error := self._authorize(request):
            return error
        content_type = request.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        if content_type and content_type != "application/json":
            return JSONResponse(
                {"code": "390000", "message": "Content-Type must be application/json."}, status_code=415
            )
        try:
            body = await request.json()
        except (ValueError, UnicodeDecodeError):
            return self._bad_request("Request body must be valid JSON.")
        if not isinstance(body, dict) or not isinstance(body.get("statement"), str) or not body["statement"].strip():
            return self._bad_request("A non-empty statement is required.")
        for name in ("database", "schema"):
            value = body.get(name)
            if value is not None and (not isinstance(value, str) or not _IDENTIFIER.fullmatch(value)):
                return self._bad_request(f"Invalid {name} identifier.")
        timeout = body.get("timeout", 0)
        if not isinstance(timeout, int) or isinstance(timeout, bool) or timeout < 0:
            return self._bad_request("timeout must be a non-negative integer.")
        if body.get("parameters"):
            return self._bad_request("Statement parameters are not supported.")
        requested_metadata = body.get("resultSetMetaData", {})
        if not isinstance(requested_metadata, dict) or requested_metadata.get("format", "jsonv2") != "jsonv2":
            return self._bad_request("Only jsonv2 result format is supported.")
        try:
            bindings = self._bindings(body.get("bindings", {}))
        except (ValueError, TypeError, KeyError, ArithmeticError) as error:
            return self._bad_request(f"Invalid bindings: {error}")

        request_id = request.query_params.get("requestId")
        retry = request.query_params.get("retry", "false").lower() == "true"
        statement = self.requests.get(request_id) if request_id and retry else None
        if statement is not None and statement.response is not None and statement.status_code != 200:
            # Only a successfully executed (or still running) request is deduplicated.
            statement = None
        if statement is None:
            statement = Statement()
            self.statements[statement.handle] = statement
            if request_id:
                self.requests[request_id] = statement
            nullable = request.query_params.get("nullable", "true").lower() != "false"
            statement.task = asyncio.create_task(self._run(statement, body, bindings, nullable, timeout))

        if request.query_params.get("async", "false").lower() == "true":
            return JSONResponse(statement.status("333334", "Statement is still running."), status_code=202)
        assert statement.task is not None
        try:
            await asyncio.wait_for(asyncio.shield(statement.task), timeout=SYNC_WAIT_SECONDS)
        except TimeoutError:
            return JSONResponse(statement.status("333334", "Statement is still running."), status_code=202)
        # Snowflake reports a timeout on the synchronous submit as 408; polling
        # the same failed statement instead returns 422.
        if statement.timed_out:
            return JSONResponse(statement.response, status_code=408)
        return self._result(statement, 0)

    @staticmethod
    def _bindings(bindings: object) -> tuple[Any, ...] | None:
        if not isinstance(bindings, dict):
            raise ValueError("bindings must be an object")
        if not bindings:
            return None
        if set(bindings) != {str(i) for i in range(1, len(bindings) + 1)}:
            raise ValueError("binding positions must start at 1 and be contiguous")
        values = []
        for i in range(1, len(bindings) + 1):
            binding = bindings[str(i)]
            if not isinstance(binding, dict) or not isinstance(binding.get("type"), str) or "value" not in binding:
                raise ValueError("each binding must have a type and value")
            if binding["value"] is not None and not isinstance(binding["value"], str):
                raise ValueError("binding values must be strings or null")
            values.append(from_binding(binding))
        return tuple(values)

    async def _run(
        self, statement: Statement, body: dict[str, Any], bindings: tuple[Any, ...] | None, nullable: bool, limit: int
    ) -> None:
        worker = asyncio.create_task(run_in_threadpool(self._execute, statement, body, bindings, nullable))
        timed_out = False
        try:
            if limit:
                try:
                    response = await asyncio.wait_for(asyncio.shield(worker), timeout=limit)
                except TimeoutError:
                    timed_out = True
                    statement.timed_out = True
                    statement.interrupt()
                    response = await worker
            else:
                response = await worker
            if timed_out:
                statement.response = statement.status("000630", "Statement reached its timeout.", "57014")
                statement.status_code = 422
            elif statement.cancelled:
                statement.response = statement.status("000604", "Statement was canceled.", "57014")
                statement.status_code = 422
            else:
                statement.response = response
                statement.status_code = 200
        except snowflake.connector.errors.Error as error:
            if statement.cancelled:
                statement.response = statement.status(
                    "000630" if timed_out else "000604",
                    "Statement reached its timeout." if timed_out else "Statement was canceled.",
                    "57014",
                )
            else:
                statement.response = statement.status(
                    f"{error.errno or 0:06d}", error.msg or str(error), error.sqlstate or "XX000"
                )
            statement.status_code = 422
        except Exception:
            if statement.cancelled:
                statement.response = statement.status(
                    "000630" if timed_out else "000604",
                    "Statement reached its timeout." if timed_out else "Statement was canceled.",
                    "57014",
                )
                statement.status_code = 422
            else:
                logger.exception("Unhandled SQL API statement error")
                statement.response = statement.status("261000", "Internal error executing statement.", "XX000")
                statement.status_code = 500

    def _execute(
        self, statement: Statement, body: dict[str, Any], bindings: tuple[Any, ...] | None, nullable: bool
    ) -> dict[str, Any]:
        expressions = [expression for expression in parse(body["statement"]) if expression is not None]
        if len(expressions) != 1:
            raise snowflake.connector.errors.ProgrammingError(
                msg="Exactly one SQL statement is supported.", errno=1003, sqlstate="42000"
            )
        if statement.cancelled:
            return {}
        # Snowflake creates a new session per submission, even when the same bearer token is reused.
        connection = self.fs.connect(body.get("database"), body.get("schema"))
        try:
            if statement.cancelled:
                return {}
            statement.connection = connection
            cursor = connection.cursor()
            cursor.execute(body["statement"], binding_params=bindings, server=True)
            sfqid = cursor.sfqid
            assert sfqid
            result = connection.results_cache[sfqid]
            rowtype = result.rowtype()
            rows = _rows(result.table, rowtype, nullable)
            partitions = [rows[i : i + PARTITION_ROWS] for i in range(0, len(rows), PARTITION_ROWS)] or [[]]
            partition_info = [{"rowCount": len(part), "uncompressedSize": 0} for part in partitions]
            metadata = {
                "format": "jsonv2",
                "numRows": len(rows),
                "rowType": [
                    {
                        "name": column["name"],
                        "type": column["type"],
                        "nullable": column["nullable"],
                        **{key: column[key] for key in ("length", "precision", "scale") if column[key] is not None},
                    }
                    for column in rowtype
                ],
                "partitionInfo": partition_info,
            }
            status = statement.status("090001", "Statement executed successfully.")
            response = status | {"resultSetMetaData": metadata, "data": partitions[0]}
            for i, part in enumerate(partitions[1:], 1):
                partition_info[i]["uncompressedSize"] = len(_json_bytes(status | {"data": part}))
            # The first response includes its own size in the metadata. Iterate until
            # the number of digits stabilizes so it describes the response on the wire.
            while True:
                size = len(_json_bytes(response))
                if partition_info[0]["uncompressedSize"] == size:
                    break
                partition_info[0]["uncompressedSize"] = size
            return response | {"_partitions": partitions}
        finally:
            statement.connection = None
            connection.close()

    async def get(self, request: Request) -> Response:
        if error := self._authorize(request):
            return error
        statement = self.statements.get(request.path_params["handle"])
        if statement is None:
            return JSONResponse({"code": "090105", "message": "Statement not found."}, status_code=404)
        raw_partition = request.query_params.get("partition", "0")
        try:
            partition = int(raw_partition)
        except ValueError:
            return self._bad_request("Invalid partition number.")
        if partition < 0:
            return self._bad_request("Invalid partition number.")
        return self._result(statement, partition, accept_gzip="gzip" in request.headers.get("accept-encoding", ""))

    async def cancel(self, request: Request) -> Response:
        if error := self._authorize(request):
            return error
        statement = self.statements.get(request.path_params["handle"])
        if statement is None:
            return JSONResponse({"code": "090105", "message": "Statement not found."}, status_code=404)
        if statement.task is not None and not statement.task.done():
            statement.interrupt()
        return JSONResponse(statement.status("000000", "Statement canceled."))

    @staticmethod
    def _bad_request(message: str) -> JSONResponse:
        return JSONResponse({"code": "390000", "message": message}, status_code=400)

    def _result(self, statement: Statement, partition: int, accept_gzip: bool = False) -> Response:
        if statement.response is None:
            return JSONResponse(statement.status("333334", "Statement is still running."), status_code=202)
        if statement.status_code != 200:
            return JSONResponse(statement.response, status_code=statement.status_code)
        partitions = statement.response["_partitions"]
        if partition >= len(partitions):
            return self._bad_request("Partition does not exist.")
        response = {key: value for key, value in statement.response.items() if key != "_partitions"}
        if partition:
            response.pop("resultSetMetaData")
            response["data"] = partitions[partition]
            if accept_gzip:
                data = gzip.compress(_json_bytes(response))
                return Response(data, media_type="application/json", headers={"Content-Encoding": "gzip"})
        return JSONResponse(response)


def _json_bytes(value: dict[str, Any]) -> bytes:
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode("utf-8")


def _rows(table: pa.Table, rowtype: list[ColumnInfo], nullable: bool) -> list[list[str | None]]:
    columns = [column.to_pylist() for column in table.columns]
    result: list[list[str | None]] = []
    for index in range(table.num_rows):
        row: list[str | None] = []
        for column, values, info in zip(table.columns, columns, rowtype, strict=True):
            value = values[index]
            if value is None:
                row.append(None if nullable else "null")
            elif info["type"] == "boolean":
                row.append(str(value).lower())
            elif info["type"] == "binary":
                row.append(value.hex().upper())
            elif info["type"] == "date":
                row.append(str((value - date(1970, 1, 1)).days))
            elif info["type"] in {"time", "timestamp_ntz", "timestamp_ltz", "timestamp_tz"}:
                scalar = column[index]
                seconds = Decimal(scalar.value) / _UNITS_PER_SECOND[scalar.type.unit]
                formatted = f"{seconds:.9f}"
                row.append(f"{formatted} 1440" if info["type"] == "timestamp_tz" else formatted)
            elif info["type"] in {"variant", "object", "array"}:
                row.append(json.dumps(json.loads(value), indent=2, ensure_ascii=False))
            else:
                row.append(str(value))
        result.append(row)
    return result

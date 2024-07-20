import secrets

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from fakesnow.instance import FakeSnow

fs = FakeSnow()
sessions = {}


def session_login_request(request: Request) -> JSONResponse:
    database = request.query_params.get("databaseName")
    schema = request.query_params.get("schemaName")
    token = secrets.token_urlsafe(32)
    sessions[token] = fs.connect(database, schema)
    return JSONResponse({"data": {"token": token}, "success": True})


def startup():
    print("Ready to go")


routes = [
    Route(
        "/session/v1/login-request",
        session_login_request,
        methods=["POST"],
    ),
]

app = Starlette(debug=True, routes=routes, on_startup=[startup])


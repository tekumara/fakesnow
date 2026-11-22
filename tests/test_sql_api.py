import json
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from typing import Any
from urllib.parse import parse_qs, urljoin, urlsplit

import pytest
import requests
import snowflake.connector


@pytest.fixture
def api(server: dict) -> Iterator[tuple[str, dict[str, str], str]]:
    url = f"http://{server['host']}:{server['port']}/api/v2/statements"
    headers = {"Authorization": "Bearer fake", "Accept": "application/json"}
    database = f"API_{uuid.uuid4().hex.upper()}"
    yield url, headers, database


def submit(api: tuple[str, dict[str, str], str], statement: str, **kwargs: Any) -> requests.Response:
    url, headers, database = api
    return requests.post(
        url,
        headers=headers,
        json={"statement": statement, "database": database, "schema": "PUBLIC", **kwargs},
        timeout=30,
    )


def test_sql_api_shares_tables_with_connector(api: tuple[str, dict[str, str], str], server: dict) -> None:
    _, _, database = api
    conn_kwargs = server | {"database": database, "schema": "PUBLIC", "session_parameters": {}}
    with snowflake.connector.connect(**conn_kwargs) as conn, conn.cursor() as cur:
        cur.execute("create table orders (id integer, total number(12,2))")
        cur.execute("insert into orders values (1, 12.50), (2, 99.95)")

    response = submit(api, "select id, total from orders order by id")
    assert response.status_code == 200
    assert response.json()["data"] == [["1", "12.50"], ["2", "99.95"]]


def test_sql_api_concurrent_requests_use_the_same_new_database(api: tuple[str, dict[str, str], str]) -> None:
    barrier = Barrier(8)

    def run_query() -> requests.Response:
        barrier.wait(timeout=10)
        return submit(api, "select 1")

    with ThreadPoolExecutor(max_workers=8) as executor:
        responses = list(executor.map(lambda _: run_query(), range(8)))
    assert all(response.status_code == 200 for response in responses), [response.text for response in responses]


def test_sql_api_result_set_metadata(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, "select 1 as id, 12.50::number(12,2) as total")
    assert response.status_code == 200
    result = response.json()
    assert result["code"] == "090001"
    assert result["resultSetMetaData"]["format"] == "jsonv2"
    assert result["resultSetMetaData"]["numRows"] == 1
    columns = result["resultSetMetaData"]["rowType"]
    assert [(c["name"], c["type"], c["scale"]) for c in columns] == [("ID", "fixed", 0), ("TOTAL", "fixed", 2)]


def test_sql_api_statement_status_url(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, "select 1")
    assert response.status_code == 200, response.text
    result = response.json()
    status_url = urlsplit(result["statementStatusUrl"])
    assert status_url.path == f"/api/v2/statements/{result['statementHandle']}"
    assert parse_qs(status_url.query)["requestId"] == [result["requestId"]]
    uuid.UUID(result["requestId"])

    url, headers, _ = api
    status = requests.get(urljoin(url, result["statementStatusUrl"]), headers=headers, timeout=30)
    assert status.status_code == 200, status.text
    assert status.json()["statementHandle"] == result["statementHandle"]


def test_sql_api_positional_bindings_preserve_null_position(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(
        api,
        "select ? as first, ? as second, ? as third",
        bindings={
            "3": {"type": "TEXT", "value": "third"},
            "2": {"type": "TEXT", "value": None},
            "1": {"type": "TEXT", "value": "first"},
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"] == [["first", None, "third"]]


def test_sql_api_rejects_missing_binding_position(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, "select ?", bindings={"2": {"type": "TEXT", "value": "x"}})
    assert response.status_code == 400


@pytest.mark.parametrize(
    ("expression", "type_name", "value"),
    [
        ("true", "boolean", "true"),
        ("null::varchar", "text", None),
        ("1.25::float", "real", "1.25"),
        ("'2021-01-02'::date", "date", "18629"),
        ("'01:02:03.123456'::time", "time", "3723.123456000"),
        ("'2021-01-02 03:04:05.123456'::timestamp_ntz", "timestamp_ntz", "1609556645.123456000"),
        ("to_binary('ABCD', 'HEX')", "binary", "ABCD"),
    ],
)
def test_sql_api_jsonv2_values(
    api: tuple[str, dict[str, str], str], expression: str, type_name: str, value: str | None
) -> None:
    response = submit(api, f"select {expression} as value")
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["data"] == [[value]]
    assert result["resultSetMetaData"]["rowType"][0]["type"] == type_name


def test_sql_api_variant_result_contains_json(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, 'select parse_json(\'{"a": [1, null], "b": true}\') as value')
    assert response.status_code == 200, response.text
    result = response.json()
    assert result["resultSetMetaData"]["rowType"][0]["type"] == "variant"
    assert json.loads(result["data"][0][0]) == {"a": [1, None], "b": True}


def test_sql_api_nullable_false_returns_null_as_text(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    response = requests.post(
        f"{url}?nullable=false",
        headers=headers,
        json={"statement": "select null as value", "database": database, "schema": "PUBLIC"},
        timeout=30,
    )
    assert response.status_code == 200, response.text
    assert response.json()["data"] == [["null"]]


def test_sql_api_reports_sql_errors(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, "select * from no_such_table")
    assert response.status_code == 422
    error = response.json()
    assert error["code"]
    assert error["sqlState"]
    assert error["statementHandle"]


def test_sql_api_accepts_jsonv2_format_request(api: tuple[str, dict[str, str], str]) -> None:
    response = submit(api, "select 1", resultSetMetaData={"format": "jsonv2"})
    assert response.status_code == 200
    assert response.json()["data"] == [["1"]]


def test_sql_api_rejects_non_json_content_type(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, _ = api
    response = requests.post(url, headers=headers | {"Content-Type": "text/plain"}, data="select 1", timeout=30)
    assert response.status_code == 415


def test_sql_api_requires_bearer_auth(api: tuple[str, dict[str, str], str]) -> None:
    url, _, _ = api
    response = requests.post(url, json={"statement": "select 1"}, timeout=30)
    assert response.status_code == 401
    assert response.json()["code"] == "390146"
    assert response.json()["message"] == "Bearer token is missing in the HTTP request authorization header."


def test_sql_api_async_poll_returns_query_result(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    response = requests.post(
        f"{url}?async=true",
        headers=headers,
        json={"statement": "select 42 as value", "database": database, "schema": "PUBLIC"},
        timeout=30,
    )
    assert response.status_code == 202
    assert response.json()["code"] == "333334"
    handle = response.json()["statementHandle"]
    deadline = time.monotonic() + 10
    while True:
        result = requests.get(f"{url}/{handle}", headers=headers, timeout=30)
        if result.status_code != 202 or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert result.status_code == 200, result.text
    assert result.json()["data"] == [["42"]]
    assert result.json()["statementHandle"] == handle


def test_sql_api_async_failure_is_reported_by_poll(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    response = requests.post(
        f"{url}?async=true",
        headers=headers,
        json={"statement": "select * from no_such_table", "database": database, "schema": "PUBLIC"},
        timeout=30,
    )
    assert response.status_code == 202
    handle = response.json()["statementHandle"]
    deadline = time.monotonic() + 10
    while True:
        result = requests.get(f"{url}/{handle}", headers=headers, timeout=30)
        if result.status_code != 202 or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert result.status_code == 422, result.text
    assert result.json()["statementHandle"] == handle
    assert result.json()["sqlState"]


def test_sql_api_cancels_running_statement(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    sql = (
        "select sum(a.n::bigint * b.n::bigint) from "
        "(select seq4() as n from table(generator(rowcount => 100000))) a cross join "
        "(select seq4() as n from table(generator(rowcount => 100000))) b"
    )
    response = requests.post(
        f"{url}?async=true",
        headers=headers,
        json={"statement": sql, "database": database, "schema": "PUBLIC"},
        timeout=30,
    )
    assert response.status_code == 202
    handle = response.json()["statementHandle"]
    time.sleep(0.2)
    assert requests.get(f"{url}/{handle}", headers=headers, timeout=30).status_code == 202
    assert requests.post(f"{url}/{handle}/cancel", headers=headers, timeout=30).status_code == 200
    deadline = time.monotonic() + 10
    while True:
        result = requests.get(f"{url}/{handle}", headers=headers, timeout=30)
        if result.status_code != 202 or time.monotonic() >= deadline:
            break
        time.sleep(0.01)
    assert result.status_code == 422, result.text
    assert result.json()["sqlState"] == "57014"


def test_sql_api_statement_timeout_interrupts_query(api: tuple[str, dict[str, str], str]) -> None:
    sql = (
        "select sum(a.n::bigint * b.n::bigint) from "
        "(select seq4() as n from table(generator(rowcount => 100000))) a cross join "
        "(select seq4() as n from table(generator(rowcount => 100000))) b"
    )
    response = submit(api, sql, timeout=1)
    assert response.status_code == 408, response.text
    assert response.json()["statementHandle"]


def test_sql_api_partition_manifest_fetches_all_rows(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, _ = api
    response = submit(api, "select seq4() as id from table(generator(rowcount => 10050))")
    assert response.status_code == 200, response.text
    result = response.json()
    partitions = result["resultSetMetaData"]["partitionInfo"]
    assert sum(part["rowCount"] for part in partitions) == 10050
    rows = result["data"]
    for i in range(1, len(partitions)):
        part = requests.get(f"{url}/{result['statementHandle']}?partition={i}", headers=headers, timeout=30)
        assert part.status_code == 200, part.text
        rows.extend(part.json()["data"])
    assert len(rows) == 10050
    assert {int(row[0]) for row in rows} == set(range(10050))


def test_sql_api_rejects_out_of_range_partition(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, _ = api
    response = submit(api, "select 1")
    assert response.status_code == 200
    result = response.json()
    partition = len(result["resultSetMetaData"]["partitionInfo"])
    response = requests.get(f"{url}/{result['statementHandle']}?partition={partition}", headers=headers, timeout=30)
    assert response.status_code in (400, 404, 422)


def test_sql_api_retry_after_failed_statement_can_succeed(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    request_id = str(uuid.uuid4())
    body = {"statement": "select count(*) as n from retry_after_error", "database": database, "schema": "PUBLIC"}
    first = requests.post(f"{url}?requestId={request_id}", headers=headers, json=body, timeout=30)
    assert first.status_code == 422
    assert submit(api, "create table retry_after_error (id integer)").status_code == 200
    second = requests.post(f"{url}?requestId={request_id}&retry=true", headers=headers, json=body, timeout=30)
    assert second.status_code == 200, second.text
    assert second.json()["data"] == [["0"]]


def test_sql_api_retry_does_not_repeat_successful_statement(api: tuple[str, dict[str, str], str]) -> None:
    url, headers, database = api
    assert submit(api, "create table retry_test (id integer)").status_code == 200
    body = {"statement": "insert into retry_test values (1)", "database": database, "schema": "PUBLIC"}
    request_id = str(uuid.uuid4())
    first = requests.post(f"{url}?requestId={request_id}", headers=headers, json=body, timeout=30)
    second = requests.post(f"{url}?requestId={request_id}&retry=true", headers=headers, json=body, timeout=30)
    assert first.status_code == second.status_code == 200
    assert submit(api, "select count(*) as n from retry_test").json()["data"] == [["1"]]

# fakesnow ❄️

[![ci](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml)
[![release](https://github.com/tekumara/fakesnow/actions/workflows/release.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/fakesnow?color=violet)](https://pypi.org/project/fakesnow/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/fakesnow?color=violet)](https://pypi.org/project/fakesnow/)

Run, mock and test fake Snowflake databases locally.

## Install

```
pip install fakesnow
```

Or to install with the server:

```
pip install fakesnow[server]
```

## Usage

fakesnow offers two main approaches for faking Snowflake: [in-process patching](#in-process-patching) of the Snowflake Connector for Python or a [standalone HTTP server](#run-fakesnow-as-a-server).

Patching only applies to the current Python process. If a subprocess is spawned it won't be patched. For subprocesses, or for non-Python clients, use the server instead.

### In-process patching

To run script.py with patching:

```shell
fakesnow script.py
```

Or a module, eg: pytest

```shell
fakesnow -m pytest
```

`fakesnow` executes `fakesnow.patch` before running the script or module.

#### Use fakesnow.patch in your code

Alternatively, use fakesnow.patch in your code:

```python
import fakesnow
import snowflake.connector

with fakesnow.patch():
    conn = snowflake.connector.connect()

    print(conn.cursor().execute("SELECT 'Hello fake world!'").fetchone())
```

#### What gets patched

The following standard imports are automatically patched:

- `import snowflake.connector.connect`
- `import snowflake.connector.pandas_tools.write_pandas`

#### Handling "from ... import" Statements

To patch modules that use the `from ... import` syntax, you need to manually specify them, eg: if _mymodule.py_ contains:

```python
from snowflake.connector.pandas_tools import write_pandas
```

Then patch it using:

```python
with fakesnow.patch("mymodule.write_pandas"):
    ...
```

#### Database Persistence

By default, databases are in-memory and will be lost when the process ends. To persist databases between processes, specify a databases path:

```python
with fakesnow.patch(db_path="databases/"):
    ...
```

### Run fakesnow as a server

For scenarios where patching won't work (like subprocesses or non-Python clients), you can run fakesnow as an HTTP server.

#### From the command line

Using [uv](https://docs.astral.sh/uv/):

```
uvx 'fakesnow[server]' -s
```

Or from within a virtualenv that has `fakesnow[server]` installed:

```
fakesnow -s
```

By default the server listens on a random available port. Use `-p` to specify a port.

#### Within your python program

```python
import fakesnow
import snowflake.connector

# Start the fakesnow server in a context manager
# This yields connection kwargs (host, port, etc.)
with fakesnow.server() as conn_kwargs:
    # Connect to the fakesnow server using the yielded kwargs
    with snowflake.connector.connect(**conn_kwargs) as conn:
        print(conn.cursor().execute("SELECT 'Hello fake server!'").fetchone())

    # The server is automatically stopped when exiting the context manager
```

This starts an HTTP server in its own thread listening for requests on a random available port.

##### Server Configuration Options

To specify a port for the server:

```python
with fakesnow.server(port=12345) as conn_kwargs:
    ...
```

By default, the server uses a single in-memory database for its lifetime. To configure database persistence or isolation:

```python
# Databases will be saved to the "databases/" directory
with fakesnow.server(session_parameters={"FAKESNOW_DB_PATH": "databases/"}):
    ...

# Each connection gets its own isolated in-memory database
with fakesnow.server(session_parameters={"FAKESNOW_DB_PATH": ":isolated:"}):
    ...
```

#### Connecting from non-Python clients

The server is available via HTTP and accepts any username/password/account combination, eg:

```
user: fake
password: snow
account: fakesnow
host: 127.0.0.1
port: <port number from server startup>
protocol: http
```

Additional parameters that may be helpful:
- Session parameter `CLIENT_OUT_OF_BAND_TELEMETRY_ENABLED` set to `false`
- Network timeout set to 1 second (since retries aren't needed in testing)

For example, with the Snowflake CLI:

```
snowsql -a fakesnow -u fake -p snow -h 127.0.0.1 -P <port> --protocol http
```

### pytest fixtures

fakesnow provides [fixtures](fakesnow/fixtures.py) for easier test integration. Add them in _conftest.py_:

```python
pytest_plugins = "fakesnow.fixtures"
```

To autouse the fixture you can wrap it like this in _conftest.py_:

```python
from typing import Iterator

import pytest

pytest_plugins = "fakesnow.fixtures"

@pytest.fixture(scope="session", autouse=True)
def setup(_fakesnow_session: None) -> Iterator[None]:
    # the standard imports are now patched
    # Add any additional setup here
    yield
    # Add any teardown here
```

For code that uses `from ... import` statements:

```python
from typing import Iterator

import fakesnow
import pytest

pytest_plugins = "fakesnow.fixtures"

@pytest.fixture(scope="session", autouse=True)
def _fakesnow_session() -> Iterator[None]:
    with fakesnow.patch("mymodule.write_pandas"):
        yield
```

#### server fixture

To start a fakesnow server instance, enable the plugin in _conftest.py_:

```python
pytest_plugins = "fakesnow.fixtures"
```

And then use the `fakesnow_server` session fixture like this:

```python
import snowflake.connector

def test_with_server(fakesnow_server: dict):
    # fakesnow_server contains connection kwargs (host, port, etc.)
    with snowflake.connector.connect(**fakesnow_server) as conn:
        conn.cursor().execute("SELECT 1")
        assert conn.cursor().fetchone() == (1,)
```

## Implementation coverage

Fully supported:

- Standard SQL operations and cursors
- Information schema queries
- Multiple databases
- [Parameter binding](https://docs.snowflake.com/en/user-guide/python-connector-example#binding-data) in queries
- Table comments
- Pandas integration including [write_pandas(..)](https://docs.snowflake.com/en/user-guide/python-connector-api#write_pandas)
- Result batch retrieval via [get_result_batches()](https://docs.snowflake.com/en/user-guide/python-connector-api#get_result_batches)
- HTTP server for non-Python connectors

Partially supported:

- Date functions
- Regular expression functions
- Semi-structured data operations
- Tags
- User management
- Stages and PUT
- `COPY INTO` from S3 sources and stages, see [COPY INTO](#copy-into)

Not yet implemented:

- [Access control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [Stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures)

For more detail see the [test suite](tests/).

## Caveats

- Row ordering is non-deterministic and may differ from Snowflake unless you fully specify the ORDER BY clause.
- fakesnow supports a more liberal SQL dialect than actual Snowflake. This means some queries that work with fakesnow might not work with a real Snowflake instance.

## COPY INTO

`COPY INTO` can be used from S3 sources and stages. By default the standard AWS credential chain will be used. If you are getting an HTTP 403 or need to provide alternative S3 credentials you can use the duckdb [CREATE SECRET](https://duckdb.org/docs/stable/extensions/httpfs/s3api) statement. For an example of creating a secret to use a moto S3 endpoint see `s3_client` in [conftest.py](tests/conftest.py#L80)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for instructions on getting started with development and contributing to this project.

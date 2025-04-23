# fakesnow ❄️

[![ci](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml)
[![release](https://github.com/tekumara/fakesnow/actions/workflows/release.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/fakesnow?color=violet)](https://pypi.org/project/fakesnow/)
[![PyPI - Downloads](https://img.shields.io/pypi/dm/fakesnow?color=violet)](https://pypi.org/project/fakesnow/)

Fake [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector). Run and mock Snowflake DB locally.

## Install

```
pip install fakesnow
```

## Usage

Run script.py with fakesnow:

```shell
fakesnow script.py
```

Or a module, eg: pytest

```shell
fakesnow -m pytest
```

`fakesnow` executes `fakesnow.patch` before running the script or module.

### fakesnow.patch

To use fakesnow within your code:

```python
import fakesnow
import snowflake.connector

with fakesnow.patch():
    conn = snowflake.connector.connect()

    print(conn.cursor().execute("SELECT 'Hello fake world!'").fetchone())
```

The following standard imports are automatically patched:

- `import snowflake.connector.connect`
- `import snowflake.connector.pandas_tools.write_pandas`

To patch modules that use the `from ... import` syntax, manually specify them, eg: if _mymodule.py_ has the import:

```python
from snowflake.connector.pandas_tools import write_pandas
```

Then patch it using:

```python
with fakesnow.patch("mymodule.write_pandas"):
    ...
```

By default databases are in-memory. To persist databases between processes, specify a databases path:

```python
with fakesnow.patch(db_path="databases/"):
    ...
```

#### Patching not working

Patching only applies to the current process. If a subprocess is spawned it won't be patched. Use the server instead.

### fakesnow.server

To run fakesnow as a separate server thread listening for HTTP requests on localhost:

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

By default, the server uses an isolated in-memory database for its lifetime. This can be configured as follows, eg:

```python
# Use a persistent database path
with fakesnow.server(session_parameters={"FAKESNOW_DB_PATH": "databases/"}):
    # ...

# Use a separate in-memory database per connection
with fakesnow.server(session_parameters={"FAKESNOW_DB_PATH": ":isolated:"}):
    # ...
```

Specify a port for the server, rather than selecting any available port

```python
with fakesnow.server(port=12345) as conn_kwargs:
    # conn_kwargs will include port=12345
    # ...
```

The sever accepts any username/password combination.

### pytest fixtures

pytest [fixtures](fakesnow/fixtures.py) are provided for testing. Example _conftest.py_:

```python
from typing import Iterator

import fakesnow.fixtures
import pytest

pytest_plugins = fakesnow.fixtures.__name__

@pytest.fixture(scope="session", autouse=True)
def setup(_fakesnow_session: None) -> Iterator[None]:
    # the standard imports are now patched
    ...
    yield
```

Or with `from ... import` patch targets:

```python
from typing import Iterator

import fakesnow
import pytest

@pytest.fixture(scope="session", autouse=True)
def _fakesnow_session() -> Iterator[None]:
    with fakesnow.patch("mymodule.write_pandas"):
        yield
```

To test against a fakesnow server instance, use the `fakesnow_server` session fixture. It yields connection parameters:

```python
import snowflake.connector

def test_with_server(fakesnow_server: dict):
    # fakesnow_server contains connection kwargs (host, port, etc.)
    with snowflake.connector.connect(**fakesnow_server) as conn:
        conn.cursor().execute("SELECT 1")
        assert conn.cursor().fetchone() == (1,)
```

## Implementation coverage

- [x] cursors and standard SQL
- [x] [get_result_batches()](https://docs.snowflake.com/en/user-guide/python-connector-api#get_result_batches)
- [x] information schema
- [x] multiple databases
- [x] [parameter binding](https://docs.snowflake.com/en/user-guide/python-connector-example#binding-data)
- [x] table comments
- [x] [write_pandas(..)](https://docs.snowflake.com/en/user-guide/python-connector-api#write_pandas)
- [x] standalone/out of process api/support for faking non-python connectors
- [ ] [access control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [ ] [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures)

Partial support

- [x] date functions
- [x] regex functions
- [x] semi-structured data
- [x] tags
- [x] user management (See [tests/test_users.py](tests/test_users.py))

For more detail see [tests/test_fakes.py](tests/test_fakes.py)

## Caveats

- The order of rows is non deterministic and may not match Snowflake unless ORDER BY is fully specified.
- A more liberal Snowflake SQL dialect than used by a real Snowflake instance is supported, ie: some queries might pass using fakesnow that a real Snowflake instance would reject.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started and develop in this repo.

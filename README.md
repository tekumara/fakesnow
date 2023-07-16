# fakesnow ❄️

[![ci](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/ci.yml)
[![release](https://github.com/tekumara/fakesnow/actions/workflows/release.yml/badge.svg)](https://github.com/tekumara/fakesnow/actions/workflows/release.yml)
[![PyPI](https://img.shields.io/pypi/v/fakesnow?color=violet)](https://pypi.org/project/fakesnow/)

Fake [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector). Run and mock Snowflake DB locally.

## Install

```
pip install fakesnow
```

## Usage

```python
import fakesnow
import snowflake.connector

with fakesnow.patch():
    conn = snowflake.connector.connect()

    print(conn.cursor().execute("SELECT 'Hello fake world!'").fetchone())
```

The following imports are automatically patched:

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

## Implementation coverage

- [x] cursors and standard SQL
- [x] [get_result_batches()](https://docs.snowflake.com/en/user-guide/python-connector-api#get_result_batches)
- [x] information schema
- [x] multiple databases
- [x] [qmark binding](https://docs.snowflake.com/en/user-guide/python-connector-example#binding-data)
- [x] table comments
- [x] [write_pandas(..)](https://docs.snowflake.com/en/user-guide/python-connector-api#write_pandas)
- [ ] [access control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [ ] standalone/out of process api/support for faking non-python connectors
- [ ] [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures)

Partial support

- [x] date functions
- [x] regex functions
- [x] semi-structured data
- [x] tags

For more detail see [tests/test_fakes.py](tests/test_fakes.py)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started and develop in this repo.

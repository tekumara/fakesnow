# fakesnow ❄️

Fake [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector). Run Snowflake DB locally.

## Usage

```python
import fakesnow
import snowflake.connector

with fakesnow.patch():
    conn = snowflake.connector.connect()

    print(conn.cursor().execute("SELECT 'Hello fake world!'").fetchone())
```

Standard imports (eg: `snowflake.connector.connect`) are patched. To patch additional imports, eg:

```python
with fakesnow.patch("mymodule.connect"):
    ...
```

pytest [fixtures](fakesnow/fixtures.py) are provided for testing. Example _conftest.py_:

```python
from typing import Iterator
import pytest

pytest_plugins = ("fakesnow.fixtures",)

@pytest.fixture(scope="session", autouse=True)
def setup(_fake_snow_session: None) -> Iterator[None]:
    # the standard imports are now patched
    ...
    yield
```

## Implementation coverage

- [x] multiple databases
- [x] cursors
- [x] [get_result_batches()](https://docs.snowflake.com/en/user-guide/python-connector-api#get_result_batches)
- [x] [write_pandas(..)](https://docs.snowflake.com/en/user-guide/python-connector-api#write_pandas)
- [x] table comments
- [ ] [access control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [ ] standalone/out of process api/support for faking non-python connectors
- [ ] [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures)

Partial support
- [x] tags
- [x] semi-structured data

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started and develop in this repo.

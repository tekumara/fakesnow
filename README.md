# fakesnow ❄️

Fake [Snowflake Connector for Python](https://docs.snowflake.com/en/user-guide/python-connector). Run Snowflake DB locally.

## Usage

```python
import fakesnow
import snowflake.connector

with fakesnow.mock():
    conn = snowflake.connector.connect()

    print(conn.cursor().execute("SELECT 'Hello fake world!'").fetchone())
```

[Fixtures](fakesnow/fixtures.py) are provided for testing. Example _conftest.py_:

```python
from typing import Iterator
import pytest

pytest_plugins = ("fakesnow.fixtures",)

@pytest.fixture(scope="session", autouse=True)
def _fake_snow_autouse(_fake_snow_session: None) -> Iterator[None]:
    pass
```

## Implementation coverage

- [X] multiple databases
- [X] cursors
- [X] [get_result_batches()](https://docs.snowflake.com/en/user-guide/python-connector-api#get_result_batches)
- [X] [write_pandas(..)](https://docs.snowflake.com/en/user-guide/python-connector-api#write_pandas)
- [ ] [access control](https://docs.snowflake.com/en/user-guide/security-access-control-overview)
- [ ] table comments
- [ ] standalone/out of process api/support for faking non-python connectors
- [ ] [stored procedures](https://docs.snowflake.com/en/sql-reference/stored-procedures)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) to get started and develop in this repo.

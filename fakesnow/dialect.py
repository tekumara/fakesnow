"""Parse Snowflake SQL with Snowflake-compatible syntax errors, and generate DuckDB SQL."""

import duckdb
import snowflake.connector.errors
import sqlglot
from sqlglot import Expr
from sqlglot.dialects.duckdb import DuckDB
from sqlglot.errors import ParseError


def _duckdb_reserved_keywords() -> set[str]:
    """Keywords DuckDB can't take as an unquoted table, column or alias name."""
    with duckdb.connect() as conn:
        rows = conn.execute(
            "SELECT keyword_name FROM duckdb_keywords() WHERE keyword_category IN ('reserved', 'type_function')"
        ).fetchall()
    return {name for (name,) in rows}


class FakesnowDuckDB(DuckDB):
    """DuckDB, quoting every identifier DuckDB reserves.

    Snowflake allows names such as SEMI or PIVOT unquoted but DuckDB doesn't, and sqlglot's list of DuckDB
    reserved keywords misses some, so take them from DuckDB itself. Quoting doesn't change how a name
    resolves, because DuckDB matches quoted identifiers case-insensitively too.
    """

    class Generator(DuckDB.Generator):
        RESERVED_KEYWORDS = DuckDB.Generator.RESERVED_KEYWORDS | _duckdb_reserved_keywords()


def parse(command: str) -> list[Expr | None]:
    try:
        return sqlglot.parse(command, read="snowflake")
    except ParseError as error:
        raise syntax_error(error) from None


def parse_one(command: str) -> Expr:
    try:
        return sqlglot.parse_one(command, read="snowflake")
    except ParseError as error:
        raise syntax_error(error) from None


def syntax_error(error: ParseError) -> snowflake.connector.errors.ProgrammingError:
    # Strip terminal highlighting from SQLGlot's error message.
    message = str(error).replace("\x1b[4m", "").replace("\x1b[0m", "")
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=1003, sqlstate="42000")

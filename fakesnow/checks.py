from __future__ import annotations

import snowflake.connector.errors
from sqlglot import exp

import fakesnow.transforms as transforms


def has_database(expression: exp.Expression) -> None:
    """Checks the expression is qualified with a valid database.

    Args:
        expression (exp.Expression): Expression to check.

    Raises:
        snowflake.connector.errors.ProgrammingError if there's no database.
    """

    if transforms.MISSING_DATABASE in expression.sql():
        kind = expression.args.get("kind", None)
        if isinstance(kind, str) and kind.upper() == "SCHEMA":
            # "DROP/CREATE SCHEMA"
            op = f"{expression.key.upper()} SCHEMA"
        elif isinstance(kind, exp.Var) and kind.name.upper() == "SCHEMA":
            # "USE SCHEMA" - missing database caught during execution
            return
        else:
            op = "SELECT"

        raise snowflake.connector.errors.ProgrammingError(
            msg=f"Cannot perform {op}. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",  # noqa: E501
            errno=90105,
            sqlstate="22000",
        )

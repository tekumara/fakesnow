from __future__ import annotations

from sqlglot import exp

import fakesnow.transforms as transforms


def command_missing_database(expression: exp.Expression) -> str | None:
    """Returns the command if it is missing a database.

    A missing database means the command is not qualified with a database and there is no current database.

    Args:
        expression (exp.Expression): Expression to check.

    Returns:
        str | None: Command, eg: "CREATE SCHEMA", "SELECT" if missing a database, otherwise None.
    """

    if transforms.MISSING_DATABASE not in expression.sql():
        return None

    kind = expression.args.get("kind", None)
    if isinstance(kind, str) and kind.upper() == "SCHEMA":
        # "DROP/CREATE SCHEMA"
        op = f"{expression.key.upper()} SCHEMA"
    elif isinstance(kind, exp.Var) and kind.name.upper() == "SCHEMA":
        # "USE SCHEMA" - missing database caught during execution
        return None
    else:
        op = "SELECT"
    return op

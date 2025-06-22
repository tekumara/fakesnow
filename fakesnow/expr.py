from __future__ import annotations

from sqlglot import exp


def key_command(expression: exp.Expression) -> str:
    """Identifies the key SQL command in an expression.

    Useful for conditional logic that acts on specific commands.

    Args:
        expression (exp.Expression): Expression to check.

    Returns:
        str: Command, eg: "CREATE SCHEMA", "SELECT", "SET" etc.
    """

    kind = expression.args.get("kind")

    if isinstance(kind, str):
        # eg: "DROP/CREATE SCHEMA"
        key = f"{expression.key.upper()} {kind.upper()}"
    elif isinstance(kind, exp.Var):
        # eg: "USE DATABASE/SCHEMA"
        key = f"{expression.key.upper()} {kind.name.upper()}"
    elif isinstance(expression, exp.Command) and isinstance(expression.this, str):
        # eg: "SET"
        key = expression.this.upper()
    else:
        key = expression.key.upper()

    return key


def normalise_ident(name: str) -> str:
    """
    Strip double quotes if present else return uppercased.
    Snowflake treats quoted identifiers as case-sensitive and un-quoted identifiers as case-insensitive
    """
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1]  # Strip quotes

    return name.upper()

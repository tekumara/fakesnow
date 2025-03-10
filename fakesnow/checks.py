from __future__ import annotations

from sqlglot import exp


def is_unqualified_table_expression(expression: exp.Expression) -> tuple[bool, bool]:
    """Checks if the table expression is unqualified, eg: no database or schema.

    NB: sqlglot treats the identifier in "CREATE SCHEMA schema1" as a table expression.

    Example:
        >>> import sqlglot
        >>> is_unqualified_table_expression("SELECT * FROM customers")
        (True, True)
        >>> is_unqualified_table_expression("CREATE SCHEMA schema1")
        (True, False)
        >>> is_unqualified_table_expression("USE DATABASE db1")
        (False, False)

        See tests for more examples.
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if not (node := expression.find(exp.Table)):
        return False, False

    assert node.parent, f"No parent for table expression {node.sql()}"

    if (parent_kind := node.parent.args.get("kind")) and isinstance(parent_kind, str):
        if parent_kind.upper() == "DATABASE":
            # "CREATE/DROP DATABASE"
            no_database = False
            no_schema = False
        elif parent_kind.upper() == "SCHEMA":
            # "CREATE/DROP SCHEMA"
            no_database = not node.args.get("catalog")
            no_schema = False
        elif parent_kind.upper() in {"TABLE", "VIEW"}:
            # "CREATE/DROP TABLE/VIEW"
            no_database = not node.args.get("catalog")
            no_schema = not node.args.get("db")
        else:
            raise AssertionError(f"Unexpected parent kind: {parent_kind}")

    elif (
        node.parent.key == "use"
        and (parent_kind := node.parent.args.get("kind"))
        and isinstance(parent_kind, exp.Var)
        and parent_kind.name
    ):
        if parent_kind.name.upper() == "DATABASE":
            # "USE DATABASE"
            no_database = False
            no_schema = False
        elif parent_kind.name.upper() == "SCHEMA":
            # "USE SCHEMA"
            no_database = not node.args.get("db")
            no_schema = False
        else:
            raise AssertionError(f"Unexpected parent kind: {parent_kind.name}")

    elif node.parent.key == "show":
        # don't require a database or schema for SHOW
        # TODO: make this more nuanced
        no_database = False
        no_schema = False
    else:
        no_database = not node.args.get("catalog")
        no_schema = not node.args.get("db")

    return no_database, no_schema


def equal(left: exp.Identifier, right: exp.Identifier) -> bool:
    # as per https://docs.snowflake.com/en/sql-reference/identifiers-syntax#label-identifier-casing
    lid = left.this if left.quoted else left.this.upper()
    rid = right.this if right.quoted else right.this.upper()

    return lid == rid

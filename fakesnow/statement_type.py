from __future__ import annotations

from sqlglot import Expr, exp

# Statement type ids returned by Snowflake as statementTypeId.
# Clients use them to tell whether a statement produces a result set, eg: the JDBC driver
# rejects executeUpdate for statements it can't identify, and the python connector uses
# them to decide how to count affected rows.
# See net.snowflake.client.core.SFStatementType in snowflake-jdbc, and
# STATEMENT_TYPE_ID_DML_SET in snowflake-connector-python.
UNKNOWN = 0x0000
SELECT = 0x1000
DML = 0x3000
INSERT = DML + 0x100
UPDATE = DML + 0x200
DELETE = DML + 0x300
MERGE = DML + 0x400
MULTI_INSERT = DML + 0x500
COPY = DML + 0x600
SCL = 0x4000
USE = SCL + 0x300
USE_DATABASE = USE + 0x01
USE_SCHEMA = USE + 0x02
USE_WAREHOUSE = USE + 0x03
SHOW = SCL + 0x400
DESCRIBE = SCL + 0x500
TCL = 0x5000
DDL = 0x6000

# Statements the clients treat as DML, ie: they report affected rows rather than a result set.
DML_TYPE_IDS = frozenset({DML, INSERT, UPDATE, DELETE, MERGE, MULTI_INSERT})

_EXPRESSION_TYPES: dict[type[Expr], int] = {
    exp.Select: SELECT,
    exp.Insert: INSERT,
    exp.MultitableInserts: MULTI_INSERT,
    exp.Update: UPDATE,
    exp.Delete: DELETE,
    exp.Merge: MERGE,
    exp.Copy: COPY,
    exp.Show: SHOW,
    exp.Describe: DESCRIBE,
    exp.Create: DDL,
    exp.Drop: DDL,
    exp.Alter: DDL,
    exp.TruncateTable: DDL,
    exp.Commit: TCL,
    exp.Rollback: TCL,
    exp.Transaction: TCL,
}

_USE_KINDS = {
    "DATABASE": USE_DATABASE,
    "SCHEMA": USE_SCHEMA,
    "WAREHOUSE": USE_WAREHOUSE,
}


def statement_type_id(expression: Expr) -> int:
    """The Snowflake statement type id for an expression.

    Returns UNKNOWN for statements we don't recognise, which is what Snowflake returns
    for statements that produce a result set.

    Args:
        expression (Expr): Expression to check.

    Returns:
        int: Statement type id, eg: 12544 (INSERT).
    """

    if isinstance(expression, exp.Use):
        kind = expression.args.get("kind")
        return _USE_KINDS.get(kind.name.upper(), USE) if isinstance(kind, exp.Var) else USE

    return _EXPRESSION_TYPES.get(type(expression), UNKNOWN)

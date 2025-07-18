"""DDL (Data Definition Language) transforms for fakesnow.

This module contains transformations for DDL statements like ALTER TABLE, CREATE TABLE, etc.

Future DDL transforms can be added here:
- CREATE TABLE enhancements
- DROP TABLE transformations
- ALTER TABLE modifications (beyond ADD COLUMN)
- INDEX operations
- VIEW operations
- etc.
"""

from __future__ import annotations

from sqlglot import exp


def alter_table_add_multiple_columns(expression: exp.Expression) -> list[exp.Expression]:
    """Transform ALTER TABLE ADD COLUMN with multiple columns into separate statements.

    Snowflake supports: ALTER TABLE tab1 ADD COLUMN col1 INT, col2 VARCHAR(50), col3 BOOLEAN;
    DuckDB requires separate statements for each column.

    Args:
        expression: The expression to potentially transform

    Returns:
        List of expressions - multiple ALTER statements if transformation applied,
        otherwise single original expression
    """
    if not isinstance(expression, exp.Alter):
        return [expression]

    actions = expression.args.get("actions")
    if not (
        actions
        and isinstance(actions, list)
        and all(isinstance(action, exp.ColumnDef) for action in actions)
        and len(actions) > 1
    ):
        return [expression]

    # Create separate ALTER statements for each column
    alter_statements = []
    for action in actions:
        new_alter = exp.Alter(
            this=expression.this,
            kind=expression.args.get("kind"),
            exists=expression.args.get("exists"),
            actions=[action],
        )
        alter_statements.append(new_alter)

    return alter_statements

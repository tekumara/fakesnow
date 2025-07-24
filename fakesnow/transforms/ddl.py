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

from fakesnow.transforms.transforms import SUCCESS_NOP


def alter_table_add_multiple_columns(expression: exp.Expression) -> list[exp.Expression]:
    """Transform ALTER TABLE ADD COLUMN with multiple columns into separate statements.

    Snowflake supports: ALTER TABLE IF EXISTS tab1 ADD COLUMN IF NOT EXISTS col1 INT, col2 VARCHAR(50), col3 BOOLEAN;
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
    # Check if any column has IF NOT EXISTS - in Snowflake, this applies to all columns
    column_if_not_exists = any(action.args.get("exists", False) for action in actions)

    for action in actions:
        # If any column has IF NOT EXISTS, apply it to all columns
        if column_if_not_exists and not action.args.get("exists", False):
            # Create a new ColumnDef with exists=True
            new_action = exp.ColumnDef(
                this=action.this,
                kind=action.kind,
                constraints=action.args.get("constraints", []),
                exists=True,
            )
        else:
            new_action = action

        new_alter = exp.Alter(
            this=expression.this,
            kind=expression.args.get("kind"),
            exists=expression.args.get("exists"),
            actions=[new_action],
        )
        alter_statements.append(new_alter)

    return alter_statements


def alter_table_strip_cluster_by(expression: exp.Expression) -> exp.Expression:
    """Turn alter table cluster by into a no-op"""
    if (
        isinstance(expression, exp.Alter)
        and (actions := expression.args.get("actions"))
        and len(actions) == 1
        and (isinstance(actions[0], exp.Cluster))
    ):
        return SUCCESS_NOP
    return expression

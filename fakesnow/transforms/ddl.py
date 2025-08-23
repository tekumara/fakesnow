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

import secrets

import sqlglot
from sqlglot import exp

from fakesnow.transforms.transforms import SUCCESS_NOP


def alter_table_add_multiple_columns(
    expression: exp.Expression,
) -> list[exp.Expression]:
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


def create_table_autoincrement(
    expression: exp.Expression,
) -> list[exp.Expression]:
    """Split CREATE TABLE with AUTOINCREMENT into CREATE SEQUENCE + CREATE TABLE with DEFAULT NEXTVAL.

    Example transform:
        CREATE TABLE test_table (id NUMERIC NOT NULL AUTOINCREMENT, name VARCHAR)
        ->
        CREATE SEQUENCE test_table_id_seq START 1;
        CREATE TABLE test_table (id NUMERIC NOT NULL DEFAULT NEXTVAL('test_table_id_seq'), name VARCHAR)
    """

    if not (
        isinstance(expression, exp.Create)
        and expression.kind == "TABLE"
        and (schema := expression.this)
        and (table := schema.this)
        and isinstance(schema, exp.Schema)
        and isinstance(table, exp.Table)
        # Find AUTOINCREMENT/IDENTITY columns
        and (
            auto_cols := [
                cd
                for cd in (schema.expressions or [])
                if isinstance(cd, exp.ColumnDef)
                and (cd.find(exp.AutoIncrementColumnConstraint) or cd.find(exp.GeneratedAsIdentityColumnConstraint))
            ]
        )
    ):
        return [expression]

    if len(auto_cols) > 1:
        raise NotImplementedError("Multiple AUTOINCREMENT columns")

    auto = auto_cols[0]
    col_name = auto.this.name
    table_name = table.name
    # When recreating the same table with a sequence, we need to give the sequence a unique name to avoid
    # Dependency Error: Cannot drop entry "_FS_SEQ_..." because there are entries that depend on it.
    random_suffix = secrets.token_hex(4)
    seq_name = f"_fs_seq_{table_name}_{col_name}_{random_suffix}"

    # Build CREATE SEQUENCE, using START/INCREMENT if provided
    start_val = "1"
    increment_val = "1"

    identity = auto.find(exp.GeneratedAsIdentityColumnConstraint)
    if identity:
        s = identity.args.get("start")
        i = identity.args.get("increment")
        if isinstance(s, exp.Literal):
            start_val = s.this
        if isinstance(i, exp.Literal):
            increment_val = i.this

    seq_stmt = sqlglot.parse_one(
        f"CREATE SEQUENCE {seq_name} START WITH {start_val} INCREMENT BY {increment_val}",
        read="duckdb",
    )

    # Build modified CREATE TABLE with DEFAULT NEXTVAL('<seq_name>') and without AUTOINCREMENT
    new_create: exp.Create = expression.copy()
    new_schema: exp.Schema = new_create.this

    # Find the corresponding column in the copied schema
    target_col = next(
        cd for cd in (new_schema.expressions or []) if isinstance(cd, exp.ColumnDef) and cd.this.name == col_name
    )

    existing_constraints: list[exp.Expression] = target_col.args.get("constraints", []) or []

    # Replace the AUTOINCREMENT/IDENTITY constraint in-place with DEFAULT NEXTVAL('<seq_name>')
    for c in existing_constraints:
        if isinstance(c, exp.ColumnConstraint) and isinstance(
            c.args.get("kind"),
            (
                exp.AutoIncrementColumnConstraint,
                exp.GeneratedAsIdentityColumnConstraint,
            ),
        ):
            c.set(
                "kind",
                exp.DefaultColumnConstraint(
                    this=exp.Anonymous(
                        this="NEXTVAL",
                        expressions=[exp.Literal(this=seq_name, is_string=True)],
                    )
                ),
            )
            break

    target_col.set("constraints", existing_constraints)

    return [seq_stmt, new_create]

from __future__ import annotations

import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp
from sqlglot.optimizer.scope import traverse_scope

from fakesnow.transforms.stage import TableStages


def temporary_tables(
    expression: Expr,
    duck_conn: DuckDBPyConnection,
    current_database: str | None,
    current_schema: str | None,
    table_stages: TableStages,
) -> Expr:
    """Resolve the Snowflake names of temporary tables to DuckDB's temp catalog.

    A Snowflake temporary table belongs to the schema it was created in. DuckDB keeps every temporary table in
    temp.main and only accepts that catalog when creating one, so temporary tables are rewritten to temp.main
    where they're created or referenced in their own schema.
    """
    if expression.parent is not None:
        return expression

    owners = table_stages.temporary_owners()
    target = _create_table_target(expression)

    # a table created directly in DuckDB's temp catalog, like MERGE's scratch table, has no Snowflake schema
    if target and target.catalog != "temp" and expression.find(exp.TemporaryProperty):
        owner = (target.catalog or current_database, target.db or current_schema)
        # DuckDB holds one temporary table per name, ignoring case, so only the same table can be replaced
        existing = next(((n, o) for n, o in owners.items() if n.casefold() == target.name.casefold()), None)
        if existing and existing != (target.name, owner):
            name, (catalog, schema) = existing
            raise snowflake.connector.errors.ProgrammingError(
                msg=(
                    f"SQL compilation error:\nfakesnow supports one temporary table named {target.name} per "
                    f"session, ignoring case, and {name} already exists in {catalog}.{schema}."
                ),
                errno=2002,
                sqlstate="42710",
            )
        sql = "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?"
        if not duck_conn.execute(sql, owner).fetchone():
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error:\nSchema '{owner[0]}.{owner[1]}' does not exist or not authorized.",
                errno=2003,
                sqlstate="02000",
            )
        _to_temp(target)
        # the cursor records this as the table's schema once the CREATE succeeds
        expression.args["temporary_owner"] = owner

    if not owners:
        return expression

    for table in _referenced_tables(expression, target):
        if (owner := owners.get(table.name)) and (
            table.catalog or current_database,
            table.db or current_schema,
        ) == owner:
            _to_temp(table)

    return expression


def _create_table_target(expression: Expr) -> exp.Table | None:
    if isinstance(expression, exp.Create) and expression.args.get("kind") == "TABLE":
        this = expression.this
        table = this.this if isinstance(this, exp.Schema) else this
        if isinstance(table, exp.Table):
            return table
    return None


def _referenced_tables(expression: Expr, create_target: exp.Table | None) -> list[exp.Table]:
    """Tables a statement reads or writes, other than the one it creates."""
    tables = [
        source
        for scope in traverse_scope(expression)
        for source in scope.sources.values()
        if isinstance(source, exp.Table) and isinstance(source.this, exp.Identifier)
    ]

    # DESCRIBE's kind is lower case, DROP keeps its tables in a list
    is_table_statement = str(expression.args.get("kind")).upper() == "TABLE"
    if isinstance(expression, (exp.Insert, exp.Copy, exp.Update, exp.Delete)) or (
        isinstance(expression, exp.Describe) and is_table_statement
    ):
        this = expression.this
        table = this.this if isinstance(this, exp.Schema) else this
        if isinstance(table, exp.Table):
            tables.append(table)
    elif isinstance(expression, exp.Drop) and is_table_statement:
        tables.extend(t for t in expression.args.get("tables") or [] if isinstance(t, exp.Table))

    # the same name can be referenced more than once, and each reference is rewritten
    unique = {id(table): table for table in tables if table is not create_target}
    return list(unique.values())


def _to_temp(table: exp.Table) -> None:
    table.set("catalog", exp.to_identifier("temp"))
    table.set("db", exp.to_identifier("main"))

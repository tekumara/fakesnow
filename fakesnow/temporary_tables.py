from __future__ import annotations

import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp


class TemporaryTables:
    """Track Snowflake schema ownership for DuckDB's session-local temporary tables.

    Table OIDs survive renames and are never reused. Lookups return only live OIDs; registration prunes dropped
    tables so session history stays bounded. Both SQL references and table stages use this registry.
    """

    def __init__(self, duck_conn: DuckDBPyConnection):
        self._duck_conn = duck_conn
        self._temporary: dict[int, tuple[str | None, str | None]] = {}

    def transform(self, expression: Expr, current_database: str | None, current_schema: str | None) -> Expr:
        """Create temporary tables in DuckDB's temp catalog and resolve their qualified references.

        Snowflake temporary tables belong to a database and schema, whereas DuckDB keeps them all in temp.main.
        See https://docs.snowflake.com/en/user-guide/tables-temp-transient
        """
        if (
            isinstance(expression, exp.Create)
            and expression.kind == "TABLE"
            and expression.find(exp.TemporaryProperty)
            and (table := expression.find(exp.Table))
        ):
            catalog, schema = table.catalog or current_database, table.db or current_schema
            # Removing the qualifiers bypasses DuckDB's schema validation, so check the original owner first.
            sql = "SELECT 1 FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?"
            if not self._duck_conn.execute(sql, [catalog, schema]).fetchone():
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nSchema '{catalog}.{schema}' does not exist or not authorized.",
                    errno=2003,
                    sqlstate="42S02",
                )
            expression.set("temporary_table_owner", (catalog, schema))
            table.set("catalog", None)
            table.set("db", None)
        elif isinstance(expression, (exp.Table, exp.Column)) and expression.db:
            parent = expression.parent
            # A permanent CREATE target can share a temporary table's name.
            if isinstance(parent, exp.Create) or (
                isinstance(parent, exp.Schema) and isinstance(parent.parent, exp.Create)
            ):
                return expression
            # Non-table DDL also uses Table nodes, for example DROP SEQUENCE.
            if isinstance(parent, (exp.Drop, exp.Alter)) and parent.args.get("kind") != "TABLE":
                return expression
            # COPY resolves its target by its Snowflake name for table stages and load history.
            if isinstance(parent, exp.Copy):
                return expression
            name = expression.table if isinstance(expression, exp.Column) else expression.name
            if self.oid(expression.catalog or current_database, expression.db, name) is not None:
                expression.set("catalog", exp.to_identifier("temp"))
                expression.set("db", exp.to_identifier("main"))
        return expression

    def record(self, expression: Expr) -> None:
        if not (
            isinstance(expression, exp.Create)
            and (owner := expression.args.get("temporary_table_owner"))
            and (table := expression.find(exp.Table))
        ):
            return
        # Temporary table names are unique within the session, so the name identifies the new table's OID.
        sql = "SELECT table_name, table_oid FROM duckdb_tables() WHERE temporary"
        live = dict(self._duck_conn.execute(sql).fetchall())
        # Forget dropped tables here rather than tracking DROP, so the registry is bounded by live tables.
        live_oids = set(live.values())
        self._temporary = {oid: owner for oid, owner in self._temporary.items() if oid in live_oids}
        # CREATE TEMP TABLE IF NOT EXISTS succeeds without creating anything when the name is already taken, even
        # from another schema, so the OID may already be registered. Keep the schema it was created in; overwriting
        # it would move the existing table's stage to the current schema.
        self._temporary.setdefault(live[table.name], owner)

    def oid(self, catalog: str | None, schema: str, name: str) -> int | None:
        if not self._temporary:
            return None
        # At most one temporary table has this name in the session. Return its OID only if it was created in the
        # requested schema.
        sql = "SELECT table_oid FROM duckdb_tables() WHERE temporary AND table_name = ?"
        live = self._duck_conn.execute(sql, [name]).fetchone()
        return live[0] if live and self._temporary.get(live[0]) == (catalog, schema) else None

from __future__ import annotations

import duckdb
import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp


class TemporaryTables:
    """Track Snowflake schema ownership for DuckDB's session-local temporary tables.

    Table OIDs survive renames and are never reused. Lookups return only live OIDs. Registrations aren't pruned when
    a table is dropped, because ROLLBACK can restore it under the same OID. Both SQL references and table stages use
    this registry.
    """

    def __init__(self, duck_conn: DuckDBPyConnection):
        self._duck_conn = duck_conn
        # Temporary table OID -> (catalog, schema, schema OID) it was created in.
        self._temporary: dict[int, tuple[str | None, str | None, int]] = {}

    def transform(self, expression: Expr, current_database: str | None, current_schema: str | None) -> Expr:
        """Create temporary tables in DuckDB's temp catalog and resolve their qualified references.

        Snowflake temporary tables belong to a database and schema, whereas DuckDB keeps them all in temp.main.
        See https://docs.snowflake.com/en/user-guide/tables-temp-transient
        """
        if expression.parent is None and self._temporary:
            self._drop_orphans()
        if (
            isinstance(expression, exp.Create)
            and expression.kind == "TABLE"
            and expression.find(exp.TemporaryProperty)
            and (table := expression.find(exp.Table))
        ):
            catalog, schema = table.catalog or current_database, table.db or current_schema
            # Removing the qualifiers bypasses DuckDB's schema validation, so check the original owner first.
            sql = "SELECT oid FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?"
            if not (schema_oid := self._duck_conn.execute(sql, [catalog, schema]).fetchone()):
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nSchema '{catalog}.{schema}' does not exist or not authorized.",
                    errno=2003,
                    sqlstate="42S02",
                )
            expression.set("temporary_table_owner", (catalog, schema, schema_oid[0]))
            table.set("catalog", None)
            table.set("db", None)
            # Snowflake doesn't enforce foreign keys, and DuckDB won't drop a table that one references, which would
            # stop a temporary table being dropped with its schema.
            for node in list(expression.find_all(exp.ForeignKey, exp.Reference)):
                if isinstance(node.parent, (exp.ColumnConstraint, exp.Constraint)):
                    node.parent.pop()
                elif isinstance(node, exp.ForeignKey):
                    node.pop()
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
        # CREATE TEMP TABLE IF NOT EXISTS succeeds without creating anything when the name is already taken, even
        # from another schema, so the OID may already be registered. Keep the schema it was created in; overwriting
        # it would move the existing table's stage to the current schema.
        self._temporary.setdefault(live[table.name], owner)

    def _drop_orphans(self) -> None:
        """Drop temporary tables whose schema has been dropped or replaced since they were created, by any session.

        In Snowflake dropping a schema drops its temporary tables, which DuckDB keeps in temp.main instead. Checking
        the schema's OID before each statement covers other sessions and ROLLBACK, which restores the original OID.
        """
        sql = "SELECT table_oid, table_name FROM duckdb_tables() WHERE temporary"
        try:
            live = self._duck_conn.execute(sql).fetchall()
        except duckdb.TransactionException:
            # An aborted transaction rejects every query until it ends, so leave them to the next statement.
            return
        if not (owned := [(name, self._temporary[oid]) for oid, name in live if oid in self._temporary]):
            return
        sql = "SELECT database_name, schema_name, oid FROM duckdb_schemas()"
        schemas = {(catalog, schema): oid for catalog, schema, oid in self._duck_conn.execute(sql).fetchall()}
        for name, (catalog, schema, schema_oid) in owned:
            if schemas.get((catalog, schema)) != schema_oid:
                table = exp.table_(name, db="main", catalog="temp", quoted=True)
                self._duck_conn.execute(f"DROP TABLE {table.sql(dialect='duckdb')}")

    def oid(self, catalog: str | None, schema: str, name: str) -> int | None:
        if not self._temporary:
            return None
        # At most one temporary table has this name in the session. Return its OID only if it was created in the
        # requested schema, and that schema hasn't since been replaced.
        sql = """
            SELECT table_oid, (SELECT oid FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?)
            FROM duckdb_tables() WHERE temporary AND table_name = ?
        """
        live = self._duck_conn.execute(sql, [catalog, schema, name]).fetchone()
        return live[0] if live and self._temporary.get(live[0]) == (catalog, schema, live[1]) else None

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
        # Names of the registered temporary tables live when this statement started, plus any created since, and
        # while a transaction is open, any live since it began, which ROLLBACK can restore. A qualified reference to
        # any other name can't be a temporary table, so it needs no catalog read.
        self._names: set[str] = set()

    def transform(
        self, expression: Expr, current_database: str | None, current_schema: str | None, in_transaction: bool
    ) -> Expr:
        """Create temporary tables in DuckDB's temp catalog and resolve their qualified references.

        Snowflake temporary tables belong to a database and schema, whereas DuckDB keeps them all in temp.main.
        See https://docs.snowflake.com/en/user-guide/tables-temp-transient
        """
        if expression.parent is None and self._temporary:
            self._drop_orphans(in_transaction)
        if (
            isinstance(expression, exp.Create)
            and expression.kind == "TABLE"
            and expression.find(exp.TemporaryProperty)
            and (table := expression.find(exp.Table))
        ):
            catalog, schema = table.catalog or current_database, table.db or current_schema
            # Removing the qualifiers bypasses DuckDB's schema validation, so check the original owner first.
            try:
                schema_oid = self._schema_oid(catalog, schema)
            except duckdb.TransactionException:
                # An aborted transaction rejects the lookup. Outside autocommit, DDL ends the transaction before it
                # runs, so validate() checks the schema then, before the table is created or replaced.
                schema_oid = None
            expression.set("temporary_table_owner", (catalog, schema, schema_oid))
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
            # Foreign keys left in a CREATE are a permanent table's, which reference permanent tables.
            if expression.find_ancestor(exp.Reference):
                return expression
            name = expression.table if isinstance(expression, exp.Column) else expression.name
            # Only read the catalog for a name that could be a temporary table's: in an aborted transaction the read
            # fails, see #470. A reference that does match one still fails then, even in DDL, which outside autocommit
            # would otherwise end the transaction and run.
            if (
                name in self._names
                and self.oid(expression.catalog or current_database, expression.db, name) is not None
            ):
                expression.set("catalog", exp.to_identifier("temp"))
                expression.set("db", exp.to_identifier("main"))
        return expression

    def validate(self, expression: Expr, in_transaction: bool) -> None:
        """Check the schema of a temporary table that transform() couldn't, just before it's created.

        Called once DDL has ended the aborted transaction, so nothing is created or replaced in a missing schema. In
        autocommit mode the transaction is still aborted, and the lookup fails as the CREATE itself would.
        """
        if (owner := expression.args.get("temporary_table_owner")) and owner[2] is None:
            # transform() couldn't drop orphans either, and one could take the new table's name.
            self._drop_orphans(in_transaction)
            catalog, schema, _ = owner
            expression.set("temporary_table_owner", (catalog, schema, self._schema_oid(catalog, schema)))

    def record(self, expression: Expr) -> None:
        if not (
            isinstance(expression, exp.Create)
            and (owner := expression.args.get("temporary_table_owner"))
            and (table := expression.find(exp.Table))
        ):
            return
        # Temporary table names are unique within the session, so the name identifies the new table's OID.
        sql = "SELECT table_name, table_oid FROM duckdb_tables() WHERE temporary"
        oid = dict(self._duck_conn.execute(sql).fetchall())[table.name]
        # CREATE TEMP TABLE IF NOT EXISTS succeeds without creating anything when the name is already taken, even
        # from another schema, so the OID may already be registered. Keep the schema it was created in; overwriting
        # it would move the existing table's stage to the current schema.
        self._temporary.setdefault(oid, owner)
        self._names.add(table.name)

    def _drop_orphans(self, in_transaction: bool) -> None:
        """Drop temporary tables whose schema has been dropped or replaced since they were created, by any session.

        In Snowflake dropping a schema drops its temporary tables, which DuckDB keeps in temp.main instead. Checking
        the schema's OID before each statement covers other sessions and ROLLBACK, which restores the original OID.
        """
        sql = "SELECT table_oid, table_name FROM duckdb_tables() WHERE temporary"
        try:
            live = self._duck_conn.execute(sql).fetchall()
        except duckdb.TransactionException:
            # An aborted transaction rejects every query until it ends, so leave them to the next statement. Until it
            # ends, nothing can rename, drop or restore a temporary table, so the names still hold.
            return
        owned = [(name, self._temporary[oid]) for oid, name in live if oid in self._temporary]
        # Refresh the names, which renames, drops and ROLLBACK change. Ending a transaction, even by DDL once a failed
        # statement has aborted it, can restore the names live when it began, so until then only add to them.
        names = {name for name, _ in owned}
        self._names = self._names | names if in_transaction else names
        if not owned:
            return
        sql = "SELECT database_name, schema_name, oid FROM duckdb_schemas()"
        schemas = {(catalog, schema): oid for catalog, schema, oid in self._duck_conn.execute(sql).fetchall()}
        for name, (catalog, schema, schema_oid) in owned:
            if schemas.get((catalog, schema)) != schema_oid:
                table = exp.table_(name, db="main", catalog="temp", quoted=True)
                self._duck_conn.execute(f"DROP TABLE {table.sql(dialect='duckdb')}")
                if not in_transaction:
                    self._names.discard(name)

    def _schema_oid(self, catalog: str | None, schema: str | None) -> int:
        sql = "SELECT oid FROM duckdb_schemas() WHERE database_name = ? AND schema_name = ?"
        if not (row := self._duck_conn.execute(sql, [catalog, schema]).fetchone()):
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error:\nSchema '{catalog}.{schema}' does not exist or not authorized.",
                errno=2003,
                sqlstate="42S02",
            )
        return row[0]

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

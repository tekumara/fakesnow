from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import duckdb
import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
from sqlglot import Expr, exp
from sqlglot.optimizer.scope import Scope, traverse_scope

from fakesnow.transforms.transforms import SUCCESS_NOP

TEMP_CATALOG = "temp"
TEMP_SCHEMA = "main"
TEMP_TABLE_PREFIX = "_fs_temp_tbl_"
TEMP_CATALOG_TABLE = "_fs_temp_tables"
TEMP_COLUMNS_EXT_TABLE = "_fs_temp_columns_ext"
TEMP_INFO_TABLES_VIEW = "_fs_temp_info_tables"
TEMP_INFO_COLUMNS_VIEW = "_fs_temp_info_columns"

LogicalName = tuple[str, str, str]

_SQL_INIT_TEMP_CATALOG = f"""
CREATE TEMPORARY TABLE IF NOT EXISTS {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} (
    temp_id VARCHAR PRIMARY KEY,
    physical_name VARCHAR NOT NULL UNIQUE,
    database_name VARCHAR NOT NULL,
    schema_name VARCHAR NOT NULL,
    table_name VARCHAR NOT NULL,
    owner_schema_oid BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    last_altered TIMESTAMPTZ NOT NULL
);

CREATE TEMPORARY TABLE IF NOT EXISTS {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE} (
    physical_name VARCHAR NOT NULL,
    column_name VARCHAR NOT NULL,
    character_maximum_length INTEGER,
    character_octet_length INTEGER,
    PRIMARY KEY (physical_name, column_name)
);

CREATE OR REPLACE TEMPORARY VIEW {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_INFO_TABLES_VIEW} AS
SELECT
    r.database_name AS "TABLE_CATALOG",
    r.schema_name AS "TABLE_SCHEMA",
    r.table_name AS "TABLE_NAME",
    'SYSADMIN' AS "TABLE_OWNER",
    'LOCAL TEMPORARY' AS "TABLE_TYPE",
    'NO' AS "IS_TRANSIENT",
    NULL::VARCHAR AS "CLUSTERING_KEY",
    t.estimated_size AS "ROW_COUNT",
    0 AS "BYTES",
    1 AS "RETENTION_TIME",
    NULL::VARCHAR AS "SELF_REFERENCING_COLUMN_NAME",
    NULL::VARCHAR AS "REFERENCE_GENERATION",
    NULL::VARCHAR AS "USER_DEFINED_TYPE_CATALOG",
    NULL::VARCHAR AS "USER_DEFINED_TYPE_SCHEMA",
    NULL::VARCHAR AS "USER_DEFINED_TYPE_NAME",
    'YES' AS "IS_INSERTABLE_INTO",
    'YES' AS "IS_TYPED",
    NULL::VARCHAR AS "COMMIT_ACTION",
    r.created_at AS "CREATED",
    r.last_altered AS "LAST_ALTERED",
    r.last_altered AS "LAST_DDL",
    'SYSADMIN' AS "LAST_DDL_BY",
    'NO' AS "AUTO_CLUSTERING_ON",
    t.comment AS "COMMENT",
    'YES' AS "IS_TEMPORARY",
    'NO' AS "IS_ICEBERG",
    'NO' AS "IS_DYNAMIC",
    'NO' AS "IS_IMMUTABLE",
    'NO' AS "IS_HYBRID"
FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} r
JOIN duckdb_tables() t
  ON t.temporary AND t.table_name = r.physical_name
JOIN duckdb_schemas() s
  ON s.oid = r.owner_schema_oid;

CREATE OR REPLACE TEMPORARY VIEW {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_INFO_COLUMNS_VIEW} AS
SELECT
    r.database_name AS TABLE_CATALOG,
    r.schema_name AS TABLE_SCHEMA,
    r.table_name AS TABLE_NAME,
    columns.column_name AS COLUMN_NAME,
    columns.ordinal_position AS ORDINAL_POSITION,
    columns.column_default AS COLUMN_DEFAULT,
    columns.is_nullable AS IS_NULLABLE,
    CASE
        WHEN starts_with(columns.data_type, 'DECIMAL') OR columns.data_type = 'BIGINT' THEN 'NUMBER'
        WHEN columns.data_type = 'VARCHAR' THEN 'TEXT'
        WHEN columns.data_type = 'DOUBLE' THEN 'FLOAT'
        WHEN columns.data_type = 'BLOB' THEN 'BINARY'
        WHEN columns.data_type = 'TIMESTAMP' THEN 'TIMESTAMP_NTZ'
        WHEN columns.data_type = 'TIMESTAMP WITH TIME ZONE' THEN 'TIMESTAMP_TZ'
        WHEN columns.data_type = 'JSON' THEN 'VARIANT'
        ELSE columns.data_type
    END AS DATA_TYPE,
    ext.character_maximum_length AS CHARACTER_MAXIMUM_LENGTH,
    ext.character_octet_length AS CHARACTER_OCTET_LENGTH,
    CASE
        WHEN columns.data_type = 'BIGINT' THEN 38
        WHEN columns.data_type = 'DOUBLE' THEN NULL
        ELSE columns.numeric_precision
    END AS NUMERIC_PRECISION,
    CASE
        WHEN columns.data_type = 'BIGINT' THEN 10
        WHEN columns.data_type = 'DOUBLE' THEN NULL
        ELSE columns.numeric_precision_radix
    END AS NUMERIC_PRECISION_RADIX,
    CASE
        WHEN columns.data_type = 'DOUBLE' THEN NULL
        ELSE columns.numeric_scale
    END AS NUMERIC_SCALE,
    columns.collation_name AS COLLATION_NAME,
    columns.is_identity AS IS_IDENTITY,
    columns.identity_generation AS IDENTITY_GENERATION,
    columns.identity_cycle AS IDENTITY_CYCLE,
    ddb_columns.comment AS COMMENT,
    NULL::VARCHAR AS IDENTITY_START,
    NULL::VARCHAR AS IDENTITY_INCREMENT,
    NULL::VARCHAR AS IDENTITY_ORDERED,
    NULL::VARCHAR AS DATA_TYPE_ALIAS
FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} r
JOIN duckdb_schemas() s
  ON s.oid = r.owner_schema_oid
JOIN system.information_schema.columns columns
  ON columns.table_catalog = '{TEMP_CATALOG}'
 AND columns.table_schema = '{TEMP_SCHEMA}'
 AND columns.table_name = r.physical_name
LEFT JOIN {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE} ext
  ON ext.physical_name = r.physical_name
 AND ext.column_name = columns.column_name
LEFT JOIN duckdb_columns() ddb_columns
  ON ddb_columns.database_name = '{TEMP_CATALOG}'
 AND ddb_columns.schema_name = '{TEMP_SCHEMA}'
 AND ddb_columns.table_name = r.physical_name
 AND ddb_columns.column_name = columns.column_name;
"""


@dataclass(frozen=True)
class TempTable:
    temp_id: str
    physical_name: str
    database: str
    schema: str
    table: str
    owner_schema_oid: int

    @property
    def logical_name(self) -> LogicalName:
        return (self.database, self.schema, self.table)


@dataclass(frozen=True)
class _PendingCreate:
    table: TempTable
    replaced: TempTable | None = None
    comment: str | None = None
    text_lengths: list[tuple[str, int]] = field(default_factory=list)
    like_source: TempTable | None = None


@dataclass(frozen=True)
class _PendingRename:
    table: TempTable
    new_database: str
    new_schema: str
    new_table: str
    new_schema_oid: int


@dataclass
class PreparedStatement:
    expression: Expr
    create_temp: _PendingCreate | None = None
    drop_temps: list[TempTable] = field(default_factory=list)
    rename_temp: _PendingRename | None = None
    altered_temp: TempTable | None = None
    altered_text_lengths: list[tuple[str, int]] = field(default_factory=list)
    describe_temp: TempTable | None = None
    comment_temp: tuple[TempTable, str] | None = None
    copy_target_temp: TempTable | None = None
    logical_result_name: str | None = None
    created_logical_target: LogicalName | None = None


class TemporaryTableCatalog:
    """Per-connection DuckDB-resident overlay catalog for Snowflake temporary tables.

    Snowflake scopes temporary tables to (session, database, schema, table) and resolves
    unqualified names along SEARCH_PATH ($current, $public), checking temporary before
    permanent within each schema. DuckDB stores session temporary tables in a single flat
    temp.main namespace.

    This catalog maps logical Snowflake names to unique physical tables in temp.main and
    stores all catalog and column-length metadata in connection-local DuckDB temp tables,
    so closing the connection cleans up everything automatically.
    """

    def __init__(self, duck_conn: DuckDBPyConnection) -> None:
        self._duck_conn = duck_conn
        self._initialized = False

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        self._duck_conn.execute(_SQL_INIT_TEMP_CATALOG)
        self._initialized = True

    def _live_catalog_rows(self) -> list[TempTable]:
        if not self._initialized:
            return []
        rows = self._duck_conn.execute(
            f"""
            SELECT r.temp_id, r.physical_name, r.database_name, r.schema_name, r.table_name, r.owner_schema_oid
            FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} r
            JOIN duckdb_tables() t
              ON t.temporary AND t.table_name = r.physical_name
            JOIN duckdb_schemas() s
              ON s.oid = r.owner_schema_oid
            """
        ).fetchall()
        return [TempTable(*row) for row in rows]

    def resolve_qualified(self, database: str, schema: str, table: str) -> TempTable | None:
        """Resolve an explicit (database, schema, table) against live session temp tables."""
        if not self._initialized:
            return None
        rows = self._duck_conn.execute(
            f"""
            SELECT r.temp_id, r.physical_name, r.database_name, r.schema_name, r.table_name, r.owner_schema_oid
            FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} r
            JOIN duckdb_tables() t
              ON t.temporary AND t.table_name = r.physical_name
            JOIN duckdb_schemas() s
              ON s.oid = r.owner_schema_oid
            WHERE r.database_name = ? AND r.schema_name = ? AND r.table_name = ?
            LIMIT 1
            """,
            [database, schema, table],
        ).fetchall()
        return TempTable(*rows[0]) if rows else None

    def _schema_oid(self, database: str, schema: str) -> int | None:
        rows = self._duck_conn.execute(
            """
            SELECT oid FROM duckdb_schemas()
            WHERE upper(database_name) = upper(?) AND upper(schema_name) = upper(?)
            LIMIT 1
            """,
            [database, schema],
        ).fetchall()
        return int(rows[0][0]) if rows else None

    def _permanent_relation_exists(self, database: str, schema: str, table: str) -> bool:
        rows = self._duck_conn.execute(
            """
            SELECT 1 FROM duckdb_tables()
            WHERE NOT temporary
              AND upper(database_name) = upper(?)
              AND upper(schema_name) = upper(?)
              AND table_name = ?
            UNION ALL
            SELECT 1 FROM duckdb_views()
            WHERE NOT temporary
              AND upper(database_name) = upper(?)
              AND upper(schema_name) = upper(?)
              AND view_name = ?
            LIMIT 1
            """,
            [database, schema, table, database, schema, table],
        ).fetchall()
        return bool(rows)

    def _search_path(self, current_schema: str | None) -> list[str]:
        schemas: list[str] = []
        if current_schema:
            schemas.append(current_schema)
        if "PUBLIC" not in schemas:
            schemas.append("PUBLIC")
        return schemas

    def _resolve_table(
        self,
        table: exp.Table,
        current_database: str | None,
        current_schema: str | None,
        live_by_name: dict[LogicalName, TempTable],
        *,
        allow_public_fallback: bool = True,
    ) -> tuple[TempTable | None, LogicalName | None]:
        """Resolve a table reference to either a live TempTable or a PUBLIC permanent fallback.

        Returns:
            (temp_table, public_permanent_logical_name) — at most one is non-None.
        """
        name = _ident_name(table.this)
        if not name:
            return None, None

        catalog = _ident_name(table.args.get("catalog"))
        db = _ident_name(table.args.get("db"))

        if catalog or db:
            resolved_db = catalog or current_database
            resolved_schema = db or current_schema
            if resolved_db and resolved_schema:
                return live_by_name.get((resolved_db, resolved_schema, name)), None
            return None, None

        if not current_database:
            return None, None

        candidates = (
            self._search_path(current_schema) if allow_public_fallback else ([current_schema] if current_schema else [])
        )
        for idx, cand_schema in enumerate(candidates):
            if temp := live_by_name.get((current_database, cand_schema, name)):
                return temp, None
            # Check permanent relation before continuing to the next schema in SEARCH_PATH,
            # because a permanent object in $current shadows a temp or permanent object in $public.
            if idx == 0 and len(candidates) > 1:
                if self._permanent_relation_exists(current_database, cand_schema, name):
                    return None, None
            elif idx > 0 and self._permanent_relation_exists(current_database, cand_schema, name):
                return None, (current_database, cand_schema, name)

        return None, None

    def prepare(
        self,
        expression: Expr,
        current_database: str | None,
        current_schema: str | None,
    ) -> PreparedStatement:
        """Rewrite a normalized Snowflake AST against the session's temporary-table overlay."""
        plan = PreparedStatement(expression=expression)

        # Handle table_comment already extracted by extract_comment_on_table for
        # COMMENT ON TABLE / ALTER TABLE SET COMMENT (transformed into SUCCESS_NOP).
        if (
            not isinstance(expression, exp.Create)
            and (table_comment := expression.args.get("table_comment"))
            and isinstance(table_comment, tuple)
            and len(table_comment) == 2
        ):
            target_table, comment = table_comment
            if isinstance(target_table, exp.Table) and isinstance(comment, str):
                live = {t.logical_name: t for t in self._live_catalog_rows()}
                temp, _ = self._resolve_table(target_table, current_database, current_schema, live)
                if temp is not None:
                    plan.comment_temp = (temp, comment)
                    expression.args.pop("table_comment", None)
                    return plan

        is_create_temp = _is_create_temp_table(expression)
        if not self._initialized and not is_create_temp:
            # Fast path when no temp tables exist in this session; still honor $public
            # fallback for unqualified query/DML references if PUBLIC exists.
            if current_database and current_schema and current_schema != "PUBLIC":
                self._rewrite_references(expression, current_database, current_schema, {}, set())
            return plan

        live_rows = self._live_catalog_rows()
        live = {t.logical_name: t for t in live_rows}
        skip_ids: set[int] = set()

        # 1. Statement targets (must be classified distinctly from query sources).
        if is_create_temp:
            assert isinstance(expression, exp.Create)
            self._prepare_create_temp(expression, current_database, current_schema, live, plan, skip_ids)
        elif _is_create_perm_table(expression):
            assert isinstance(expression, exp.Create)
            target = _extract_target_table(expression.this)
            if target is not None:
                skip_ids.add(id(target))
        elif isinstance(expression, exp.Drop) and str(expression.args.get("kind") or "").upper() == "TABLE":
            self._prepare_drop(expression, current_database, current_schema, live, plan, skip_ids)
        elif isinstance(expression, exp.Alter) and str(expression.args.get("kind") or "").upper() == "TABLE":
            if self._prepare_alter(expression, current_database, current_schema, live, plan, skip_ids):
                return plan
        elif isinstance(expression, exp.TruncateTable):
            self._prepare_truncate(expression, current_database, current_schema, live, skip_ids)
        elif isinstance(expression, exp.Describe) and str(expression.args.get("kind") or "").upper() in (
            "TABLE",
            "VIEW",
        ):
            self._prepare_describe(expression, current_database, current_schema, live, plan, skip_ids)
        elif isinstance(expression, exp.Copy):
            self._prepare_copy(expression, current_database, current_schema, live, plan, skip_ids)
        elif isinstance(expression, exp.Insert):
            self._prepare_dml_target(expression, current_database, current_schema, live, skip_ids, allow_alias=False)
        elif isinstance(expression, (exp.Update, exp.Delete)):
            self._prepare_dml_target(expression, current_database, current_schema, live, skip_ids, allow_alias=True)

        # 2. Query sources, subqueries, JOINs, LIKE/USING sources, and INFORMATION_SCHEMA overlays.
        self._rewrite_references(expression, current_database, current_schema, live, skip_ids)
        return plan

    def _prepare_create_temp(
        self,
        expression: exp.Create,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        plan: PreparedStatement,
        skip_ids: set[int],
    ) -> None:
        target = _extract_target_table(expression.this)
        if target is None:
            return
        skip_ids.add(id(target))

        table_name = _ident_name(target.this)
        db = _ident_name(target.args.get("catalog")) or current_database
        schema = _ident_name(target.args.get("db")) or current_schema
        if not (db and schema and table_name):
            return

        schema_oid = self._schema_oid(db, schema)
        if schema_oid is None:
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error:\nSchema '{db}.{schema}' does not exist or not authorized.",
                errno=2003,
                sqlstate="42S02",
            )

        # DuckDB rejects foreign-key constraints on temp tables referencing permanent tables;
        # Snowflake parses FK syntax without enforcing it.
        _strip_foreign_key_constraints(expression)

        # Capture LIKE source before rewriting references so we can propagate VARCHAR(N) lengths.
        like_source: TempTable | None = None
        if (like_prop := expression.find(exp.LikeProperty)) and isinstance(like_prop.this, exp.Table):
            like_source, _ = self._resolve_table(like_prop.this, current_database, current_schema, live)

        existing = live.get((db, schema, table_name))
        replace = bool(expression.args.get("replace"))
        if_not_exists = bool(expression.args.get("exists"))

        plan.logical_result_name = table_name

        if existing is not None:
            if if_not_exists:
                # Let DuckDB treat CREATE TEMPORARY TABLE IF NOT EXISTS <existing> as a no-op.
                _set_temp_target(target, existing.physical_name)
                expression.args.pop("table_comment", None)
                expression.args.pop("text_lengths", None)
                return
            if not replace:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nObject '{table_name}' already exists.",
                    errno=2002,
                    sqlstate="42710",
                )

        temp_id = uuid.uuid4().hex[:16]
        physical_name = f"{TEMP_TABLE_PREFIX}{temp_id}"
        _set_temp_target(target, physical_name)
        # Creating under a fresh unique physical name makes self-referential
        # CREATE OR REPLACE TEMP TABLE t AS SELECT * FROM t atomic.
        expression.set("replace", False)

        comment: str | None = None
        if table_comment := expression.args.pop("table_comment", None):
            _, comment_val = table_comment
            if isinstance(comment_val, str):
                comment = comment_val

        text_lengths: list[tuple[str, int]] = list(expression.args.pop("text_lengths", None) or [])

        new_table = TempTable(
            temp_id=temp_id,
            physical_name=physical_name,
            database=db,
            schema=schema,
            table=table_name,
            owner_schema_oid=schema_oid,
        )
        plan.create_temp = _PendingCreate(
            table=new_table,
            replaced=existing,
            comment=comment,
            text_lengths=text_lengths,
            like_source=like_source,
        )
        plan.created_logical_target = (db, schema, table_name)

    def _prepare_drop(
        self,
        expression: exp.Drop,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        plan: PreparedStatement,
        skip_ids: set[int],
    ) -> None:
        tables: list[exp.Table] = []
        if isinstance(expression.this, exp.Table):
            tables.append(expression.this)
        for tbl in expression.args.get("tables") or []:
            if isinstance(tbl, exp.Table) and tbl not in tables:
                tables.append(tbl)

        for table in tables:
            skip_ids.add(id(table))
            temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
            if temp is not None:
                _rewrite_to_temp(table, temp.physical_name, alias=None)
                plan.drop_temps.append(temp)
                if plan.logical_result_name is None:
                    plan.logical_result_name = temp.table
            elif public_perm is not None:
                _qualify_table(table, *public_perm)

    def _prepare_alter(
        self,
        expression: exp.Alter,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        plan: PreparedStatement,
        skip_ids: set[int],
    ) -> bool:
        """Prepare ALTER TABLE. Returns True if execution is replaced by a catalog-only rename."""
        target = expression.this
        if not isinstance(target, exp.Table):
            return False
        skip_ids.add(id(target))

        temp, public_perm = self._resolve_table(target, current_database, current_schema, live)
        if temp is None:
            if public_perm is not None:
                _qualify_table(target, *public_perm)
            return False

        actions = expression.args.get("actions") or []
        if len(actions) == 1 and isinstance(actions[0], exp.AlterRename) and isinstance(actions[0].this, exp.Table):
            new_tbl = actions[0].this
            new_name = _ident_name(new_tbl.this)
            if not new_name:
                return False
            new_db = _ident_name(new_tbl.args.get("catalog")) or temp.database
            new_schema = _ident_name(new_tbl.args.get("db")) or temp.schema
            new_schema_oid = self._schema_oid(new_db, new_schema)
            if new_schema_oid is None:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nSchema '{new_db}.{new_schema}' does not exist or not authorized.",
                    errno=2003,
                    sqlstate="42S02",
                )
            conflict = live.get((new_db, new_schema, new_name))
            if conflict is not None and conflict.temp_id != temp.temp_id:
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"SQL compilation error:\nObject '{new_name}' already exists.",
                    errno=2002,
                    sqlstate="42710",
                )
            plan.rename_temp = _PendingRename(
                table=temp,
                new_database=new_db,
                new_schema=new_schema,
                new_table=new_name,
                new_schema_oid=new_schema_oid,
            )
            plan.expression = SUCCESS_NOP.copy()
            return True

        _rewrite_to_temp(target, temp.physical_name, alias=None)
        plan.altered_temp = temp
        if text_lengths := expression.args.pop("text_lengths", None):
            plan.altered_text_lengths = list(text_lengths)
        return False

    def _prepare_truncate(
        self,
        expression: exp.TruncateTable,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        skip_ids: set[int],
    ) -> None:
        targets: list[exp.Table] = []
        if isinstance(expression.this, exp.Table):
            targets.append(expression.this)
        for tbl in expression.expressions:
            if isinstance(tbl, exp.Table) and tbl not in targets:
                targets.append(tbl)
        for table in targets:
            skip_ids.add(id(table))
            temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
            if temp is not None:
                _rewrite_to_temp(table, temp.physical_name, alias=None)
            elif public_perm is not None:
                _qualify_table(table, *public_perm)

    def _prepare_describe(
        self,
        expression: exp.Describe,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        plan: PreparedStatement,
        skip_ids: set[int],
    ) -> None:
        table = expression.find(exp.Table)
        if table is None:
            return
        skip_ids.add(id(table))
        if _ident_name(table.args.get("db")) == "_FS_INFORMATION_SCHEMA":
            return

        temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
        if temp is not None:
            _rewrite_to_temp(table, temp.physical_name, alias=None)
            plan.describe_temp = temp
        elif public_perm is not None:
            _qualify_table(table, *public_perm)

    def _prepare_copy(
        self,
        expression: exp.Copy,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        plan: PreparedStatement,
        skip_ids: set[int],
    ) -> None:
        target = _extract_target_table(expression.this)
        if target is None:
            return
        skip_ids.add(id(target))

        temp, public_perm = self._resolve_table(target, current_database, current_schema, live)
        if temp is not None:
            _rewrite_to_temp(target, temp.physical_name, alias=None)
            plan.copy_target_temp = temp
        elif public_perm is not None:
            _qualify_table(target, *public_perm)

    def _prepare_dml_target(
        self,
        expression: exp.Insert | exp.Update | exp.Delete,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        skip_ids: set[int],
        *,
        allow_alias: bool,
    ) -> None:
        target = _extract_target_table(expression.this)
        if target is None:
            return
        skip_ids.add(id(target))

        temp, public_perm = self._resolve_table(target, current_database, current_schema, live)
        if temp is not None:
            orig_catalog = _ident_name(target.args.get("catalog"))
            orig_db = _ident_name(target.args.get("db"))
            alias = target.args.get("alias")
            if allow_alias and alias is None:
                alias = exp.TableAlias(this=exp.Identifier(this=temp.table, quoted=True))
            _rewrite_to_temp(target, temp.physical_name, alias=alias if allow_alias else None)
            if allow_alias:
                bound_alias = _ident_name(alias.this) if isinstance(alias, exp.TableAlias) else temp.table
                assert bound_alias is not None
                _strip_qualified_column_prefixes(expression, orig_catalog, orig_db, temp.table, bound_alias)
        elif public_perm is not None:
            _qualify_table(target, *public_perm)

    def _rewrite_references(
        self,
        expression: Expr,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        skip_ids: set[int],
    ) -> None:
        is_merge_internal = bool(expression.args.get("merge_internal"))

        # 1. Scope-based query sources (SELECT, JOIN, subqueries, CTE bodies).
        for scope in traverse_scope(expression):
            self._rewrite_scope_sources(
                scope, expression, current_database, current_schema, live, skip_ids, is_merge_internal
            )

        # 2. Non-SELECT table sources not exposed by traverse_scope (DELETE USING, LIKE, CLONE).
        for table in list(expression.find_all(exp.Table)):
            if id(table) in skip_ids or not isinstance(table.this, exp.Identifier):
                continue
            if is_merge_internal and _ident_name(table.this) == "MERGE_CANDIDATES":
                continue
            parent = table.parent
            if isinstance(parent, (exp.Clone, exp.LikeProperty)):
                skip_ids.add(id(table))
                temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
                if temp is not None:
                    _rewrite_to_temp(table, temp.physical_name, alias=None)
                elif public_perm is not None:
                    _qualify_table(table, *public_perm)
            elif isinstance(parent, (exp.Delete, exp.Merge)) and table.arg_key == "using":
                skip_ids.add(id(table))
                temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
                if temp is not None:
                    orig_catalog = _ident_name(table.args.get("catalog"))
                    orig_db = _ident_name(table.args.get("db"))
                    alias = table.args.get("alias") or exp.TableAlias(this=exp.Identifier(this=temp.table, quoted=True))
                    _rewrite_to_temp(table, temp.physical_name, alias=alias)
                    bound_alias = _ident_name(alias.this) or temp.table
                    _strip_qualified_column_prefixes(parent, orig_catalog, orig_db, temp.table, bound_alias)
                elif public_perm is not None:
                    _qualify_table(table, *public_perm)

    def _rewrite_scope_sources(
        self,
        scope: Scope,
        statement: Expr,
        current_database: str | None,
        current_schema: str | None,
        live: dict[LogicalName, TempTable],
        skip_ids: set[int],
        is_merge_internal: bool,
    ) -> None:
        tables = [
            source
            for source in scope.sources.values()
            if isinstance(source, exp.Table)
            and id(source) not in skip_ids
            and isinstance(source.this, exp.Identifier)
            and not (is_merge_internal and _ident_name(source.this) == "MERGE_CANDIDATES")
        ]
        if not tables:
            return

        # For UPDATE ... FROM and DELETE ... USING, SQLGlot wraps the FROM/USING table in a
        # scope whose expression is the Table itself; walk the enclosing statement to reach
        # SET/WHERE column qualifiers.
        col_root = scope.expression if isinstance(scope.expression, exp.Select) else statement

        # Track unaliased logical table names in this scope to avoid duplicate implicit aliases
        # when joining same-named tables from different schemas (e.g. s1.t JOIN s2.t).
        unaliased_counts: dict[str, int] = {}
        for table in tables:
            if not table.args.get("alias") and (name := _ident_name(table.this)):
                unaliased_counts[name] = unaliased_counts.get(name, 0) + 1

        for table in tables:
            skip_ids.add(id(table))
            if self._initialized and _is_info_schema_overlay_target(table):
                self._overlay_info_schema_table(table, current_database)
                continue

            orig_catalog = _ident_name(table.args.get("catalog"))
            orig_db = _ident_name(table.args.get("db"))
            temp, public_perm = self._resolve_table(table, current_database, current_schema, live)
            if temp is not None:
                explicit_alias = table.args.get("alias")
                if explicit_alias is not None:
                    alias = explicit_alias
                    bound_alias = _ident_name(explicit_alias.this) or temp.table
                elif unaliased_counts.get(temp.table, 0) == 1:
                    # Hygienic implicit alias preservation: keeps unqualified t.col valid in DuckDB.
                    alias = exp.TableAlias(this=exp.Identifier(this=temp.table, quoted=True))
                    bound_alias = temp.table
                else:
                    # Collision within scope (e.g. s1.t JOIN s2.t): bind under physical name.
                    alias = None
                    bound_alias = temp.physical_name
                _rewrite_to_temp(table, temp.physical_name, alias=alias)
                _strip_qualified_column_prefixes(col_root, orig_catalog, orig_db, temp.table, bound_alias)
            elif public_perm is not None:
                _qualify_table(table, *public_perm)

    def _overlay_info_schema_table(self, table: exp.Table, current_database: str | None) -> None:
        name = _ident_name(table.this)
        assert name in {"_FS_TABLES", "_FS_COLUMNS"}
        catalog = _ident_name(table.args.get("catalog")) or current_database
        if not catalog:
            return

        temp_view = TEMP_INFO_TABLES_VIEW if name == "_FS_TABLES" else TEMP_INFO_COLUMNS_VIEW
        alias = table.args.get("alias")
        perm_ref = table.copy()
        perm_ref.set("alias", None)
        catalog_lit = exp.Literal.string(catalog).sql(dialect="duckdb")
        union_sql = (
            f"SELECT * FROM {perm_ref.sql(dialect='duckdb')} "
            f"UNION ALL "
            f"SELECT * FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{temp_view} "
            f"WHERE TABLE_CATALOG = {catalog_lit}"
        )
        subquery = exp.Subquery(
            this=sqlglot.parse_one(union_sql, read="duckdb"),
            alias=alias or exp.TableAlias(this=exp.Identifier(this=name, quoted=False)),
        )
        table.replace(subquery)

    def commit(self, plan: PreparedStatement, *, in_transaction: bool) -> None:
        """Apply catalog and session-local metadata updates after DuckDB execution succeeds."""
        if plan.create_temp is not None:
            self._ensure_initialized()
            pending = plan.create_temp
            tbl = pending.table
            if pending.replaced is not None:
                self._remove_temp_record(pending.replaced, drop_physical=True)
            self._duck_conn.execute(
                f"""
                INSERT INTO {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE}
                    (temp_id, physical_name, database_name, schema_name, table_name,
                     owner_schema_oid, created_at, last_altered)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
                """,
                [tbl.temp_id, tbl.physical_name, tbl.database, tbl.schema, tbl.table, tbl.owner_schema_oid],
            )
            if pending.comment is not None:
                self._set_table_comment(tbl.physical_name, pending.comment)
            if pending.like_source is not None:
                self._duck_conn.execute(
                    f"""
                    INSERT INTO {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE}
                        (physical_name, column_name, character_maximum_length, character_octet_length)
                    SELECT ?, column_name, character_maximum_length, character_octet_length
                    FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE}
                    WHERE physical_name = ?
                    """,
                    [tbl.physical_name, pending.like_source.physical_name],
                )
            if pending.text_lengths:
                self._upsert_text_lengths(tbl.physical_name, pending.text_lengths)

        if plan.drop_temps:
            for dropped in plan.drop_temps:
                self._remove_temp_record(dropped, drop_physical=False)

        if plan.rename_temp is not None:
            ren = plan.rename_temp
            self._duck_conn.execute(
                f"""
                UPDATE {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE}
                SET database_name = ?, schema_name = ?, table_name = ?,
                    owner_schema_oid = ?, last_altered = CURRENT_TIMESTAMP
                WHERE temp_id = ?
                """,
                [ren.new_database, ren.new_schema, ren.new_table, ren.new_schema_oid, ren.table.temp_id],
            )

        if plan.altered_temp is not None:
            self._duck_conn.execute(
                f"""
                UPDATE {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE}
                SET last_altered = CURRENT_TIMESTAMP
                WHERE temp_id = ?
                """,
                [plan.altered_temp.temp_id],
            )
            if plan.altered_text_lengths:
                self._upsert_text_lengths(plan.altered_temp.physical_name, plan.altered_text_lengths)

        if plan.comment_temp is not None:
            tbl, comment = plan.comment_temp
            self._set_table_comment(tbl.physical_name, comment)
            self._duck_conn.execute(
                f"""
                UPDATE {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE}
                SET last_altered = CURRENT_TIMESTAMP
                WHERE temp_id = ?
                """,
                [tbl.temp_id],
            )

        if self._initialized and not in_transaction:
            self._prune_orphaned()

    def _set_table_comment(self, physical_name: str, comment: str) -> None:
        comment_lit = exp.Literal.string(comment).sql(dialect="duckdb")
        self._duck_conn.execute(f"COMMENT ON TABLE {TEMP_CATALOG}.{TEMP_SCHEMA}.{physical_name} IS {comment_lit}")

    def _upsert_text_lengths(self, physical_name: str, text_lengths: list[tuple[str, int]]) -> None:
        self._duck_conn.executemany(
            f"""
            INSERT INTO {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE}
                (physical_name, column_name, character_maximum_length, character_octet_length)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (physical_name, column_name)
            DO UPDATE SET
                character_maximum_length = excluded.character_maximum_length,
                character_octet_length = excluded.character_octet_length
            """,
            [(physical_name, col_name, size, min(size * 4, 16777216)) for col_name, size in text_lengths],
        )

    def _remove_temp_record(self, table: TempTable, *, drop_physical: bool) -> None:
        self._duck_conn.execute(
            f"DELETE FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} WHERE temp_id = ?",
            [table.temp_id],
        )
        self._duck_conn.execute(
            f"DELETE FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_COLUMNS_EXT_TABLE} WHERE physical_name = ?",
            [table.physical_name],
        )
        if drop_physical:
            self._duck_conn.execute(f"DROP TABLE IF EXISTS {TEMP_CATALOG}.{TEMP_SCHEMA}.{table.physical_name}")

    def _prune_orphaned(self) -> None:
        """Drop physical temp tables whose owning schema was dropped or replaced."""
        try:
            orphans = self._duck_conn.execute(
                f"""
                SELECT r.temp_id, r.physical_name, r.database_name, r.schema_name, r.table_name, r.owner_schema_oid
                FROM {TEMP_CATALOG}.{TEMP_SCHEMA}.{TEMP_CATALOG_TABLE} r
                LEFT JOIN duckdb_schemas() s
                  ON s.oid = r.owner_schema_oid
                WHERE s.oid IS NULL
                """
            ).fetchall()
            for row in orphans:
                self._remove_temp_record(TempTable(*row), drop_physical=True)
        except duckdb.TransactionException:
            # Never interfere with an active or aborted transaction; stale rows are already
            # hidden from reads and views by the duckdb_schemas() OID join.
            pass


def _ident_name(identifier: Expr | None) -> str | None:
    if not isinstance(identifier, exp.Identifier):
        return None
    return identifier.name if identifier.quoted else identifier.name.upper()


def _extract_target_table(node: Expr | None) -> exp.Table | None:
    if isinstance(node, exp.Schema):
        node = node.this
    return node if isinstance(node, exp.Table) else None


def _is_create_temp_table(expression: Expr) -> bool:
    return (
        isinstance(expression, exp.Create)
        and str(expression.args.get("kind") or "").upper() == "TABLE"
        and bool(expression.find(exp.TemporaryProperty))
        and not expression.args.get("merge_internal")
    )


def _is_create_perm_table(expression: Expr) -> bool:
    return (
        isinstance(expression, exp.Create)
        and str(expression.args.get("kind") or "").upper() == "TABLE"
        and not expression.find(exp.TemporaryProperty)
    )


def _is_info_schema_overlay_target(table: exp.Table) -> bool:
    return _ident_name(table.args.get("db")) == "_FS_INFORMATION_SCHEMA" and _ident_name(table.this) in {
        "_FS_TABLES",
        "_FS_COLUMNS",
    }


def _set_temp_target(table: exp.Table, physical_name: str) -> None:
    table.set("catalog", None)
    table.set("db", None)
    table.set("this", exp.Identifier(this=physical_name, quoted=False))


def _rewrite_to_temp(table: exp.Table, physical_name: str, *, alias: exp.TableAlias | None) -> None:
    table.set("catalog", exp.Identifier(this=TEMP_CATALOG, quoted=False))
    table.set("db", exp.Identifier(this=TEMP_SCHEMA, quoted=False))
    table.set("this", exp.Identifier(this=physical_name, quoted=False))
    table.set("alias", alias)


def _qualify_table(table: exp.Table, database: str, schema: str, name: str) -> None:
    table.set("catalog", exp.Identifier(this=database, quoted=True))
    table.set("db", exp.Identifier(this=schema, quoted=True))
    table.set("this", exp.Identifier(this=name, quoted=True))


def _strip_foreign_key_constraints(expression: exp.Create) -> None:
    for constraint in list(expression.find_all(exp.ColumnConstraint)):
        if isinstance(constraint.args.get("kind"), exp.Reference):
            constraint.pop()
    for fk in list(expression.find_all(exp.ForeignKey)):
        fk.pop()


def _strip_qualified_column_prefixes(
    root: Expr,
    orig_catalog: str | None,
    orig_db: str | None,
    logical_table: str,
    bound_alias: str,
) -> None:
    """Rewrite 2-/3-part column qualifiers (db.schema.t.col or schema.t.col) to bound_alias.col.

    Single-part qualifiers (t.col) already bind naturally via hygienic AS "<logical>" aliasing;
    only multi-part qualifiers or colliding scope aliases need rewriting.
    """
    for col in root.find_all(exp.Column):
        col_table = _ident_name(col.args.get("table"))
        if col_table != logical_table:
            continue
        col_db = _ident_name(col.args.get("db"))
        col_catalog = _ident_name(col.args.get("catalog"))
        if col_db is None and col_catalog is None:
            if bound_alias != logical_table:
                col.set("table", exp.Identifier(this=bound_alias, quoted=True))
            continue
        if orig_db is not None and col_db != orig_db:
            continue
        if orig_catalog is not None and col_catalog is not None and col_catalog != orig_catalog:
            continue
        col.set("catalog", None)
        col.set("db", None)
        col.set("table", exp.Identifier(this=bound_alias, quoted=True))

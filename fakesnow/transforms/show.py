from __future__ import annotations

from typing import Literal

import sqlglot
from sqlglot import exp


def fs_global_creation_sql() -> str:
    return f"""
        {SQL_CREATE_VIEW_SHOW_OBJECTS};
        {SQL_CREATE_VIEW_SHOW_TABLES};
        {SQL_CREATE_VIEW_SHOW_VIEWS};
        {SQL_CREATE_VIEW_SHOW_COLUMNS};
        {SQL_CREATE_VIEW_SHOW_DATABASES};
        {SQL_CREATE_VIEW_SHOW_FUNCTIONS};
        {SQL_CREATE_VIEW_SHOW_SCHEMAS};
        {SQL_CREATE_VIEW_SHOW_PROCEDURES};
    """


# see https://docs.snowflake.com/en/sql-reference/sql/show-columns
SQL_CREATE_VIEW_SHOW_COLUMNS = """
create view if not exists _fs_global._fs_information_schema._fs_show_columns as
SELECT
    table_name as "table_name",
    table_schema as "schema_name",
    column_name as "column_name",
    CASE
        WHEN data_type = 'NUMBER' THEN
            '{"type":"FIXED","precision":' || numeric_precision || ',"scale":' || numeric_scale || ',"nullable":true}'
        WHEN data_type = 'TEXT' THEN
            '{"type":"TEXT","length":' || coalesce(character_maximum_length,16777216)  || ',"byteLength":' ||
            CASE
                WHEN character_maximum_length = 16777216 THEN 16777216
                ELSE coalesce(character_maximum_length*4,16777216)
            END  || ',"nullable":true,"fixed":false}'
        WHEN data_type in ('TIMESTAMP_NTZ','TIMESTAMP_TZ','TIME') THEN
            '{"type":"' || data_type || '","precision":0,"scale":9,"nullable":true}'
        WHEN data_type = 'FLOAT' THEN '{"type":"REAL","nullable":true}'
        WHEN data_type = 'BINARY' THEN
            '{"type":"BINARY","length":8388608,"byteLength":8388608,"nullable":true,"fixed":true}'
        ELSE '{"type":"' || data_type || '","nullable":true}'
    END as "data_type",
    CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'false' END as "null?",
    COALESCE(column_default, '') as "default",
    'COLUMN' as "kind",
    '' as "expression",
    COALESCE(comment, '') as "comment",
    table_catalog as "database_name",
    '' as "autoincrement",
    NULL as "schema_evolution_record"
FROM _fs_global._fs_information_schema._fs_columns
ORDER BY table_catalog, table_schema, table_name, ordinal_position
"""


def show_columns(
    expression: exp.Expression, current_database: str | None, current_schema: str | None
) -> exp.Expression:
    """Transform SHOW COLUMNS to a query against the fs global information_schema columns table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-columns
    """
    if not (isinstance(expression, exp.Show) and expression.name.upper() == "COLUMNS"):
        return expression

    scope_kind = expression.args.get("scope_kind")
    table = expression.find(exp.Table)

    if scope_kind == "DATABASE" and table:
        catalog = table.name
        schema = None
        table = None
    elif scope_kind == "SCHEMA" and table:
        catalog = table.db or current_database
        schema = table.name
        table = None
    elif scope_kind in ("TABLE", "VIEW") and table:
        catalog = table.catalog or current_database
        schema = table.db or current_schema
        table = table.name
    elif scope_kind == "ACCOUNT":
        # all columns
        catalog = None
        schema = None
        table = None
    elif not scope_kind:
        # no explicit scope - show current database and schema only
        catalog = current_database
        schema = current_schema
        table = None
    else:
        raise NotImplementedError(f"show_object_columns: {expression.sql(dialect='snowflake')}")

    where = ["1=1"]
    if catalog:
        where.append(f"database_name = '{catalog}'")
    if schema:
        where.append(f"schema_name = '{schema}'")
    if table:
        where.append(f"table_name = '{table}'")
    where_clause = " AND ".join(where)

    query = f"""
    SELECT *
    FROM _fs_global._fs_information_schema._fs_show_columns
    WHERE {where_clause}
    """

    return sqlglot.parse_one(query, read="duckdb")


SQL_CREATE_VIEW_SHOW_DATABASES = """
create view if not exists _fs_global._fs_information_schema._fs_show_databases as
SELECT
    to_timestamp(0)::timestamptz as 'created_on',
    database_name as 'name',
    'N' as 'is_default',
    'N' as 'is_current',
    '' as 'origin',
    'SYSADMIN' as 'owner',
    comment,
    '' as 'options',
    1 as 'retention_time',
    'STANDARD' as 'kind',
    NULL as 'budget',
    'ROLE' as 'owner_role_type',
    NULL as 'object_visibility'
FROM duckdb_databases
WHERE database_name NOT IN ('memory', '_fs_global')
"""


def show_databases(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW DATABASES to a query against _fs_show_databases.

    See https://docs.snowflake.com/en/sql-reference/sql/show-databases
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "DATABASES":
        return sqlglot.parse_one("SELECT * FROM _fs_global._fs_information_schema._fs_show_databases", read="duckdb")

    return expression


SQL_CREATE_VIEW_SHOW_FUNCTIONS = """
create view if not exists _fs_global._fs_information_schema._fs_show_functions as
SELECT
    '1970-01-01 00:00:00 UTC'::timestamptz as created_on,
    'SYSTIMESTAMP' as name,
    '' as schema_name,
    'Y' as is_builtin,
    'N' as is_aggregate,
    'N' as is_ansi,
    0 as min_num_arguments,
    0 as max_num_arguments,
    'SYSTIMESTAMP() RETURN TIMESTAMP_LTZ' as arguments,
    'Returns the current timestamp' as description,
    '' as catalog_name,
    'N' as is_table_function,
    'N' as valid_for_clustering,
    NULL as is_secure,
    '' as secrets,
    '' as external_access_integrations,
    'N' as is_external_function,
    'SQL' as language,
    'N' as is_memoizable,
    'N' as is_data_metric
WHERE 0 = 1;
"""


def show_functions(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW FUNCTIONS.

    See https://docs.snowflake.com/en/sql-reference/sql/show-functions
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "FUNCTIONS":
        return sqlglot.parse_one("SELECT * FROM _fs_global._fs_information_schema._fs_show_functions", read="duckdb")

    return expression


def show_keys(
    expression: exp.Expression,
    current_database: str | None,
    *,
    kind: Literal["PRIMARY", "UNIQUE", "FOREIGN"],
) -> exp.Expression:
    """Transform SHOW <kind> KEYS to a query against the duckdb_constraints meta-table.

    https://docs.snowflake.com/en/sql-reference/sql/show-primary-keys
    """
    snowflake_kind = kind
    if kind == "FOREIGN":
        snowflake_kind = "IMPORTED"

    if isinstance(expression, exp.Show) and expression.name.upper() == f"{snowflake_kind} KEYS":
        if kind == "FOREIGN":
            statement = f"""
                SELECT
                    to_timestamp(0)::timestamptz as created_on,

                    '' as pk_database_name,
                    '' as pk_schema_name,
                    '' as pk_table_name,
                    '' as pk_column_name,
                    unnest(constraint_column_names) as pk_column_name,

                    database_name as fk_database_name,
                    schema_name as fk_schema_name,
                    table_name as fk_table_name,
                    unnest(constraint_column_names) as fk_column_name,
                    1 as key_sequence,
                    'NO ACTION' as update_rule,
                    'NO ACTION' as delete_rule,
                    LOWER(CONCAT(database_name, '_', schema_name, '_', table_name, '_pkey')) AS fk_name,
                    LOWER(CONCAT(database_name, '_', schema_name, '_', table_name, '_pkey')) AS pk_name,
                    'NOT DEFERRABLE' as deferrability,
                    'false' as rely,
                    null as "comment"
                FROM duckdb_constraints
                WHERE constraint_type = 'PRIMARY KEY'
                  AND database_name = '{current_database}'
                  AND table_name NOT LIKE '_fs_%'
                """
        else:
            statement = f"""
                SELECT
                    to_timestamp(0)::timestamptz as created_on,
                    database_name as database_name,
                    schema_name as schema_name,
                    table_name as table_name,
                    unnest(constraint_column_names) as column_name,
                    1 as key_sequence,
                    LOWER(CONCAT(database_name, '_', schema_name, '_', table_name, '_pkey')) AS constraint_name,
                    'false' as rely,
                    null as "comment"
                FROM duckdb_constraints
                WHERE constraint_type = '{kind} KEY'
                  AND database_name = '{current_database}'
                  AND table_name NOT LIKE '_fs_%'
                """

        if scope_kind := expression.args.get("scope_kind"):
            table = expression.args["scope"]

            if scope_kind == "SCHEMA":
                db = table and table.db
                schema = table and table.name
                if db:
                    statement += f"AND database_name = '{db}' "

                if schema:
                    statement += f"AND schema_name = '{schema}' "
            elif scope_kind == "TABLE":
                if not table:
                    raise ValueError(f"SHOW PRIMARY KEYS with {scope_kind} scope requires a table")

                statement += f"AND table_name = '{table.name}' "
            else:
                raise NotImplementedError(f"SHOW PRIMARY KEYS with {scope_kind}")
        return sqlglot.parse_one(statement)
    return expression


SQL_CREATE_VIEW_SHOW_PROCEDURES = """
create view if not exists _fs_global._fs_information_schema._fs_show_procedures as
SELECT
    '2012-08-01 07:00:00 UTC'::timestamptz as 'created_on',
    'SYSTEM$CLASSIFY' as 'name',
    '' as 'schema_name',
    'Y' as 'is_builtin',
    'N' as 'is_aggregate',
    'N' as 'is_ansi',
    2 as 'min_num_arguments',
    2 as 'max_num_arguments',
    'SYSTEM$CLASSIFY(VARCHAR, OBJECT) RETURN OBJECT' as 'arguments',
    'classify stored proc' as 'description',
    '' as 'catalog_name',
    'N' as 'is_table_function',
    'N' as 'valid_for_clustering',
    NULL as 'is_secure',
    '' as 'secrets',
    '' as 'external_access_integrations',
WHERE 0 = 1;
"""


def show_procedures(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW PROCEDURES.

    See https://docs.snowflake.com/en/sql-reference/sql/show-procedures
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "PROCEDURES":
        return sqlglot.parse_one(
            "SELECT * FROM _fs_global._fs_information_schema._fs_show_procedures",
            read="duckdb",
        )

    return expression


SQL_CREATE_VIEW_SHOW_SCHEMAS = """
create view if not exists _fs_global._fs_information_schema._fs_show_schemas as
select
    to_timestamp(0)::timestamptz as 'created_on',
    case
        when schema_name = '_fs_information_schema' then 'information_schema'
        else schema_name
    end as 'name',
    NULL as 'kind',
    catalog_name as 'database_name',
    NULL as 'schema_name'
from information_schema.schemata
where not catalog_name in ('memory', 'system', 'temp', '_fs_global')
  and not schema_name in ('main', 'pg_catalog')
"""


def show_schemas(expression: exp.Expression, current_database: str | None) -> exp.Expression:
    """Transform SHOW SCHEMAS to a query against the _fs_show_schemas view.

    See https://docs.snowflake.com/en/sql-reference/sql/show-schemas
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "SCHEMAS":
        if (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            database = ident.this
        else:
            database = current_database

        query = "SELECT * FROM _fs_global._fs_information_schema._fs_show_schemas"

        if database:
            query += f" WHERE database_name = '{database}'"
        return sqlglot.parse_one(query, read="duckdb")

    return expression


def show_stages(expression: exp.Expression, current_database: str | None, current_schema: str | None) -> exp.Expression:
    """Transform SHOW STAGES to a select from the fake _fs_stages table."""
    if not (isinstance(expression, exp.Show) and expression.name.upper() == "STAGES"):
        return expression

    scope_kind = expression.args.get("scope_kind")
    table = expression.find(exp.Table)

    if scope_kind == "DATABASE":
        catalog = (table and table.name) or current_database
        schema = None
    elif scope_kind == "SCHEMA" and table:
        catalog = table.db or current_database
        schema = table.name
    elif scope_kind == "TABLE" and table:
        catalog = table.db or current_database
        assert isinstance(table.this, exp.Identifier)
        schema = table.this.this
    elif scope_kind == "ACCOUNT":
        # show all objects / tables in the account
        catalog = None
        schema = None
    else:
        # no explicit scope - show current database and schema only
        catalog = current_database
        schema = current_schema

    where = ["1=1"]
    if catalog:
        where.append(f"database_name = '{catalog}'")
    if schema:
        where.append(f"schema_name = '{schema}'")
    where_clause = " AND ".join(where)

    query = f"""
        SELECT *
        from _fs_global._fs_information_schema._fs_stages
        where {where_clause}
    """

    return sqlglot.parse_one(query, read="duckdb")


# see https://docs.snowflake.com/en/sql-reference/sql/show-objects
SQL_CREATE_VIEW_SHOW_OBJECTS = """
create view if not exists _fs_global._fs_information_schema._fs_show_objects as
select
    to_timestamp(0)::timestamptz as created_on,
    table_name as name,
    table_catalog as database_name,
    table_schema as schema_name,
    case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind',
    '' as comment,
    '' as cluster_by,
    -- TODO: implement rows and bytes as rows * 1024
    0 as rows,
    0 as bytes,
    'SYSADMIN' as owner,
    1 as retention_time,
    'ROLE' as owner_role_type,
    'N' as is_hybrid,
    'N' as is_dynamic,
    'N' as is_iceberg
from information_schema.tables
where not (table_schema == '_fs_information_schema')
"""

# see https://docs.snowflake.com/en/sql-reference/sql/show-tables
SQL_CREATE_VIEW_SHOW_TABLES = """
create view if not exists _fs_global._fs_information_schema._fs_show_tables as
select
    to_timestamp(0)::timestamptz as created_on,
    table_name as name,
    'TABLE' as kind,
    table_catalog as database_name,
    table_schema as schema_name,
    '' as comment,
    '' as cluster_by,
    -- TODO: implement rows and bytes as rows * 1024
    0 as rows,
    0 as bytes,
    'SYSADMIN' as owner,
    1 as retention_time,
    'OFF' as automatic_clustering,
    'OFF' as change_tracking,
    'OFF' as search_optimization,
    null as search_optimization_progress,
    null as search_optimization_bytes,
    'N' as is_external,
    'N' as enable_schema_evolution,
    'ROLE' as owner_role_type,
    'N' as is_event,
    null as budget,
    'N' as is_hybrid,
    'N' as is_iceberg,
    'N' as is_dynamic,
    'N' as is_immutable
from information_schema.tables
where not (table_schema == '_fs_information_schema')
and table_type = 'BASE TABLE'
"""

# see https://docs.snowflake.com/en/sql-reference/sql/show-views
SQL_CREATE_VIEW_SHOW_VIEWS = """
create view if not exists _fs_global._fs_information_schema._fs_show_views as
select
    to_timestamp(0)::timestamptz as created_on,
    table_name as name,
    '' as reserved,
    table_catalog as database_name,
    table_schema as schema_name,
    'SYSADMIN' as owner,
    '' as comment,
    view_definition as text,
    false as is_secure,
    false as is_materialized,
    'ROLE' as owner_role_type,
    'OFF' as change_tracking
from information_schema.views
where not table_catalog in ('system')
  and not table_schema in ('main', '_fs_information_schema')
"""


def show_tables_etc(
    expression: exp.Expression, current_database: str | None, current_schema: str | None
) -> exp.Expression:
    """Transform SHOW OBJECTS/TABLES/VIEWS to a query against the _fs_information_schema views."""
    if not (
        isinstance(expression, exp.Show)
        and (show := expression.name.upper())
        and show in {"OBJECTS", "TABLES", "VIEWS"}
    ):
        return expression

    scope_kind = expression.args.get("scope_kind")
    table = expression.find(exp.Table)

    if scope_kind == "DATABASE":
        catalog = (table and table.name) or current_database
        schema = None
    elif scope_kind == "SCHEMA" and table:
        catalog = table.db or current_database
        schema = table.name
    elif scope_kind == "ACCOUNT":
        # show all objects / tables in the account
        catalog = None
        schema = None
    else:
        # no explicit scope - show current database and schema only
        catalog = current_database
        schema = current_schema

    if expression.args["terse"] and show == "VIEWS":
        columns = ["created_on, name, 'VIEW' as kind, database_name, schema_name"]
    elif expression.args["terse"]:
        columns = ["created_on, name, kind, database_name, schema_name"]
    else:
        columns = ["*"]
    columns_clause = ", ".join(columns)

    where = ["1=1"]
    if catalog:
        where.append(f"database_name = '{catalog}'")
    if schema:
        where.append(f"schema_name = '{schema}'")
    if (like := expression.args.get("like")) and isinstance(like, exp.Expression):
        where.append(f"name ilike {like.sql()}")
    where_clause = " AND ".join(where)

    limit = limit.sql() if (limit := expression.args.get("limit")) and isinstance(limit, exp.Expression) else ""

    query = f"""
        SELECT {columns_clause}
        from _fs_global._fs_information_schema._fs_show_{show.lower()}
        where {where_clause}
        {limit}
    """

    return sqlglot.parse_one(query, read="duckdb")


def show_users(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW USERS to a query against the global database's information_schema._fs_users table.

    https://docs.snowflake.com/en/sql-reference/sql/show-users
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "USERS":
        return sqlglot.parse_one("SELECT * FROM _fs_global._fs_information_schema._fs_users", read="duckdb")

    return expression


# returns zero rows
SQL_SHOW_WAREHOUSES = """
SELECT
    'FAKESNOW_WAREHOUSE' as name,
    'STARTED' as state,
    'STANDARD' as type,
    'X-Small' as size,
    1 as min_cluster_count,
    1 as max_cluster_count,
    1 as started_clusters,
    0 as running,
    0 as queued,
    'N' as is_default,
    'N' as is_current,
    600 as auto_suspend,
    'true' as auto_resume,
    -- nb: deliberate space before '100' to match Snowflake's output
    ' 100' as available,
    '0' as provisioning,
    '0' as quiescing,
    '0' as other,
    '1970-01-01 00:00:00.000000 UTC'::timestamptz as created_on,
    '1970-01-01 00:00:00.000000 UTC'::timestamptz as resumed_on,
    '1970-01-01 00:00:00.000000 UTC'::timestamptz as updated_on,
    'SYSADMIN' as owner,
    '' as comment,
    'false' as enable_query_acceleration,
    8 as query_acceleration_max_scale_factor,
    'null' as resource_monitor,

    -- deprecated - these 5 cols are for internal use
    0 as actives,
    0 as pendings,
    0 as failed,
    0 as suspended,
    '123456789012' as uuid,

    'STANDARD' as scaling_policy,
    NULL as budget,
    'ROLE' as owner_role_type,
    NULL as resource_constraint;
"""


def show_warehouses(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW WAREHOUSES.

    See https://docs.snowflake.com/en/sql-reference/sql/show-warehouses
    """
    if isinstance(expression, exp.Show) and expression.name.upper() == "WAREHOUSES":
        return sqlglot.parse_one(SQL_SHOW_WAREHOUSES, read="duckdb")

    return expression

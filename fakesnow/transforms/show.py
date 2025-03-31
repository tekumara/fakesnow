from __future__ import annotations

from typing import Literal

import sqlglot
from sqlglot import exp


def show_columns(
    expression: exp.Expression, current_database: str | None = None, current_schema: str | None = None
) -> exp.Expression:
    """Transform SHOW COLUMNS to a query against the fs global information_schema columns table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-columns
    """
    if not (
        isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "COLUMNS"
    ):
        return expression

    scope_kind = expression.args.get("scope_kind")
    table = expression.find(exp.Table)

    if scope_kind == "ACCOUNT" or not scope_kind:
        # all columns
        catalog = None
        schema = None
        table = None
    elif scope_kind == "DATABASE" and table:
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
    else:
        raise NotImplementedError(f"show_object_columns: {expression.sql(dialect='snowflake')}")

    query = f"""
    SELECT
        table_name,
        table_schema as "schema_name",
        column_name,
        CASE
            WHEN data_type = 'NUMBER' THEN '{{"type":"FIXED","precision":'|| numeric_precision || ',"scale":' || numeric_scale || ',"nullable":true}}'
            WHEN data_type = 'TEXT' THEN '{{"type":"TEXT","length":' || coalesce(character_maximum_length,16777216)  || ',"byteLength":' || CASE WHEN character_maximum_length = 16777216 THEN 16777216 ELSE coalesce(character_maximum_length*4,16777216) END  || ',"nullable":true,"fixed":false}}'
            WHEN data_type in ('TIMESTAMP_NTZ','TIMESTAMP_TZ','TIME') THEN '{{"type":"' || data_type || '","precision":0,"scale":9,"nullable":true}}'
            WHEN data_type = 'FLOAT' THEN '{{"type":"REAL","nullable":true}}'
            WHEN data_type = 'BINARY' THEN '{{"type":"BINARY","length":8388608,"byteLength":8388608,"nullable":true,"fixed":true}}'
            ELSE '{{"type":"' || data_type || '","nullable":true}}'
        END as "data_type",
        CASE WHEN is_nullable = 'YES' THEN 'true' ELSE 'false' END as "null?",
        COALESCE(column_default, '') as "default",
        'COLUMN' as "kind",
        '' as "expression",
        COALESCE(comment, '') as "comment",
        table_catalog as "database_name",
        '' as "autoincrement",
        NULL as "schema_evolution_record"
    FROM _fs_global._fs_information_schema._fs_columns c
    WHERE 1=1
    {f"AND table_catalog = '{catalog}'" if catalog else ""}
    {f"AND table_schema = '{schema}'" if schema else ""}
    {f"AND table_name = '{table}'" if table else ""}
    ORDER BY table_name, ordinal_position
    """  # noqa: E501

    return sqlglot.parse_one(query, read="duckdb")


SQL_SHOW_DATABASES = """
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
    """Transform SHOW DATABASES to a query against the information_schema.schemata table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-databases
    """
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "DATABASES":
        return sqlglot.parse_one(SQL_SHOW_DATABASES, read="duckdb")

    return expression


# returns zero rows
SQL_SHOW_FUNCTIONS = """
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
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "FUNCTIONS":
        return sqlglot.parse_one(SQL_SHOW_FUNCTIONS, read="duckdb")

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
    if (
        isinstance(expression, exp.Show)
        and isinstance(expression.this, str)
        and expression.this.upper() == "WAREHOUSES"
    ):
        return sqlglot.parse_one(SQL_SHOW_WAREHOUSES, read="duckdb")

    return expression


def show_keys(
    expression: exp.Expression,
    current_database: str | None = None,
    *,
    kind: Literal["PRIMARY", "UNIQUE", "FOREIGN"],
) -> exp.Expression:
    """Transform SHOW <kind> KEYS to a query against the duckdb_constraints meta-table.

    https://docs.snowflake.com/en/sql-reference/sql/show-primary-keys
    """
    snowflake_kind = kind
    if kind == "FOREIGN":
        snowflake_kind = "IMPORTED"

    if (
        isinstance(expression, exp.Show)
        and isinstance(expression.this, str)
        and expression.this.upper() == f"{snowflake_kind} KEYS"
    ):
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
                raise NotImplementedError(f"SHOW PRIMARY KEYS with {scope_kind} not yet supported")
        return sqlglot.parse_one(statement)
    return expression


def show_objects_tables(expression: exp.Expression, current_database: str | None = None) -> exp.Expression:
    """Transform SHOW OBJECTS/TABLES to a query against the information_schema.tables table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-objects
        https://docs.snowflake.com/en/sql-reference/sql/show-tables
    """
    if not (
        isinstance(expression, exp.Show)
        and isinstance(expression.this, str)
        and (show := expression.this.upper())
        and show in {"OBJECTS", "TABLES"}
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
    else:
        # all objects / tables - will show everything in the "account"
        catalog = None
        schema = None

    columns = [
        "to_timestamp(0)::timestamptz as 'created_on'",
        "table_name as 'name'",
        "case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind'",
        "table_catalog as 'database_name'",
        "table_schema as 'schema_name'",
    ]
    if not expression.args["terse"]:
        if show == "OBJECTS":
            columns.extend(
                [
                    "'' as 'comment'",
                    "'' as 'cluster_by'",
                    # TODO: implement rows and bytes as rows * 1024
                    "0 as 'rows'",
                    "0 as 'bytes'",
                    "'SYSADMIN' as 'owner'",
                    "1 as 'retention_time'",
                    "'ROLE' as 'owner_role_type'",
                    "null as 'budget'",
                    "'N' as 'is_hybrid'",
                    "'N' as 'is_dynamic'",
                ]
            )
        else:
            # show == "TABLES"
            columns.extend(
                [
                    "'' as 'comment'",
                    "'' as 'cluster_by'",
                    # TODO: implement rows and bytes as rows * 1024
                    "0 as 'rows'",
                    "0 as 'bytes'",
                    "'SYSADMIN' as 'owner'",
                    "1 as 'retention_time'",
                    "'OFF' as 'automatic_clustering'",
                    "'OFF' as 'change_tracking'",
                    "'OFF' as 'search_optimization'",
                    "null as 'search_optimization_progress'",
                    "null as 'search_optimization_bytes'",
                    "'N' as 'is_external'",
                    "'N' as 'enable_schema_evolution'",
                    "'ROLE' as 'owner_role_type'",
                    "'N' as 'is_event'",
                    "null as 'budget'",
                    "'N' as 'is_hybrid'",
                    "'N' as 'is_iceberg'",
                    "'N' as 'is_dynamic'",
                    "'N' as 'is_immutable'",
                ]
            )

    columns_clause = ", ".join(columns)

    where = ["not (table_schema == '_fs_information_schema')"]  # exclude fakesnow's internal schemas
    if show == "TABLES":
        where.append("table_type = 'BASE TABLE'")
    if catalog:
        where.append(f"table_catalog = '{catalog}'")
    if schema:
        where.append(f"table_schema = '{schema}'")
    if (like := expression.args.get("like")) and isinstance(like, exp.Expression):
        where.append(f"table_name ilike {like.sql()}")
    where_clause = " AND ".join(where)

    limit = limit.sql() if (limit := expression.args.get("limit")) and isinstance(limit, exp.Expression) else ""

    query = f"""
        SELECT {columns_clause}
        from information_schema.tables
        where {where_clause}
        {limit}
    """

    return sqlglot.parse_one(query, read="duckdb")


# returns zero rows
SQL_SHOW_PROCEDURES = """
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
    if (
        isinstance(expression, exp.Show)
        and isinstance(expression.this, str)
        and expression.this.upper() == "PROCEDURES"
    ):
        return sqlglot.parse_one(SQL_SHOW_PROCEDURES, read="duckdb")

    return expression


SQL_SHOW_SCHEMAS = """
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


def show_schemas(expression: exp.Expression, current_database: str | None = None) -> exp.Expression:
    """Transform SHOW SCHEMAS to a query against the information_schema.schemata table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-schemas
    """
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "SCHEMAS":
        if (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            database = ident.this
        else:
            database = current_database

        return sqlglot.parse_one(
            f"{SQL_SHOW_SCHEMAS} and catalog_name = '{database}'" if database else SQL_SHOW_SCHEMAS, read="duckdb"
        )

    return expression


def show_users(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW USERS to a query against the global database's information_schema._fs_users table.

    https://docs.snowflake.com/en/sql-reference/sql/show-users
    """
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "USERS":
        return sqlglot.parse_one("SELECT * FROM _fs_global._fs_information_schema._fs_users_ext", read="duckdb")

    return expression

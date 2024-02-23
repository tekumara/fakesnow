"""Info schema extension tables/views used for storing snowflake metadata not captured by duckdb."""
from __future__ import annotations

from string import Template

# use ext prefix in columns to disambiguate when joining with information_schema.tables
SQL_CREATE_INFORMATION_SCHEMA_TABLES_EXT = Template(
    """
create table if not exists ${catalog}.information_schema._fs_tables_ext (
    ext_table_catalog varchar,
    ext_table_schema varchar,
    ext_table_name varchar,
    comment varchar,
    PRIMARY KEY(ext_table_catalog, ext_table_schema, ext_table_name)
)
"""
)

SQL_CREATE_INFORMATION_SCHEMA_COLUMNS_EXT = Template(
    """
create table if not exists ${catalog}.information_schema._fs_columns_ext (
    ext_table_catalog varchar,
    ext_table_schema varchar,
    ext_table_name varchar,
    ext_column_name varchar,
    ext_character_maximum_length integer,
    ext_character_octet_length integer,
    PRIMARY KEY(ext_table_catalog, ext_table_schema, ext_table_name, ext_column_name)
)
"""
)

# only include fields applicable to snowflake (as mentioned by describe table information_schema.columns)
# snowflake integers are 38 digits, base 10, See https://docs.snowflake.com/en/sql-reference/data-types-numeric
SQL_CREATE_INFORMATION_SCHEMA_COLUMNS_VIEW = Template(
    """
create view if not exists ${catalog}.information_schema._fs_columns_snowflake AS
select
    columns.table_catalog AS table_catalog,
    columns.table_schema AS table_schema,
    columns.table_name AS table_name,
    columns.column_name AS column_name,
    columns.ordinal_position AS ordinal_position,
    columns.column_default AS column_default,
    columns.is_nullable AS is_nullable,
case when starts_with(columns.data_type, 'DECIMAL') or columns.data_type='BIGINT' then 'NUMBER'
     when columns.data_type='VARCHAR' then 'TEXT'
     when columns.data_type='DOUBLE' then 'FLOAT'
     when columns.data_type='BLOB' then 'BINARY'
     when columns.data_type='TIMESTAMP' then 'TIMESTAMP_NTZ'
     when columns.data_type='TIMESTAMP WITH TIME ZONE' then 'TIMESTAMP_TZ'
     when columns.data_type='JSON' then 'VARIANT'
     else columns.data_type end as data_type,
ext_character_maximum_length as character_maximum_length, ext_character_octet_length as character_octet_length,
case when columns.data_type='BIGINT' then 38
     when columns.data_type='DOUBLE' then NULL
    else columns.numeric_precision end as numeric_precision,
case when columns.data_type='BIGINT' then 10
    when columns.data_type='DOUBLE' then NULL
    else columns.numeric_precision_radix end as numeric_precision_radix,
case when columns.data_type='DOUBLE' then NULL else columns.numeric_scale end as numeric_scale,
collation_name, is_identity, identity_generation, identity_cycle,
    ddb_columns.comment as comment,
    null as identity_start,
    null as identity_increment,
from ${catalog}.information_schema.columns columns
left join ${catalog}.information_schema._fs_columns_ext ext
  on ext_table_catalog = columns.table_catalog
 AND ext_table_schema = columns.table_schema
 AND ext_table_name = columns.table_name
 AND ext_column_name = columns.column_name
LEFT JOIN duckdb_columns ddb_columns
  ON ddb_columns.database_name = columns.table_catalog
 AND ddb_columns.schema_name = columns.table_schema
 AND ddb_columns.table_name = columns.table_name
 AND ddb_columns.column_name = columns.column_name
"""
)

# replicates https://docs.snowflake.com/sql-reference/info-schema/databases
SQL_CREATE_INFORMATION_SCHEMA_DATABASES_VIEW = Template(
    """
create view if not exists ${catalog}.information_schema.databases AS
select
    catalog_name as database_name,
    'SYSADMIN' as database_owner,
    'NO' as is_transient,
    null as comment,
    to_timestamp(0)::timestamptz as created,
    to_timestamp(0)::timestamptz as last_altered,
    1 as retention_time,
    'STANDARD' as type
from information_schema.schemata
where catalog_name not in ('memory', 'system', 'temp', '_fs_global')
  and schema_name = 'information_schema'
"""
)


# replicates https://docs.snowflake.com/sql-reference/info-schema/views
SQL_CREATE_INFORMATION_SCHEMA_VIEWS_VIEW = Template(
    """
create view if not exists ${catalog}.information_schema.views AS
select
    database_name as table_catalog,
    schema_name as table_schema,
    view_name as table_name,
    'SYSADMIN' as table_owner,
    sql as view_definition,
    'NONE' as check_option,
    'NO' as is_updatable,
    'NO' as insertable_into,
    'NO' as is_secure,
    to_timestamp(0)::timestamptz as created,
    to_timestamp(0)::timestamptz as last_altered,
    to_timestamp(0)::timestamptz as last_ddl,
    'SYSADMIN' as last_ddl_by,
    null as comment
from duckdb_views
where database_name = '${catalog}'
  and schema_name != 'information_schema'
"""
)


def creation_sql(catalog: str) -> str:
    return f"""
        {SQL_CREATE_INFORMATION_SCHEMA_TABLES_EXT.substitute(catalog=catalog)};
        {SQL_CREATE_INFORMATION_SCHEMA_COLUMNS_EXT.substitute(catalog=catalog)};
        {SQL_CREATE_INFORMATION_SCHEMA_COLUMNS_VIEW.substitute(catalog=catalog)};
        {SQL_CREATE_INFORMATION_SCHEMA_DATABASES_VIEW.substitute(catalog=catalog)};
        {SQL_CREATE_INFORMATION_SCHEMA_VIEWS_VIEW.substitute(catalog=catalog)};
    """


def insert_table_comment_sql(catalog: str, schema: str, table: str, comment: str) -> str:
    return f"""
        INSERT INTO {catalog}.information_schema._fs_tables_ext
        values ('{catalog}', '{schema}', '{table}', '{comment}')
        ON CONFLICT (ext_table_catalog, ext_table_schema, ext_table_name)
        DO UPDATE SET comment = excluded.comment
    """


def insert_text_lengths_sql(catalog: str, schema: str, table: str, text_lengths: list[tuple[str, int]]) -> str:
    values = ", ".join(
        f"('{catalog}', '{schema}', '{table}', '{col_name}', {size}, {min(size*4,16777216)})"
        for (col_name, size) in text_lengths
    )

    return f"""
        INSERT INTO {catalog}.information_schema._fs_columns_ext
        values {values}
        ON CONFLICT (ext_table_catalog, ext_table_schema, ext_table_name, ext_column_name)
        DO UPDATE SET ext_character_maximum_length = excluded.ext_character_maximum_length,
            ext_character_octet_length = excluded.ext_character_octet_length
    """

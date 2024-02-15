from duckdb import DuckDBPyConnection

GLOBAL_DATABASE_NAME = "_fs_global"
USERS_TABLE_FQ_NAME = f"{GLOBAL_DATABASE_NAME}._fs_users_ext"

# replicates the output structure of https://docs.snowflake.com/en/sql-reference/sql/show-users
SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT = f"""
create table if not exists {USERS_TABLE_FQ_NAME} (
    name varchar,
    created_on TIMESTAMPTZ,
    login_name varchar,
    display_name varchar,
    first_name varchar,
    last_name varchar,
    email varchar,
    mins_to_unlock varchar,
    days_to_expiry varchar,
    comment varchar,
    disabled varchar,
    must_change_password varchar,
    snowflake_lock varchar,
    default_warehouse varchar,
    default_namespace varchar,
    default_role varchar,
    default_secondary_roles varchar,
    ext_authn_duo varchar,
    ext_authn_uid varchar,
    mins_to_bypass_mfa varchar,
    owner varchar,
    last_success_login TIMESTAMPTZ,
    expires_at_time TIMESTAMPTZ,
    locked_until_time TIMESTAMPTZ,
    has_password varchar,
    has_rsa_public_key varchar,
)
"""


def create_global_database(conn: DuckDBPyConnection) -> None:
    """Create a "global" database for storing objects which span database.

    Including (but not limited to):
     - Users
    """
    conn.execute(f"ATTACH IF NOT EXISTS ':memory:' AS {GLOBAL_DATABASE_NAME}")
    conn.execute(SQL_CREATE_INFORMATION_SCHEMA_USERS_TABLE_EXT)

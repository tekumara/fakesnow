from __future__ import annotations

import datetime
import tempfile
from collections.abc import Sequence
from typing import Any
from urllib.parse import urlparse
from urllib.request import url2pathname

import snowflake.connector.errors
import sqlglot
from sqlglot import exp

# TODO: clean up temp files on exit
LOCAL_BUCKET_PATH = tempfile.mkdtemp(prefix="fakesnow_bucket_")


def create_stage(
    expression: exp.Expression,
    current_database: str | None,
    current_schema: str | None,
    params: Sequence[Any] | dict[Any, Any] | None,
) -> exp.Expression:
    """Transform CREATE STAGE to an INSERT statement for the fake stages table."""
    if not (
        isinstance(expression, exp.Create)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, str)
        and kind.upper() == "STAGE"
        and (table := expression.find(exp.Table))
    ):
        return expression

    ident = table.this
    if isinstance(ident, exp.Placeholder):
        if not (isinstance(params, Sequence) and len(params) == 1):
            raise NotImplementedError("PUT requires a single parameter for the stage name")
        var = params[0]
        catalog, schema, stage_name = parts_from_var(
            var, current_database=current_database, current_schema=current_schema
        )
    elif isinstance(ident, exp.Identifier):
        catalog = table.catalog or current_database
        schema = table.db or current_schema
        stage_name = ident.this if ident.quoted else ident.this.upper()
    else:
        raise ValueError(f"Invalid identifier type {ident.__class__.__name__} for stage name")
    now = datetime.datetime.now(datetime.timezone.utc).isoformat()

    is_temp = False
    url = ""
    properties = expression.args.get("properties") or []
    for prop in properties:
        if isinstance(prop, exp.TemporaryProperty):
            is_temp = True
        elif (
            isinstance(prop, exp.Property)
            and isinstance(prop.this, exp.Var)
            and isinstance(prop.this.this, str)
            and prop.this.this.upper() == "URL"
        ):
            value = prop.args.get("value")
            if isinstance(value, exp.Literal):
                url = value.this

    # Determine cloud provider based on url
    cloud = "AWS" if url.startswith("s3://") else None

    stage_type = ("EXTERNAL" if url else "INTERNAL") + (" TEMPORARY" if is_temp else "")

    insert_sql = f"""
        INSERT INTO _fs_global._fs_information_schema._fs_stages
        (created_on, name, database_name, schema_name, url, has_credentials, has_encryption_key, owner,
        comment, region, type, cloud, notification_channel, storage_integration, endpoint, owner_role_type,
        directory_enabled)
        VALUES (
            '{now}', '{stage_name}', '{catalog}', '{schema}', '{url}', 'N', 'N', 'SYSADMIN',
            '', NULL, '{stage_type}', {f"'{cloud}'" if cloud else "NULL"}, NULL, NULL, NULL, 'ROLE',
            'N'
        )
        """
    transformed = sqlglot.parse_one(insert_sql, read="duckdb")
    transformed.args["create_stage_name"] = stage_name
    transformed.args["consumed_params"] = isinstance(ident, exp.Placeholder)
    return transformed


def list_stage(expression: exp.Expression, current_database: str | None, current_schema: str | None) -> exp.Expression:
    """Transform LIST to list file system operation.

    See https://docs.snowflake.com/en/sql-reference/sql/list
    """
    if not (
        isinstance(expression, exp.Alias)
        and isinstance(expression.this, exp.Column)
        and isinstance(expression.this.this, exp.Identifier)
        and isinstance(expression.this.this.this, str)
        and expression.this.this.this.upper() == "LIST"
    ):
        return expression

    stage = expression.args["alias"].this
    if not isinstance(stage, exp.Var):
        raise ValueError(f"LIST command requires a stage name as a Var, got {stage}")

    var = stage.text("this")
    catalog, schema, stage_name = parts_from_var(var, current_database=current_database, current_schema=current_schema)

    query = f"""
        SELECT *
        from _fs_global._fs_information_schema._fs_stages
        where database_name = '{catalog}' and schema_name = '{schema}' and name = '{stage_name}'
    """

    transformed = sqlglot.parse_one(query, read="duckdb")
    transformed.args["list_stage_name"] = f"{catalog}.{schema}.{stage_name}"
    return transformed


def put_stage(
    expression: exp.Expression,
    current_database: str | None,
    current_schema: str | None,
    params: Sequence[Any] | dict[Any, Any] | None,
) -> exp.Expression:
    """Transform PUT to a SELECT statement to locate the stage.

    See https://docs.snowflake.com/en/sql-reference/sql/put
    """
    if not isinstance(expression, exp.Put):
        return expression

    assert isinstance(expression.this, exp.Literal), "PUT command requires a file path as a literal"
    src_url = urlparse(expression.this.this)
    src_path = url2pathname(src_url.path)
    target = expression.args["target"]

    assert isinstance(target, exp.Var), f"{target} is not a exp.Var"
    this = target.text("this")
    if this == "?":
        if not (isinstance(params, Sequence) and len(params) == 1):
            raise NotImplementedError("PUT requires a single parameter for the stage name")
        this = params[0]
    if not this.startswith("@"):
        msg = f"SQL compilation error:\n{this} does not start with @"
        raise snowflake.connector.errors.ProgrammingError(
            msg=msg,
            errno=1003,
            sqlstate="42000",
        )
    # strip leading @
    var = this[1:]
    catalog, schema, stage_name = parts_from_var(var, current_database=current_database, current_schema=current_schema)

    query = f"""
        SELECT *
        from _fs_global._fs_information_schema._fs_stages
        where database_name = '{catalog}' and schema_name = '{schema}' and name = '{stage_name}'
    """

    transformed = sqlglot.parse_one(query, read="duckdb")
    fqname = f"{catalog}.{schema}.{stage_name}"
    transformed.args["put_stage_name"] = fqname
    transformed.args["put_stage_data"] = {
        "stageInfo": {
            # use LOCAL_FS otherwise we need to mock S3 with HTTPS which requires a certificate
            "locationType": "LOCAL_FS",
            "location": local_dir(fqname),
            "creds": {},
        },
        "src_locations": [src_path],
        # defaults as per https://docs.snowflake.com/en/sql-reference/sql/put TODO: support other values
        "parallel": 4,
        "autoCompress": True,
        "sourceCompression": "auto_detect",
        "overwrite": False,
        "command": "UPLOAD",
    }
    transformed.args["consumed_params"] = target.text("this") == "?"

    return transformed


def normalise_ident(name: str) -> str:
    """
    Strip double quotes if present else return uppercased.
    Snowflake treats quoted identifiers as case-sensitive and un-quoted identifiers as case-insensitive
    """
    if name.startswith('"') and name.endswith('"'):
        return name[1:-1]  # Strip quotes

    return name.upper()


def parts_from_var(var: str, current_database: str | None, current_schema: str | None) -> tuple[str, str, str]:
    parts = var.split(".")
    if len(parts) == 3:
        # Fully qualified name
        database_name, schema_name, name = parts
    elif len(parts) == 2:
        # Schema + stage name
        assert current_database, "Current database must be set when stage name is not fully qualified"
        database_name, schema_name, name = current_database, parts[0], parts[1]
    elif len(parts) == 1:
        # Stage name only
        assert current_database, "Current database must be set when stage name is not fully qualified"
        assert current_schema, "Current schema must be set when stage name is not fully qualified"
        database_name, schema_name, name = current_database, current_schema, parts[0]
    else:
        raise ValueError(f"Invalid stage name: {var}")

    # Normalize names to uppercase if not wrapped in double quotes
    database_name = normalise_ident(database_name)
    schema_name = normalise_ident(schema_name)
    name = normalise_ident(name)

    return database_name, schema_name, name


def local_dir(fqname: str) -> str:
    """
    Given a fully qualified stage name, return the directory path where the stage files are stored.
    """
    catalog, schema, stage_name = fqname.split(".")
    return f"{LOCAL_BUCKET_PATH}/{catalog}/{schema}/{stage_name}/"


def list_stage_files_sql(stage_name: str) -> str:
    """
    Generate SQL to list files in a stage directory, matching Snowflake's LIST output format.
    """
    sdir = local_dir(stage_name)
    return f"""
        select
            lower(split_part(filename, '/', -2)) || '/' || split_part(filename, '/', -1) AS name,
            size,
            md5(content) as md5,
            strftime(last_modified, '%a, %d %b %Y %H:%M:%S GMT') as last_modified
        from read_blob('{sdir}/*')
    """

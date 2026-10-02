from __future__ import annotations

import datetime
import os
import re
import shutil
import tempfile
from contextlib import suppress
from pathlib import PurePath
from typing import Any, TypedDict
from urllib.parse import urlparse
from urllib.request import url2pathname

import snowflake.connector.errors
import sqlglot
from snowflake.connector.file_util import SnowflakeFileUtil
from sqlglot import Expr, exp

from fakesnow.expr import normalise_ident
from fakesnow.params import MutableParams
from fakesnow.transforms.options import parse_options

# TODO: clean up temp files on exit
LOCAL_BUCKET_PATH = tempfile.mkdtemp(prefix="fakesnow_bucket_")


class StageInfoDict(TypedDict):
    locationType: str
    location: str
    creds: dict[str, Any]


class UploadCommandDict(TypedDict):
    stageInfo: StageInfoDict
    src_locations: list[str]
    parallel: int
    autoCompress: bool
    sourceCompression: str
    overwrite: bool
    command: str


def create_stage(
    expression: Expr,
    current_database: str | None,
    current_schema: str | None,
) -> Expr:
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
    if not isinstance(ident, exp.Identifier):
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\nInvalid identifier type {ident.__class__.__name__} for stage name.",
            errno=1003,
            sqlstate="42000",
        )

    catalog = table.catalog or current_database
    schema = table.db or current_schema
    stage_name = ident.this
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

    replace = expression.args.get("replace")
    if_not_exists = expression.args.get("exists")

    guard = (
        ""
        if replace
        else f"""
        WHERE NOT EXISTS (
            SELECT 1 FROM _fs_global._fs_information_schema._fs_stages
            WHERE name = '{stage_name}' AND database_name = '{catalog}' AND schema_name = '{schema}'
        )"""
    )
    insert_sql = f"""
        INSERT {"OR REPLACE" if replace else ""} INTO _fs_global._fs_information_schema._fs_stages
        (created_on, name, database_name, schema_name, url, has_credentials, has_encryption_key, owner,
        comment, region, type, cloud, notification_channel, storage_integration, endpoint, owner_role_type,
        directory_enabled)
        SELECT
            '{now}', '{stage_name}', '{catalog}', '{schema}', '{url}', 'N', 'N', 'SYSADMIN',
            '', NULL, '{stage_type}', {f"'{cloud}'" if cloud else "NULL"}, NULL, NULL, NULL, 'ROLE',
            'N'
        {guard}
        """
    transformed = sqlglot.parse_one(insert_sql, read="duckdb")
    transformed.args["create_stage_name"] = stage_name
    transformed.args["create_stage_if_not_exists"] = if_not_exists
    if replace:
        # A replaced stage starts empty. Its directory may not exist yet, but other cleanup failures
        # must prevent the replacement from succeeding with stale files.
        with suppress(FileNotFoundError):
            shutil.rmtree(internal_dir(f"{catalog}.{schema}.{stage_name}"))
    return transformed


def list_stage(expression: Expr, current_database: str | None, current_schema: str | None) -> Expr:
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

    transformed = sqlglot.parse_one(stage_lookup_sql(catalog, schema, stage_name), read="duckdb")
    transformed.args["list_stage_name"] = f"{catalog}.{schema}.{stage_name}"
    return transformed


_PUT_UNQUOTED_SRC = re.compile(r"^(\s*PUT\s+)(file://\S+)", re.IGNORECASE)


def put_stage(
    expression: Expr,
    current_database: str | None,
    current_schema: str | None,
    params: MutableParams | None,
) -> Expr:
    """Transform PUT to a SELECT statement to locate the stage.

    See https://docs.snowflake.com/en/sql-reference/sql/put
    """
    # sqlglot falls back to Command for PUT with an unquoted source.
    # https://github.com/tobymao/sqlglot/issues/8399
    if isinstance(expression, exp.Command) and expression.name.upper() == "PUT":
        command = _PUT_UNQUOTED_SRC.sub(r"\1'\2'", f"PUT {expression.expression}")
        expression = sqlglot.parse_one(command, read="snowflake")

    if not isinstance(expression, exp.Put):
        return expression

    assert isinstance(expression.this, exp.Literal), "PUT command requires a file path as a literal"
    src_url = urlparse(expression.this.this)
    # The connector re-requests presigned URLs with file://data.csv.gz (no path).
    # Other non-localhost authorities can be relative directories or Windows drive letters.
    src_path = url2pathname(src_url.path)
    if src_url.netloc and src_url.netloc.lower() != "localhost":
        src_path = src_url.netloc + src_path
    target = expression.args["target"]

    assert isinstance(target, exp.Var), f"{target} is not a exp.Var"
    this = target.text("this")
    if this == "?":
        if not (isinstance(params, list) and len(params) == 1):
            raise NotImplementedError("PUT requires a single parameter for the stage name")
        this = params.pop(0)
    if not this.startswith("@"):
        msg = f"SQL compilation error:\n{this} does not start with @"
        raise snowflake.connector.errors.ProgrammingError(
            msg=msg,
            errno=1003,
            sqlstate="42000",
        )
    # strip leading @ and separate the case-sensitive path from the stage name
    var, _, path = this[1:].partition("/")
    catalog, schema, stage_name = parts_from_var(var, current_database=current_database, current_schema=current_schema)

    options = parse_options(expression.args.get("properties") or [])
    auto_compress = options.get("AUTO_COMPRESS", True)
    if not isinstance(auto_compress, bool):
        raise snowflake.connector.errors.ProgrammingError(
            msg="Invalid value specified for property 'AUTO_COMPRESS'",
            errno=1481,
            sqlstate="42601",
        )

    transformed = sqlglot.parse_one(stage_lookup_sql(catalog, schema, stage_name), read="duckdb")
    fqname = f"{catalog}.{schema}.{stage_name}"
    transformed.args["put_stage_name"] = fqname
    transformed.args["put_stage_data"] = {
        "stageInfo": {
            # use LOCAL_FS otherwise we need to mock S3 with HTTPS which requires a certificate
            "locationType": "LOCAL_FS",
            "location": internal_dir(fqname, path),
            "creds": {},
        },
        "src_locations": [src_path],
        # defaults as per https://docs.snowflake.com/en/sql-reference/sql/put TODO: support other values
        "parallel": 4,
        "autoCompress": auto_compress,
        "sourceCompression": "auto_detect",
        "overwrite": False,
        "command": "UPLOAD",
    }

    return transformed


def is_table_stage(stage_name: str) -> bool:
    """A stage name starting with % is a table stage, which exists implicitly for every table."""
    return stage_name.startswith("%")


def not_found_error(fqname: str) -> snowflake.connector.errors.ProgrammingError:
    """Build a missing-stage error using Snowflake's table-stage identifier quoting."""
    namespace, _, stage_name = fqname.rpartition(".")
    if is_table_stage(stage_name):
        fqname = f"{namespace}.{exp.to_identifier(stage_name, quoted=True).sql(dialect='snowflake')}"
    return snowflake.connector.errors.ProgrammingError(
        msg=f"SQL compilation error:\nStage '{fqname}' does not exist or not authorized.",
        errno=2003,
        sqlstate="02000",
    )


def stage_lookup_sql(catalog: str, schema: str, stage_name: str) -> str:
    """SQL that returns a single row when the stage exists."""
    if is_table_stage(stage_name):
        return f"""
            SELECT *
            from duckdb_tables()
            where database_name = '{catalog}' and schema_name = '{schema}' and table_name = '{stage_name[1:]}'
        """
    return f"""
        SELECT *
        from _fs_global._fs_information_schema._fs_stages
        where database_name = '{catalog}' and schema_name = '{schema}' and name = '{stage_name}'
    """


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


def is_internal(s: str) -> bool:
    return PurePath(s).is_relative_to(LOCAL_BUCKET_PATH)


def internal_dir(fqname: str, path: str = "") -> str:
    """Return a directory within the stage, rejecting paths that escape its root."""
    catalog, schema, stage_name = fqname.split(".")
    root = f"{LOCAL_BUCKET_PATH}/{catalog}/{schema}/{stage_name}/"
    directory = f"{root}{path}"
    if not PurePath(os.path.realpath(directory)).is_relative_to(os.path.realpath(root)):
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:\nStage path escapes the stage directory.",
            errno=1003,
            sqlstate="42000",
        )
    return directory


def _file_name_prefix(stage_name: str) -> str:
    return "" if is_table_stage(stage_name) else f"{stage_name.lower()}/"


def internal_file_name(path: str) -> str:
    """Return a Snowflake result filename, preserving the path within its stage."""
    _, _, stage_name, *file_parts = PurePath(path).relative_to(LOCAL_BUCKET_PATH).parts
    return f"{_file_name_prefix(stage_name)}{'/'.join(file_parts)}"


def list_stage_files_sql(stage_name: str) -> str:
    """
    Generate SQL to list files in a stage directory, matching Snowflake's LIST output format.
    """
    sdir = internal_dir(stage_name)
    prefix = exp.Literal.string(_file_name_prefix(stage_name.rsplit(".", 1)[-1])).sql(dialect="duckdb")
    glob = exp.Literal.string(f"{sdir}**/*").sql(dialect="duckdb")
    return f"""
        select
            {prefix} || substr(filename, {len(sdir) + 1}) AS name,
            size,
            md5(content) as md5,
            strftime(last_modified, '%a, %d %b %Y %H:%M:%S GMT') as last_modified
        from read_blob({glob})
    """


def upload_files(put_stage_data: UploadCommandDict) -> list[dict[str, Any]]:
    auto_compress = put_stage_data["autoCompress"]
    results = []
    for src in put_stage_data["src_locations"]:
        basename = os.path.basename(src)
        stage_dir = put_stage_data["stageInfo"]["location"]

        os.makedirs(stage_dir, exist_ok=True)
        source_is_gzipped = basename.endswith(".gz")

        if auto_compress and not source_is_gzipped:
            gzip_file_name, _ = SnowflakeFileUtil.compress_file_with_gzip(src, stage_dir)

            # Rename to match expected .gz extension on upload
            target_basename = basename + ".gz"
            target = os.path.join(stage_dir, target_basename)
            os.replace(gzip_file_name, target)
            target_compression = "GZIP"
        else:
            target_basename = basename
            target = os.path.join(stage_dir, target_basename)
            shutil.copyfile(src, target)
            target_compression = "GZIP" if source_is_gzipped else "NONE"

        target_size = os.path.getsize(target)
        source_size = os.path.getsize(src)

        results.append(
            {
                "source": basename,
                "target": target_basename,
                "source_size": source_size,
                "target_size": target_size,
                "source_compression": "GZIP" if source_is_gzipped else "NONE",
                "target_compression": target_compression,
                "status": "UPLOADED",
                "message": "",
            }
        )
    return results

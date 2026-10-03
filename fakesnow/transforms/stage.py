from __future__ import annotations

import datetime
import os
import shutil
import tempfile
import uuid
from contextlib import suppress
from glob import escape as glob_escape
from pathlib import PurePath
from typing import Any, TypedDict
from urllib.parse import urlparse
from urllib.request import url2pathname

import snowflake.connector.errors
import sqlglot
from duckdb import DuckDBPyConnection
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


class TableStages:
    """Resolve implicit table stages, including session-local temporary tables.

    DuckDB keeps temporary tables in one session namespace, so remember the Snowflake schema each was created in.
    Table OIDs survive renames and are never reused. Lookups embed only the live table's OID, and registrations
    for dropped tables are pruned when another temporary table is created, so session history stays bounded.
    """

    def __init__(self, duck_conn: DuckDBPyConnection):
        self._duck_conn = duck_conn
        self._session = uuid.uuid4().hex
        # Temporary table OID -> (catalog, schema) it was created in, which DuckDB does not record.
        self._temporary: dict[int, tuple[str | None, str | None]] = {}

    def record_temporary_table(self, expression: Expr, catalog: str | None, schema: str | None) -> None:
        if not (
            isinstance(expression, exp.Create)
            and expression.args.get("kind") == "TABLE"
            and expression.find(exp.TemporaryProperty)
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
        self._temporary.setdefault(live[table.name], (table.catalog or catalog, table.db or schema))

    def resolve_for_copy(
        self, catalog: str, schema: str, stage_name: str, target: tuple[str | None, str | None, str]
    ) -> str:
        """Resolve a table stage for loading, which is allowed only into its owning table."""
        result = self._duck_conn.execute(self.lookup_sql(catalog, schema, stage_name)).fetchone()
        if not result:
            raise not_found_error(f"{catalog}.{schema}.{stage_name}")
        owner = (catalog, schema, stage_name[1:])
        if owner != target:
            raise snowflake.connector.errors.ProgrammingError(
                msg=(
                    f"SQL compilation error:\nAccess to the stage area of a table ({owner[2]}) "
                    f"with the schema of another table ({target[2]}) is not allowed."
                ),
                errno=1023,
                sqlstate="42601",
            )
        return result[0]

    def lookup_sql(self, catalog: str, schema: str, stage_name: str) -> str:
        """SQL returning the storage name of the table stage, or no rows if the table does not exist.

        A temporary table shadows a permanent table of the same name, but only in the schema it was created in.
        """
        # At most one temporary table has this name in the session. Embed its OID only if it was created in the
        # requested schema; otherwise match nothing (NULL), leaving any permanent table to resolve.
        sql = "SELECT table_oid FROM duckdb_tables() WHERE temporary AND table_name = ?"
        live = self._duck_conn.execute(sql, [stage_name[1:]]).fetchone()
        oid = live[0] if live and self._temporary.get(live[0]) == (catalog, schema) else "NULL"
        # Temporary storage is keyed by session and OID, so it is isolated between sessions and follows renames.
        logical_name = exp.Literal.string(f"{catalog}.{schema}.{stage_name}").sql(dialect="duckdb")
        return f"""
            SELECT CASE WHEN temporary THEN '{self._session}.' || table_oid || '.%' ELSE {logical_name} END
            FROM duckdb_tables()
            WHERE table_name = '{stage_name[1:]}' AND (
                (temporary AND table_oid = {oid})
                OR (NOT temporary AND database_name = '{catalog}' AND schema_name = '{schema}')
            )
            ORDER BY temporary DESC
            LIMIT 1
        """


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


def list_stage(
    expression: Expr, current_database: str | None, current_schema: str | None, table_stages: TableStages
) -> Expr:
    """Transform LIST to list file system operation.

    See https://docs.snowflake.com/en/sql-reference/sql/list
    """
    if not (isinstance(expression, exp.Command) and expression.name.upper() == "LIST"):
        return expression

    if expression.expression is None:
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:\nsyntax error unexpected '<EOF>'.", errno=1003, sqlstate="42000"
        )
    # Parse the complete argument, not a SELECT whose extra clauses could be silently discarded.
    argument = expression.expression.name
    reference = sqlglot.parse_one(argument, read="snowflake", into=exp.Table)
    dialect = sqlglot.Dialect.get_or_raise("snowflake")
    argument_tokens = [(token.token_type, token.text) for token in dialect.tokenize(argument)]
    if not (
        isinstance(reference, exp.Table)
        and isinstance(reference.this, (exp.Var, exp.Literal))
        and all(value is None for key, value in reference.args.items() if key != "this")
        and reference.this.name.startswith("@")
        # Some malformed options are consumed but discarded by SQLGlot, so also check the original syntax.
        and argument_tokens
        == [(token.token_type, token.text) for token in dialect.tokenize(reference.this.sql(dialect="snowflake"))]
    ):
        raise snowflake.connector.errors.ProgrammingError(
            msg="SQL compilation error:\nsyntax error in LIST stage reference.", errno=1003, sqlstate="42000"
        )
    var = reference.this.name[1:]
    catalog, schema, stage_name = parts_from_var(var, current_database=current_database, current_schema=current_schema)

    transformed = sqlglot.parse_one(stage_lookup_sql(catalog, schema, stage_name, table_stages), read="duckdb")
    transformed.args["list_stage_name"] = f"{catalog}.{schema}.{stage_name}"
    return transformed


def put_stage(
    expression: Expr,
    current_database: str | None,
    current_schema: str | None,
    params: MutableParams | None,
    table_stages: TableStages,
) -> Expr:
    """Transform PUT to a SELECT statement to locate the stage.

    See https://docs.snowflake.com/en/sql-reference/sql/put
    """
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

    transformed = sqlglot.parse_one(stage_lookup_sql(catalog, schema, stage_name, table_stages), read="duckdb")
    fqname = f"{catalog}.{schema}.{stage_name}"
    transformed.args["put_stage_name"] = fqname
    transformed.args["put_stage_path"] = path
    transformed.args["put_stage_data"] = {
        "stageInfo": {
            # use LOCAL_FS otherwise we need to mock S3 with HTTPS which requires a certificate
            "locationType": "LOCAL_FS",
            # Filled after the lookup resolves the table instance (including temporary shadowing).
            "location": "",
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


def stage_lookup_sql(catalog: str, schema: str, stage_name: str, table_stages: TableStages) -> str:
    """SQL returning the storage identity of the single resolved stage, or no rows."""
    if is_table_stage(stage_name):
        return table_stages.lookup_sql(catalog, schema, stage_name)
    storage_name = exp.Literal.string(f"{catalog}.{schema}.{stage_name}").sql(dialect="duckdb")
    return f"""
        SELECT {storage_name} AS storage_name
        from _fs_global._fs_information_schema._fs_stages
        where database_name = '{catalog}' and schema_name = '{schema}' and name = '{stage_name}'
    """


def complete_stage_lookup(expression: Expr, result: tuple | None) -> str:
    """Complete PUT/LIST using resolved storage without leaking lookup details to the cursor."""
    stage_name = expression.args.get("list_stage_name") or expression.args["put_stage_name"]
    if result is None:
        raise not_found_error(stage_name)
    storage_name = result[0]
    if expression.args.get("list_stage_name"):
        return list_stage_files_sql(storage_name)
    expression.args["put_stage_data"]["stageInfo"]["location"] = internal_dir(
        storage_name, expression.args["put_stage_path"]
    )
    return "SELECT 'Statement executed successfully.' AS status"


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


def file_path(url: str) -> str:
    """Extract a URL's path, keeping internal-stage filesystem paths literal."""
    return url if is_internal(url) else urlparse(url).path


def internal_dir(fqname: str, path: str = "") -> str:
    """Return a directory within the stage, rejecting paths that escape its root."""
    catalog, schema, stage_name = fqname.split(".")
    root = f"{LOCAL_BUCKET_PATH}/{catalog}/{schema}/{stage_name}/"
    directory = f"{root}{path}"
    if not PurePath(os.path.realpath(directory)).is_relative_to(os.path.realpath(root)):
        error_details = f"HTTPError('403 Client Error: Forbidden for url: {directory}')"
        raise snowflake.connector.errors.OperationalError(
            msg=(
                f"While putting file(s) there was an error: '{error_details}', "
                "this might be caused by your access to the blob storage provider, or by Snowflake."
            ),
            errno=253003,
        )
    return directory


def _file_name_prefix(stage_name: str) -> str:
    return "" if is_table_stage(stage_name) else f"{stage_name.lower()}/"


def internal_files_sql(prefix: str) -> str:
    """Match an opaque stage prefix without interpreting its suffix as filesystem traversal."""
    catalog, schema, stage_name, *_ = PurePath(prefix).relative_to(LOCAL_BUCKET_PATH).parts
    root = internal_dir(f"{catalog}.{schema}.{stage_name}")
    glob = exp.Literal.string(f"{glob_escape(root)}**/*").sql(dialect="duckdb")
    literal_prefix = exp.Literal.string(prefix).sql(dialect="duckdb")
    return f"SELECT file FROM glob({glob}) WHERE starts_with(file, {literal_prefix})"


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

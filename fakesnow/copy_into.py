from __future__ import annotations

import datetime
import os
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, NamedTuple, Protocol, Union, cast
from urllib.parse import urlparse, urlunparse

import duckdb
import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import exp

import fakesnow.transforms.stage as stage
from fakesnow import logger
from fakesnow.params import MutableParams, pop_qmark_param

Params = Union[Sequence[Any], dict[Any, Any]]


class LoadHistoryRecord(NamedTuple):
    """Represents a record in the INFORMATION_SCHEMA.LOAD_HISTORY table."""

    schema_name: str
    file_name: str
    table_name: str
    last_load_time: str  # ISO8601 datetime with timezone
    status: str
    row_count: int
    row_parsed: int
    first_error_message: str | None
    first_error_line_number: int | None
    first_error_character_position: int | None
    first_error_col_name: str | None
    error_count: int
    error_limit: int | None


def copy_into(
    duck_conn: DuckDBPyConnection,
    current_database: str | None,
    current_schema: str | None,
    expr: exp.Copy,
    params: MutableParams | None = None,
) -> str:
    cparams = _params(expr, params)
    if isinstance(cparams.file_format, ReadParquet):
        from_ = expr.args["files"][0]
        # parquet must use MATCH_BY_COLUMN_NAME (TODO) or a copy transformation
        # ie: the from clause in COPY INTO must be a subquery
        if not isinstance(from_, exp.Subquery):
            raise snowflake.connector.errors.ProgrammingError(
                msg="SQL compilation error:\nPARQUET file format can produce one and only one column of type variant, object, or array. Load data into separate columns using the MATCH_BY_COLUMN_NAME copy option or copy with transformation.",  # noqa: E501
                errno=2019,
                sqlstate="0A000",
            )

    from_source = _from_source(expr)
    source = (
        stage_url_from_var(from_source[1:], duck_conn, current_database, current_schema)
        if from_source.startswith("@")
        else from_source
    )
    urls = _source_urls(source, cparams.files) if cparams.files else _source_glob(source, duck_conn)
    if not urls:
        sql = "SELECT 'Copy executed with 0 files processed.' AS status"
        duck_conn.execute(sql)
        return sql

    inserts = _inserts(expr, cparams, urls)
    table = expr.this
    if isinstance(expr.this, exp.Table):
        table = expr.this
    elif isinstance(expr.this, exp.Schema) and isinstance(expr.this.this, exp.Table):
        table = expr.this.this
    else:
        raise AssertionError(f"copy into {expr.this.__class__} is not Table or Schema")

    schema = table.db or current_schema
    assert schema

    histories: list[LoadHistoryRecord] = []
    load_time = datetime.datetime.now(datetime.timezone.utc).isoformat()
    try:
        check_sql = "SELECT 1 FROM _fs_information_schema._fs_load_history WHERE FILE_NAME = ? LIMIT 1"

        for i, url in zip(inserts, urls):
            # Check if file has been loaded into any table before
            duck_conn.execute(check_sql, [url])
            if duck_conn.fetchone() and not cparams.force:
                affected_count = 0
                status = "LOAD_SKIPPED"
                error_limit = None
                error_count = 1
                first_error_message = "File was loaded before."
            else:
                sql = i.sql(dialect="duckdb")
                logger.log_sql(sql, params)
                duck_conn.execute(sql, params)
                (affected_count,) = duck_conn.fetchall()[0]
                status = "LOADED"
                error_limit = 1
                error_count = 0
                first_error_message = None
                path = urlparse(url).path
                if cparams.purge and stage.is_internal(path):
                    # If the file is internal, we can remove it from the stage
                    os.remove(path)

            history = LoadHistoryRecord(
                schema_name=schema,
                file_name=url,
                table_name=table.name,
                last_load_time=load_time,
                status=status,
                row_count=affected_count,
                row_parsed=affected_count,
                first_error_message=first_error_message,
                first_error_line_number=None,
                first_error_character_position=None,
                first_error_col_name=None,
                error_count=error_count,
                error_limit=error_limit,
            )
            histories.append(history)

        if insert_histories := [h for h in histories if h.status != "LOAD_SKIPPED"]:
            values = "\n ,".join(str(tuple(history)).replace("None", "NULL") for history in insert_histories)
            sql = f"INSERT INTO _fs_information_schema._fs_load_history VALUES {values}"
            duck_conn.execute(sql, params)

        columns = (
            "file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, "
            "first_error_character, first_error_column_name"
        )
        values = "\n, ".join(
            f"('{_result_file_name(h.file_name)}', '{h.status}', {h.row_parsed}, {h.row_count}, "
            f"{h.error_limit or 'NULL'}, {h.error_count}, "
            f"{repr(h.first_error_message) if h.first_error_message else 'NULL'}, "
            f"{h.first_error_line_number or 'NULL'}, {h.first_error_character_position or 'NULL'}, "
            f"{h.first_error_col_name or 'NULL'})"
            for h in histories
        )
        sql = f"SELECT * FROM (VALUES\n  {values}\n) AS t({columns})"
        duck_conn.execute(sql)

        return sql
    except duckdb.HTTPException as e:
        raise snowflake.connector.errors.ProgrammingError(msg=e.args[0], errno=91016, sqlstate="22000") from None
    except duckdb.ConversionException as e:
        raise snowflake.connector.errors.ProgrammingError(msg=e.args[0], errno=100038, sqlstate="22018") from None


def _result_file_name(url: str) -> str:
    if not stage.is_internal(urlparse(url).path):
        return url

    # for internal stages, return the stage name lowered + file name
    parts = url.split("/")
    return f"{parts[-2].lower()}/{parts[-1]}"


def _params(expr: exp.Copy, params: MutableParams | None = None) -> CopyParams:
    kwargs = {}
    force = False
    purge = False
    on_error = "ABORT_STATEMENT"

    cparams = CopyParams()
    for param in cast(list[exp.CopyParameter], expr.args.get("params", [])):
        assert isinstance(param.this, exp.Var), f"{param.this.__class__} is not a Var"
        var = param.this.name.upper()
        if var == "FILE_FORMAT":
            if kwargs.get("file_format"):
                raise ValueError(cparams)

            var_type = next((e.args["value"].this for e in param.expressions if e.this.this == "TYPE"), None)
            if not var_type:
                raise NotImplementedError("FILE_FORMAT without TYPE is not currently implemented")

            if var_type == "CSV":
                kwargs["file_format"] = handle_csv(param.expressions)
            elif var_type == "PARQUET":
                kwargs["file_format"] = ReadParquet()
            else:
                raise NotImplementedError(f"{var_type} FILE_FORMAT is not currently implemented")
        elif var == "FORCE":
            force = True
        elif var == "FILES":
            kwargs["files"] = [lit.name for lit in param.find_all(exp.Literal)]
        elif var == "PURGE":
            purge = True
        elif var == "ON_ERROR":
            if isinstance(param.expression, exp.Var):
                on_error = param.expression.name.upper()
            elif isinstance(param.expression, exp.Placeholder):
                on_error = pop_qmark_param(params, expr, param.expression)
            else:
                raise NotImplementedError(f"{param.expression.__class__=}")

            if not (isinstance(on_error, str) and on_error.upper() == "ABORT_STATEMENT"):
                raise NotImplementedError(param)
        else:
            raise ValueError(f"Unknown copy parameter: {param.this}")

    return CopyParams(force=force, purge=purge, on_error=on_error, **kwargs)


def _from_source(expr: exp.Copy) -> str:
    # NB: sqlglot parses the from clause as "files" strangely
    from_ = expr.args["files"][0].this

    if isinstance(from_, exp.Select):
        from_table = from_.args["from"]
        # if a subquery is used in the FROM clause it must be loaded from a stage not an external location
        assert isinstance(from_table, exp.From), f"{from_table.__class__} is not a From"
        assert isinstance(from_table.this, exp.Table), f"{from_table.__class__} is not a Table"
        var = from_table.this.this
        if not isinstance(var, exp.Var):
            # not a very helpful message, but this is what Snowflake returns
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"SQL compilation error:\ninvalid URL prefix found in: {from_table.this.this}",
                errno=1011,
                sqlstate="42601",
            )
        # return the name of the stage, eg: @stage1
        return var.this
    elif isinstance(from_, exp.Var):
        # return the name of the stage, eg: @stage1
        return from_.this

    assert isinstance(from_, exp.Literal), f"{from_} is not a exp.Literal"
    # return url
    return from_.name


def stage_url_from_var(
    var: str, duck_conn: DuckDBPyConnection, current_database: str | None, current_schema: str | None
) -> str:
    database_name, schema_name, name = stage.parts_from_var(var, current_database, current_schema)

    # Look up the stage URL
    duck_conn.execute(
        """
        SELECT url FROM _fs_global._fs_information_schema._fs_stages
        WHERE database_name = ? and schema_name  = ? and name = ?
        """,
        (database_name, schema_name, name),
    )
    if result := duck_conn.fetchone():
        # if no URL is found, it is an internal stage ie: local directory
        url = result[0] or stage.internal_dir(f"{database_name}.{schema_name}.{name}")
        return url
    else:
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\nStage '{database_name}.{schema_name}.{name}' does not exist or not authorized.",  # noqa: E501
            errno=2003,
            sqlstate="02000",
        )


def _source_urls(source: str, files: list[str]) -> list[str]:
    """Convert from_source to a list of URLs."""
    scheme, netloc, path, params, query, fragment = urlparse(source)
    if not scheme:
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\ninvalid URL prefix found in: '{source}'", errno=1011, sqlstate="42601"
        )

    # rebuild url from components to ensure correct handling of host slash
    return [_urlunparse(scheme, netloc, path, params, query, fragment, file) for file in files] or [source]


def _source_glob(source: str, duck_conn: DuckDBPyConnection) -> list[str]:
    """List files from the source using duckdb glob."""
    if stage.is_internal(source):
        source = Path(source).as_uri()  # convert local directory to a file URL

    scheme, _netloc, _path, _params, _query, _fragment = urlparse(source)
    glob = f"{source}/*" if scheme == "file" else f"{source}*"
    sql = f"SELECT file FROM glob('{glob}')"
    logger.log_sql(sql)
    result = duck_conn.execute(sql).fetchall()
    return [r[0] for r in result]


def _urlunparse(scheme: str, netloc: str, path: str, params: str, query: str, fragment: str, suffix: str) -> str:
    """Construct a URL from its components appending suffix to the last used component."""
    if fragment:
        fragment += suffix
    elif query:
        query += suffix
    elif params:
        params += suffix
    else:
        path += suffix
    return urlunparse((scheme, netloc, path, params, query, fragment))


def _inserts(expr: exp.Copy, params: CopyParams, urls: list[str]) -> list[exp.Expression]:
    # INTO expression
    target = expr.this

    from_ = expr.args["files"][0]
    if isinstance(from_, exp.Subquery):
        select = from_.this
        assert isinstance(select, exp.Select), f"{select.__class__} is not a Select"
        columns = _strip_json_extract(select).expressions
    else:
        columns = [exp.Column(this=exp.Identifier(this=f"column{i}")) for i in range(len(target.expressions))] or [
            exp.Column(this=exp.Star())
        ]

    return [
        exp.Insert(
            this=target,
            expression=exp.Select(expressions=columns).from_(exp.Table(this=params.file_format.read_expression(url))),
        )
        for url in urls
    ]


def _strip_json_extract(expr: exp.Select) -> exp.Select:
    """
    Strip $1 prefix from SELECT statement columns.
    """
    dollar1 = exp.Parameter(this=exp.Literal(this="1", is_string=False))

    for p in expr.find_all(exp.Parameter):
        if p == dollar1 and p.parent and (key := p.parent.expression.find(exp.JSONPathKey)):
            assert p.parent.parent, expr
            p.parent.parent.args["this"] = exp.Identifier(this=key.this)

    return expr


def handle_csv(expressions: list[exp.Property]) -> ReadCSV:
    skip_header = ReadCSV.skip_header
    quote = ReadCSV.quote
    delimiter = ReadCSV.delimiter

    for expression in expressions:
        exp_type = expression.name
        if exp_type in {"TYPE"}:
            continue

        elif exp_type == "SKIP_HEADER":
            skip_header = True
        elif exp_type == "FIELD_OPTIONALLY_ENCLOSED_BY":
            quote = expression.args["value"].this
        elif exp_type == "FIELD_DELIMITER":
            delimiter = expression.args["value"].this
        else:
            raise NotImplementedError(f"{exp_type} is not currently implemented")

    return ReadCSV(
        skip_header=skip_header,
        quote=quote,
        delimiter=delimiter,
    )


@dataclass
class FileTypeHandler(Protocol):
    def read_expression(self, url: str) -> exp.Expression: ...

    @staticmethod
    def make_eq(name: str, value: list | str | int | bool) -> exp.EQ:
        if isinstance(value, list):
            expression = exp.array(*[exp.Literal(this=str(v), is_string=isinstance(v, str)) for v in value])
        elif isinstance(value, bool):
            expression = exp.Boolean(this=value)
        else:
            expression = exp.Literal(this=str(value), is_string=isinstance(value, str))

        return exp.EQ(this=exp.Literal(this=name, is_string=False), expression=expression)


@dataclass
class ReadCSV(FileTypeHandler):
    skip_header: bool = False
    quote: str | None = None
    delimiter: str = ","

    def read_expression(self, url: str) -> exp.Expression:
        # don't parse header and use as column names, keep them as column0, column1, etc
        args = [self.make_eq("header", False)]

        if self.skip_header:
            args.append(self.make_eq("skip", 1))

        if self.quote:
            quote = self.quote.replace("'", "''")
            args.append(self.make_eq("quote", quote))

        if self.delimiter and self.delimiter != ",":
            delimiter = self.delimiter.replace("'", "''")
            args.append(self.make_eq("sep", delimiter))

        return exp.func("read_csv", exp.Literal(this=url, is_string=True), *args)


@dataclass
class ReadParquet(FileTypeHandler):
    def read_expression(self, url: str) -> exp.Expression:
        return exp.func("read_parquet", exp.Literal(this=url, is_string=True))


@dataclass
class CopyParams:
    files: list[str] = field(default_factory=list)
    # Snowflake defaults to CSV when no file format is specified
    file_format: FileTypeHandler = field(default_factory=ReadCSV)
    force: bool = False
    purge: bool = False
    on_error: str = "ABORT_STATEMENT"  # Default to ABORT_STATEMENT

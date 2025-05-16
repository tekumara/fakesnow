from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol, cast
from urllib.parse import urlparse, urlunparse

import duckdb
import snowflake.connector.errors
from duckdb import DuckDBPyConnection
from sqlglot import exp

from fakesnow import logger


def copy_into(
    duck_conn: DuckDBPyConnection, expr: exp.Copy, params: Sequence[Any] | dict[Any, Any] | None = None
) -> str:
    cparams = _params(expr)
    urls = _source_urls(expr, cparams.files)
    inserts = _inserts(expr, cparams, urls)

    results = []
    try:
        # TODO: fetch files last modified dates and check if file exists in load_history already
        for i, url in zip(inserts, urls):
            sql = i.sql(dialect="duckdb")
            logger.log_sql(sql, params)
            duck_conn.execute(sql, params)
            (affected_count,) = duck_conn.fetchall()[0]
            results.append(f"('{url}', 'LOADED', {affected_count}, {affected_count}, 1, 0, NULL, NULL, NULL, NULL)")

            # TODO: update load_history with the results if loaded

        columns = "file, status, rows_parsed, rows_loaded, error_limit, errors_seen, first_error, first_error_line, first_error_character, first_error_column_name"  # noqa: E501
        values = "\n, ".join(results)
        sql = f"SELECT * FROM (VALUES\n  {values}\n) AS t({columns})"
        duck_conn.execute(sql)
        return sql
    except duckdb.HTTPException as e:
        raise snowflake.connector.errors.ProgrammingError(msg=e.args[0], errno=91016, sqlstate="22000") from None
    except duckdb.ConversionException as e:
        raise snowflake.connector.errors.ProgrammingError(msg=e.args[0], errno=100038, sqlstate="22018") from None


def _params(expr: exp.Copy) -> Params:
    kwargs = {}
    force = False

    params = cast(list[exp.CopyParameter], expr.args.get("params", []))
    cparams = Params()
    for param in params:
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
            else:
                raise NotImplementedError(f"{var_type} FILE_FORMAT is not currently implemented")
        elif var == "FORCE":
            force = True
        elif var == "FILES":
            kwargs["files"] = [lit.name for lit in param.find_all(exp.Literal)]
        else:
            raise ValueError(f"Unknown copy parameter: {param.this}")

    if not force:
        raise NotImplementedError("COPY INTO with FORCE=false (default) is not currently implemented")

    return Params(**kwargs)


def _source_urls(expr: exp.Copy, files: list[str]) -> list[str]:
    """
    Given a COPY statement and a list of files, return a list of URLs with each file appended as a fragment.
    Checks that the source is a valid URL.
    """
    source = expr.args["files"][0].this
    assert isinstance(source, exp.Literal), f"{source} is not a exp.Literal"

    scheme, netloc, path, params, query, fragment = urlparse(source.name)
    if not scheme:
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\ninvalid URL prefix found in: '{source.name}'", errno=1011, sqlstate="42601"
        )

    # rebuild url from components to ensure correct handling of host slash
    return [_urlunparse(scheme, netloc, path, params, query, fragment, file) for file in files] or [source.name]


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


def _inserts(expr: exp.Copy, params: Params, urls: list[str]) -> list[exp.Expression]:
    # INTO expression
    target = expr.this
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
        args = []

        # don't parse header and use as column names, keep them as column0, column1, etc
        args.append(self.make_eq("header", False))

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
class Params:
    files: list[str] = field(default_factory=list)
    # Snowflake defaults to CSV when no file format is specified
    file_format: FileTypeHandler = field(default_factory=ReadCSV)

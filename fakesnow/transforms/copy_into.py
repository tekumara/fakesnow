from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import PurePath
from typing import Protocol, cast
from urllib.parse import urlparse, urlunparse

import snowflake.connector.errors
from sqlglot import exp


def copy_into(expr: exp.Expression) -> exp.Expression:
    if not isinstance(expr, exp.Copy):
        return expr

    params = _params(expr)
    url = _source_url(expr, params.files)

    # INTO expression
    target = expr.this
    columns = [exp.Column(this=exp.Identifier(this=f"column{i}")) for i in range(len(target.expressions))] or [
        exp.Column(this=exp.Star())
    ]

    return exp.Insert(
        this=target,
        expression=exp.Select(expressions=columns).from_(exp.Table(this=params.file_format.read_expression(url))),
        copy_from=url,
    )


def _source_url(expr: exp.Copy, files: list[str]) -> str:
    # FROM expression
    source = expr.args["files"][0].this
    assert isinstance(source, exp.Literal), f"{source.__class__} is not a exp.Literal"

    if not files:
        raise NotImplementedError("COPY INTO without FILES is not currently implemented")
    if len(files) > 1:
        raise NotImplementedError("Multiple files not currently supported")

    scheme, netloc, path, params, query, fragment = urlparse(source.name)
    if not scheme:
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\ninvalid URL prefix found in: '{source.name}'", errno=1011, sqlstate="42601"
        )
    path = str(PurePath(path) / files[0])
    url = urlunparse((scheme, netloc, path, params, query, fragment))
    return url


def _params(expr: exp.Copy) -> Params:
    kwargs = {}
    force = False

    params = Params()
    for param in cast(list[exp.CopyParameter], expr.args.get("params", [])):
        assert isinstance(param.this, exp.Var), f"{param.this.__class__} is not a Var"
        var = param.this.name.upper()
        if var == "FILE_FORMAT":
            if kwargs.get("file_format"):
                raise ValueError(params)

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

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import PurePath
from typing import Protocol
from urllib.parse import urlparse, urlunparse

import snowflake.connector.errors
from sqlglot import exp
from typing_extensions import Self


def copy_into(expr: exp.Expression) -> exp.Expression:
    if not isinstance(expr, exp.Copy):
        return expr

    schema = expr.this

    columns = [exp.Column(this=exp.Identifier(this=f"column{i}")) for i in range(len(schema.expressions))] or [
        exp.Column(this=exp.Star())
    ]

    params = expr.args.get("params", [])
    # TODO: remove columns
    file_type_handler = _handle_params(params, [c.name for c in columns])

    # the FROM expression
    source = expr.args["files"][0].this
    assert isinstance(source, exp.Literal), f"{source.__class__} is not a exp.Literal"

    if len(file_type_handler.files) > 1:
        raise NotImplementedError("Multiple files not currently supported")
    file = file_type_handler.files[0]

    scheme, netloc, path, params, query, fragment = urlparse(source.name)
    if not scheme:
        raise snowflake.connector.errors.ProgrammingError(
            msg=f"SQL compilation error:\ninvalid URL prefix found in: '{source.name}'", errno=1011, sqlstate="42601"
        )
    path = str(PurePath(path) / file.name)
    url = urlunparse((scheme, netloc, path, params, query, fragment))

    return exp.Insert(
        this=schema,
        expression=exp.Select(expressions=columns).from_(exp.Table(this=file_type_handler.read_expression(url))),
        copy_from=url,
    )


def _handle_params(params: list[exp.CopyParameter], columns: list[str]) -> FileTypeHandler:
    file_type_handler = None
    force = False
    files = []
    for param in params:
        var = param.this.name
        if var == "FILE_FORMAT":
            if file_type_handler:
                raise ValueError(params)

            var_type = next((e.args["value"].this for e in param.expressions if e.this.this == "TYPE"), None)
            if not var_type:
                raise NotImplementedError("FILE_FORMAT without TYPE is not currently implemented")

            if var_type == "CSV":
                file_type_handler = handle_csv(param.expressions, columns)
            else:
                raise NotImplementedError(f"{var_type} FILE_FORMAT is not currently implemented")

        elif var == "FILES":
            files = param.expression.expressions if isinstance(param.expression, exp.Tuple) else [param.expression.this]
        elif var == "FORCE":
            force = True
            pass
        else:
            raise ValueError(f"Unknown copy parameter: {param.this}")

    if not force:
        raise NotImplementedError("COPY INTO with FORCE=false (default) is not currently implemented")

    if not files:
        raise NotImplementedError("COPY INTO without FILES is not currently implemented")

    if not file_type_handler:
        # default to CSV
        file_type_handler = handle_csv([], columns)

    file_type_handler = file_type_handler.with_files(files)
    return file_type_handler


def handle_csv(expressions: list[exp.Property], columns: list[str]) -> ReadCSV:
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
        columns=columns,
    )


@dataclass
class FileTypeHandler(Protocol):
    files: list = field(default_factory=list)

    def read_expression(self, url: str) -> exp.Expression: ...

    def with_files(self, files: list) -> Self:
        return replace(self, files=files)

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
    columns: list[str] = field(default_factory=list)

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

from __future__ import annotations

from dataclasses import dataclass, field, replace
from pathlib import PurePath
from typing import Any, Protocol
from urllib.parse import urlparse, urlunparse

from sqlglot import exp
from typing_extensions import Self


def copy_into(expr: exp.Expression) -> exp.Expression:
    if not isinstance(expr, exp.Copy):
        return expr

    schema = expr.this
    table = schema.this
    table_ident = table.args["this"]
    schema_ident = table.args["db"]
    columns = [exp.Column(this=c) for c in schema.expressions]

    source = expr.args["files"][0].this

    params = expr.args.get("params", [])
    file_type_handler = _handle_params(params, [c.name for c in columns])
    return exp.Insert(
        this=exp.Table(
            this=table_ident,
            db=schema_ident,
        ),
        expression=exp.Select(expressions=columns).from_(exp.Table(this=file_type_handler.to_expression(source))),
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
        raise ValueError(params)

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
    def to_expression(self, source: exp.Identifier) -> exp.Expression: ...

    def with_files(self, files: list) -> Self:
        return replace(self, files=files)

    @staticmethod
    def make_eq(name: str, value: Any) -> exp.EQ:  # noqa: ANN401
        if isinstance(value, list):
            expression = exp.array(*[exp.Literal(this=str(v), is_string=isinstance(v, str)) for v in value])
        else:
            expression = exp.Literal(this=str(value), is_string=isinstance(value, str))

        return exp.EQ(this=exp.Literal(this=name, is_string=False), expression=expression)


@dataclass
class ReadCSV(FileTypeHandler):
    skip_header: bool = False
    quote: str | None = None
    delimiter: str = ","
    files: list = field(default_factory=list)
    columns: list[str] = field(default_factory=list)

    def to_expression(self, source: exp.Identifier) -> exp.Expression:
        if len(self.files) > 1:
            raise NotImplementedError("Multiple files not currently supported")
        file = self.files[0]

        scheme, netloc, path, params, query, fragment = urlparse(source.name)
        path = str(PurePath(path) / file.name)
        filename = urlunparse((scheme, netloc, path, params, query, fragment))

        args = []
        if not self.skip_header:
            args.append(self.make_eq("header", 1))
        else:
            args.append(self.make_eq("names", self.columns))

        if self.quote:
            quote = self.quote.replace("'", "''")
            args.append(self.make_eq("quote", quote))

        if self.delimiter and self.delimiter != ",":
            delimiter = self.delimiter.replace("'", "''")
            args.append(self.make_eq("sep", delimiter))

        print(args)
        return exp.func("read_csv", exp.Literal(this=filename, is_string=True), *args)

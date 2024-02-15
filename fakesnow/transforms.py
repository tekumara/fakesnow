from __future__ import annotations

from pathlib import Path
from string import Template
from typing import cast

import sqlglot
from sqlglot import exp

from fakesnow.global_database import USERS_TABLE_FQ_NAME

MISSING_DATABASE = "missing_database"
SUCCESS_NOP = sqlglot.parse_one("SELECT 'Statement executed successfully.'")


def array_size(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.ArraySize):
        # case is used to convert 0 to null, because null is returned by duckdb when no case matches
        jal = exp.Anonymous(this="json_array_length", expressions=[expression.this])
        return exp.Case(ifs=[exp.If(this=jal, true=jal)])

    return expression


# TODO: move this into a Dialect as a transpilation
def create_database(expression: exp.Expression, db_path: Path | None = None) -> exp.Expression:
    """Transform create database to attach database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("CREATE database foo").transform(create_database).sql()
        'ATTACH DATABASE ':memory:' as foo'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression, with the database name stored in the create_db_name arg.
    """

    if isinstance(expression, exp.Create) and str(expression.args.get("kind")).upper() == "DATABASE":
        assert (ident := expression.find(exp.Identifier)), f"No identifier in {expression.sql}"
        db_name = ident.this
        db_file = f"{db_path/db_name}.db" if db_path else ":memory:"

        return exp.Command(
            this="ATTACH",
            expression=exp.Literal(this=f"DATABASE '{db_file}' AS {db_name}", is_string=True),
            create_db_name=db_name,
        )

    return expression


SQL_DESCRIBE_TABLE = Template(
    """
SELECT
    column_name AS "name",
    CASE WHEN data_type = 'NUMBER' THEN 'NUMBER(' || numeric_precision || ',' || numeric_scale || ')'
         WHEN data_type = 'TEXT' THEN 'VARCHAR(' || coalesce(character_maximum_length,16777216)  || ')'
         WHEN data_type = 'TIMESTAMP_NTZ' THEN 'TIMESTAMP_NTZ(9)'
         WHEN data_type = 'TIMESTAMP_TZ' THEN 'TIMESTAMP_TZ(9)'
         WHEN data_type = 'TIME' THEN 'TIME(9)'
         WHEN data_type = 'BINARY' THEN 'BINARY(8388608)'
        ELSE data_type END AS "type",
    'COLUMN' AS "kind",
    CASE WHEN is_nullable = 'YES' THEN 'Y' ELSE 'N' END AS "null?",
    column_default AS "default",
    'N' AS "primary key",
    'N' AS "unique key",
    NULL AS "check",
    NULL AS "expression",
    NULL AS "comment",
    NULL AS "policy name",
    NULL AS "privacy domain",
FROM information_schema._fs_columns_snowflake
WHERE table_catalog = '${catalog}' AND table_schema = '${schema}' AND table_name = '${table}'
ORDER BY ordinal_position
"""
)


def describe_table(
    expression: exp.Expression, current_database: str | None = None, current_schema: str | None = None
) -> exp.Expression:
    """Redirect to the information_schema._fs_describe_table to match snowflake.

    See https://docs.snowflake.com/en/sql-reference/sql/desc-table
    """

    if (
        isinstance(expression, exp.Describe)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, str)
        and kind.upper() == "TABLE"
        and (table := expression.find(exp.Table))
    ):
        catalog = table.catalog or current_database
        schema = table.db or current_schema

        return sqlglot.parse_one(
            SQL_DESCRIBE_TABLE.substitute(catalog=catalog, schema=schema, table=table.name),
            read="duckdb",
        )

    return expression


def drop_schema_cascade(expression: exp.Expression) -> exp.Expression:
    """Drop schema cascade.

    By default duckdb won't delete a schema if it contains tables, whereas snowflake will.
    So we add the cascade keyword to mimic snowflake's behaviour.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("DROP SCHEMA schema1").transform(remove_comment).sql()
        'DROP SCHEMA schema1 cascade'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if (
        not isinstance(expression, exp.Drop)
        or not (kind := expression.args.get("kind"))
        or not isinstance(kind, str)
        or kind.upper() != "SCHEMA"
    ):
        return expression

    new = expression.copy()
    new.args["cascade"] = True
    return new


def extract_comment_on_columns(expression: exp.Expression) -> exp.Expression:
    """Extract column comments, removing it from the Expression.

    duckdb doesn't support comments. So we remove them from the expression and store them in the column_comment arg.
    We also replace the transform the expression to NOP if the statement can't be executed by duckdb.

    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression, with any comment stored in the new 'table_comment' arg.
    """

    if isinstance(expression, exp.AlterTable) and (actions := expression.args.get("actions")):
        new_actions: list[exp.Expression] = []
        col_comments: list[tuple[str, str]] = []
        for a in actions:
            if isinstance(a, exp.AlterColumn) and (comment := a.args.get("comment")):
                col_comments.append((a.name, comment.this))
            else:
                new_actions.append(a)
        if not new_actions:
            expression = SUCCESS_NOP.copy()
        else:
            expression.set("actions", new_actions)
        expression.args["col_comments"] = col_comments

    return expression


def extract_comment_on_table(expression: exp.Expression) -> exp.Expression:
    """Extract table comment, removing it from the Expression.

    duckdb doesn't support comments. So we remove them from the expression and store them in the table_comment arg.
    We also replace the transform the expression to NOP if the statement can't be executed by duckdb.

    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression, with any comment stored in the new 'table_comment' arg.
    """

    if isinstance(expression, exp.Create) and (table := expression.find(exp.Table)):
        comment = None
        if props := cast(exp.Properties, expression.args.get("properties")):
            other_props = []
            for p in props.expressions:
                if isinstance(p, exp.SchemaCommentProperty) and (isinstance(p.this, (exp.Literal, exp.Identifier))):
                    comment = p.this.this
                else:
                    other_props.append(p)

            new = expression.copy()
            new_props: exp.Properties = new.args["properties"]
            new_props.set("expressions", other_props)
            new.args["table_comment"] = (table, comment)
            return new
    elif (
        isinstance(expression, exp.Comment)
        and (cexp := expression.args.get("expression"))
        and (table := expression.find(exp.Table))
    ):
        new = SUCCESS_NOP.copy()
        new.args["table_comment"] = (table, cexp.this)
        return new
    elif (
        isinstance(expression, exp.AlterTable)
        and (sexp := expression.find(exp.Set))
        and not sexp.args["tag"]
        and (eq := sexp.find(exp.EQ))
        and (eid := eq.find(exp.Identifier))
        and isinstance(eid.this, str)
        and eid.this.upper() == "COMMENT"
        and (lit := eq.find(exp.Literal))
        and (table := expression.find(exp.Table))
    ):
        new = SUCCESS_NOP.copy()
        new.args["table_comment"] = (table, lit.this)
        return new

    return expression


def extract_text_length(expression: exp.Expression) -> exp.Expression:
    """Extract length of text columns.

    duckdb doesn't have fixed-sized text types. So we capture the size of text types and store that in the
    character_maximum_length arg.

    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The original expression, with any text lengths stored in the new 'text_lengths' arg.
    """

    if isinstance(expression, (exp.Create, exp.AlterTable)):
        text_lengths = []
        for dt in expression.find_all(exp.DataType):
            if dt.this in (exp.DataType.Type.VARCHAR, exp.DataType.Type.TEXT):
                col_name = dt.parent and dt.parent.this and dt.parent.this.this
                if dt_size := dt.find(exp.DataTypeParam):
                    size = (
                        isinstance(dt_size.this, exp.Literal)
                        and isinstance(dt_size.this.this, str)
                        and int(dt_size.this.this)
                    )
                else:
                    size = 16777216
                text_lengths.append((col_name, size))

        if text_lengths:
            expression.args["text_lengths"] = text_lengths

    return expression


def flatten(expression: exp.Expression) -> exp.Expression:
    """Flatten an array.

    See https://docs.snowflake.com/en/sql-reference/functions/flatten

    TODO: return index.
    TODO: support objects.
    """
    if (
        isinstance(expression, exp.Lateral)
        and isinstance(expression.this, exp.Explode)
        and (alias := expression.args.get("alias"))
        # always true; when no explicit alias provided this will be _flattened
        and isinstance(alias, exp.TableAlias)
    ):
        explode_expression = expression.this.this.expression

        return exp.Lateral(
            this=exp.Unnest(
                expressions=[
                    exp.Cast(
                        this=explode_expression,
                        to=exp.DataType(
                            this=exp.DataType.Type.ARRAY,
                            expressions=[exp.DataType(this=exp.DataType.Type.JSON, nested=False, prefix=False)],
                            nested=True,
                        ),
                    )
                ],
            ),
            alias=exp.TableAlias(this=alias.this, columns=[exp.Identifier(this="VALUE", quoted=False)]),
        )

    return expression


def float_to_double(expression: exp.Expression) -> exp.Expression:
    """Convert float to double for 64 bit precision.

    Snowflake floats are all 64 bit (ie: double)
    see https://docs.snowflake.com/en/sql-reference/data-types-numeric#float-float4-float8
    """

    if isinstance(expression, exp.DataType) and expression.this == exp.DataType.Type.FLOAT:
        expression.args["this"] = exp.DataType.Type.DOUBLE

    return expression


def identifier(expression: exp.Expression) -> exp.Expression:
    """Convert identifier function to an identifier.

    See https://docs.snowflake.com/en/sql-reference/identifier-literal
    """

    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() == "IDENTIFIER"
    ):
        expression = exp.Identifier(this=expression.expressions[0].this, quoted=False)

    return expression


def indices_to_json_extract(expression: exp.Expression) -> exp.Expression:
    """Convert indices on objects and arrays to json_extract.

    Supports Snowflake array indices, see
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured#accessing-elements-of-an-array-by-index-or-by-slice
    and object indices, see
    https://docs.snowflake.com/en/sql-reference/data-types-semistructured#accessing-elements-of-an-object-by-key

    Duckdb uses the -> operator, aka the json_extract function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions

    This works for Snowflake arrays too because we convert them to JSON in duckdb.
    """
    if (
        isinstance(expression, exp.Bracket)
        and len(expression.expressions) == 1
        and (index := expression.expressions[0])
        and isinstance(index, exp.Literal)
        and index.this
    ):
        if index.is_string:
            return exp.JSONExtract(this=expression.this, expression=exp.Literal(this=f"$.{index.this}", is_string=True))
        else:
            return exp.JSONExtract(
                this=expression.this, expression=exp.Literal(this=f"$[{index.this}]", is_string=True)
            )

    return expression


def information_schema_fs_columns_snowflake(expression: exp.Expression) -> exp.Expression:
    """Redirect to the information_schema._fs_columns_snowflake view which has metadata that matches snowflake.

    Because duckdb doesn't store character_maximum_length or character_octet_length.
    """

    if (
        isinstance(expression, exp.Select)
        and (tbl_exp := expression.find(exp.Table))
        and tbl_exp.name.upper() == "COLUMNS"
        and tbl_exp.db.upper() == "INFORMATION_SCHEMA"
    ):
        tbl_exp.set("this", exp.Identifier(this="_FS_COLUMNS_SNOWFLAKE", quoted=False))

    return expression


def information_schema_fs_tables_ext(expression: exp.Expression) -> exp.Expression:
    """Join to information_schema._fs_tables_ext to access additional metadata columns (eg: comment)."""

    if (
        isinstance(expression, exp.Select)
        and (tbl_exp := expression.find(exp.Table))
        and tbl_exp.name.upper() == "TABLES"
        and tbl_exp.db.upper() == "INFORMATION_SCHEMA"
    ):
        return expression.join(
            "information_schema._fs_tables_ext",
            on=(
                """
                tables.table_catalog = _fs_tables_ext.ext_table_catalog AND
                tables.table_schema = _fs_tables_ext.ext_table_schema AND
                tables.table_name = _fs_tables_ext.ext_table_name
                """
            ),
            join_type="left",
        )

    return expression


def integer_precision(expression: exp.Expression) -> exp.Expression:
    """Convert integers to bigint.

    So dataframes will return them with a dtype of int64.
    """

    if (
        isinstance(expression, exp.DataType)
        and (expression.this == exp.DataType.Type.DECIMAL and not expression.expressions)
        or expression.this in (exp.DataType.Type.INT, exp.DataType.Type.SMALLINT, exp.DataType.Type.TINYINT)
    ):
        return exp.DataType(
            this=exp.DataType.Type.BIGINT,
            nested=False,
            prefix=False,
        )

    return expression


def json_extract_cased_as_varchar(expression: exp.Expression) -> exp.Expression:
    """Convert json to varchar inside JSONExtract.

    Snowflake case conversion (upper/lower) turns variant into varchar. This
    mimics that behaviour within get_path.

    TODO: a generic version that works on any variant, not just JSONExtract

    Returns a raw string using the Duckdb ->> operator, aka the json_extract_string function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions
    """
    if (
        isinstance(expression, (exp.Upper, exp.Lower))
        and (gp := expression.this)
        and isinstance(gp, exp.JSONExtract)
        and (path := gp.expression)
        and isinstance(path, exp.JSONPath)
    ):
        expression.set("this", exp.JSONExtractScalar(this=gp.this, expression=path))

    return expression


def json_extract_cast_as_varchar(expression: exp.Expression) -> exp.Expression:
    """Return raw unquoted string when casting json extraction to varchar.

    Returns a raw string using the Duckdb ->> operator, aka the json_extract_string function, see
    https://duckdb.org/docs/extensions/json#json-extraction-functions
    """
    if (
        isinstance(expression, exp.Cast)
        and (je := expression.this)
        and isinstance(je, exp.JSONExtract)
        and (path := je.expression)
        and isinstance(path, exp.JSONPath)
    ):
        return exp.JSONExtractScalar(this=je.this, expression=path)
    return expression


def random(expression: exp.Expression) -> exp.Expression:
    """Convert random() and random(seed).

    Snowflake random() is an signed 64 bit integer.
    Duckdb random() is a double between 0 and 1 and uses setseed() to set the seed.
    """
    if isinstance(expression, exp.Select) and (rand := expression.find(exp.Rand)):
        # shift result to between min and max signed 64bit integer
        new_rand = exp.Cast(
            this=exp.Paren(
                this=exp.Mul(
                    this=exp.Paren(this=exp.Sub(this=exp.Rand(), expression=exp.Literal(this=0.5, is_string=False))),
                    expression=exp.Literal(this=9223372036854775807, is_string=False),
                )
            ),
            to=exp.DataType(this=exp.DataType.Type.BIGINT, nested=False, prefix=False),
        )

        rand.replace(new_rand)

        # convert seed to double between 0 and 1 by dividing by max INTEGER (int32)
        # (not max BIGINT (int64) because we don't have enough floating point precision to distinguish seeds)
        # then attach to SELECT as the seed arg
        # (we can't attach it to exp.Rand because it will be rendered in the sql)
        if rand.this and isinstance(rand.this, exp.Literal):
            expression.args["seed"] = f"{rand.this}/2147483647-0.5"

    return expression


def sample(expression: exp.Expression) -> exp.Expression:
    if isinstance(expression, exp.TableSample) and not expression.args.get("method"):
        # set snowflake default (bernoulli) rather than use the duckdb default (system)
        # because bernoulli works better at small row sizes like we have in tests
        expression.set("method", exp.Var(this="BERNOULLI"))

    return expression


def object_construct(expression: exp.Expression) -> exp.Expression:
    """Convert OBJECT_CONSTRUCT to TO_JSON.

    Internally snowflake stores OBJECT types as a json string, so the Duckdb JSON type most closely matches.

    See https://docs.snowflake.com/en/sql-reference/functions/object_construct
    """

    if isinstance(expression, exp.Struct):
        # remove expressions containing NULL
        for enull in expression.find_all(exp.Null):
            if enull.parent:
                enull.parent.pop()

        return exp.Anonymous(this="TO_JSON", expressions=[expression])

    return expression


def parse_json(expression: exp.Expression) -> exp.Expression:
    """Convert parse_json() to json().

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("insert into table1 (name) select parse_json('{}')").transform(parse_json).sql()
        "CREATE TABLE table1 (name JSON)"
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() == "PARSE_JSON"
    ):
        new = expression.copy()
        new.args["this"] = "JSON"
        return new

    return expression


def regex_replace(expression: exp.Expression) -> exp.Expression:
    """Transform regex_replace expressions from snowflake to duckdb."""

    if isinstance(expression, exp.RegexpReplace) and isinstance(expression.expression, exp.Literal):
        if len(expression.args) > 3:
            # see https://docs.snowflake.com/en/sql-reference/functions/regexp_replace
            raise NotImplementedError(
                "REGEXP_REPLACE with additional parameters (eg: <position>, <occurrence>, <parameters>) not supported"
            )

        # pattern: snowflake requires escaping backslashes in single-quoted string constants, but duckdb doesn't
        # see https://docs.snowflake.com/en/sql-reference/functions-regexp#label-regexp-escape-character-caveats
        expression.args["expression"] = exp.Literal(
            this=expression.expression.this.replace("\\\\", "\\"), is_string=True
        )

        if not expression.args.get("replacement"):
            # if no replacement string, the snowflake default is ''
            expression.args["replacement"] = exp.Literal(this="", is_string=True)

        # snowflake regex replacements are global
        expression.args["modifiers"] = exp.Literal(this="g", is_string=True)

    return expression


def regex_substr(expression: exp.Expression) -> exp.Expression:
    """Transform regex_substr expressions from snowflake to duckdb.

    See https://docs.snowflake.com/en/sql-reference/functions/regexp_substr
    """

    if isinstance(expression, exp.RegexpExtract):
        subject = expression.this

        # pattern: snowflake requires escaping backslashes in single-quoted string constants, but duckdb doesn't
        # see https://docs.snowflake.com/en/sql-reference/functions-regexp#label-regexp-escape-character-caveats
        pattern = expression.expression
        pattern.args["this"] = pattern.this.replace("\\\\", "\\")

        # number of characters from the beginning of the string where the function starts searching for matches
        try:
            position = expression.args["position"]
        except KeyError:
            position = exp.Literal(this="1", is_string=False)

        # which occurrence of the pattern to match
        try:
            occurrence = int(expression.args["occurrence"].this)
        except KeyError:
            occurrence = 1

        # the duckdb dialect increments bracket (ie: index) expressions by 1 because duckdb is 1-indexed,
        # so we need to compensate by subtracting 1
        occurrence = exp.Literal(this=str(occurrence - 1), is_string=False)

        try:
            regex_parameters_value = str(expression.args["parameters"].this)
            # 'e' parameter doesn't make sense for duckdb
            regex_parameters = exp.Literal(this=regex_parameters_value.replace("e", ""), is_string=True)
        except KeyError:
            regex_parameters = exp.Literal(is_string=True)

        try:
            group_num = expression.args["group"]
        except KeyError:
            if isinstance(regex_parameters.this, str) and "e" in regex_parameters.this:
                group_num = exp.Literal(this="1", is_string=False)
            else:
                group_num = exp.Literal(this="0", is_string=False)

        expression = exp.Bracket(
            this=exp.Anonymous(
                this="regexp_extract_all",
                expressions=[
                    # slice subject from position onwards
                    exp.Bracket(this=subject, expressions=[exp.Slice(this=position)]),
                    pattern,
                    group_num,
                    regex_parameters,
                ],
            ),
            # select index of occurrence
            expressions=[occurrence],
        )

    return expression


# TODO: move this into a Dialect as a transpilation
def set_schema(expression: exp.Expression, current_database: str | None) -> exp.Expression:
    """Transform USE SCHEMA/DATABASE to SET schema.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("USE SCHEMA bar").transform(set_schema, current_database="foo").sql()
        "SET schema = 'foo.bar'"
        >>> sqlglot.parse_one("USE SCHEMA foo.bar").transform(set_schema).sql()
        "SET schema = 'foo.bar'"
        >>> sqlglot.parse_one("USE DATABASE marts").transform(set_schema).sql()
        "SET schema = 'marts.main'"

        See tests for more examples.
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: A SET schema expression if the input is a USE
            expression, otherwise expression is returned as-is.
    """

    if (
        isinstance(expression, exp.Use)
        and (kind := expression.args.get("kind"))
        and isinstance(kind, exp.Var)
        and kind.name
        and kind.name.upper() in ["SCHEMA", "DATABASE"]
    ):
        assert expression.this, f"No identifier for USE expression {expression}"

        if kind.name.upper() == "DATABASE":
            # duckdb's default schema is main
            name = f"{expression.this.name}.main"
        else:
            # SCHEMA
            if db := expression.this.args.get("db"):  # noqa: SIM108
                db_name = db.name
            else:
                # isn't qualified with a database
                db_name = current_database or MISSING_DATABASE

            name = f"{db_name}.{expression.this.name}"

        return exp.Command(this="SET", expression=exp.Literal.string(f"schema = '{name}'"))

    return expression


SQL_SHOW_OBJECTS = """
select
    to_timestamp(0)::timestamptz as 'created_on',
    table_name as 'name',
    case when table_type='BASE TABLE' then 'TABLE' else table_type end as 'kind',
    table_catalog as 'database_name',
    table_schema as 'schema_name'
from information_schema.tables
"""


def show_objects_tables(expression: exp.Expression, current_database: str | None = None) -> exp.Expression:
    """Transform SHOW OBJECTS/TABLES to a query against the information_schema.tables table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-objects
        https://docs.snowflake.com/en/sql-reference/sql/show-tables
    """
    if (
        isinstance(expression, exp.Show)
        and isinstance(expression.this, str)
        and expression.this.upper() in ["OBJECTS", "TABLES"]
    ):
        scope_kind = expression.args.get("scope_kind")
        table = expression.find(exp.Table)

        if scope_kind == "DATABASE":
            catalog = (table and table.name) or current_database
            schema = None
        elif scope_kind == "SCHEMA" and table:
            catalog = table.db or current_database
            schema = table.name
        else:
            # all objects / tables
            catalog = None
            schema = None

        tables_only = "table_type = 'BASE TABLE' and " if expression.this.upper() == "TABLES" else ""
        exclude_fakesnow_tables = "not (table_schema == 'information_schema' and table_name like '_fs_%%')"
        # without a database will show everything in the "account"
        table_catalog = f" and table_catalog = '{catalog}'" if catalog else ""
        schema = f" and table_schema = '{schema}'" if schema else ""
        limit = limit.sql() if (limit := expression.args.get("limit")) and isinstance(limit, exp.Expression) else ""

        return sqlglot.parse_one(
            f"{SQL_SHOW_OBJECTS} where {tables_only}{exclude_fakesnow_tables}{table_catalog}{schema}{limit}",
            read="duckdb",
        )

    return expression


SQL_SHOW_SCHEMAS = """
select
    to_timestamp(0)::timestamptz as 'created_on',
    schema_name as 'name',
    NULL as 'kind',
    catalog_name as 'database_name',
    NULL as 'schema_name'
from information_schema.schemata
where catalog_name not in ('memory', 'system', 'temp') and schema_name not in ('main', 'pg_catalog')
"""


def show_schemas(expression: exp.Expression, current_database: str | None = None) -> exp.Expression:
    """Transform SHOW SCHEMAS to a query against the information_schema.schemata table.

    See https://docs.snowflake.com/en/sql-reference/sql/show-schemas
    """
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "SCHEMAS":
        if (ident := expression.find(exp.Identifier)) and isinstance(ident.this, str):
            database = ident.this
        else:
            database = current_database

        return sqlglot.parse_one(
            f"{SQL_SHOW_SCHEMAS} and catalog_name = '{database}'" if database else SQL_SHOW_SCHEMAS, read="duckdb"
        )

    return expression


def tag(expression: exp.Expression) -> exp.Expression:
    """Handle tags. Transfer tags into upserts of the tag table.

    duckdb doesn't support tags. In lieu of a full implementation, for now we make it a NOP.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("ALTER TABLE table1 SET TAG foo='bar'").transform(tag).sql()
        "SELECT 'Statement executed successfully.'"
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if isinstance(expression, exp.AlterTable) and (actions := expression.args.get("actions")):
        for a in actions:
            if isinstance(a, exp.Set) and a.args["tag"]:
                return SUCCESS_NOP
    elif (
        isinstance(expression, exp.Command)
        and (cexp := expression.args.get("expression"))
        and isinstance(cexp, str)
        and "SET TAG" in cexp.upper()
    ):
        # alter table modify column set tag
        return SUCCESS_NOP

    return expression


def to_date(expression: exp.Expression) -> exp.Expression:
    """Convert to_date() to a cast.

    See https://docs.snowflake.com/en/sql-reference/functions/to_date

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT to_date(to_timestamp(0))").transform(to_date).sql()
        "SELECT CAST(DATE_TRUNC('day', TO_TIMESTAMP(0)) AS DATE)"
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() == "TO_DATE"
    ):
        return exp.Cast(
            this=expression.expressions[0],
            to=exp.DataType(this=exp.DataType.Type.DATE, nested=False, prefix=False),
        )
    return expression


def to_decimal(expression: exp.Expression) -> exp.Expression:
    """Transform to_decimal, to_number, to_numeric expressions from snowflake to duckdb.

    See https://docs.snowflake.com/en/sql-reference/functions/to_decimal
    """

    if (
        isinstance(expression, exp.Anonymous)
        and isinstance(expression.this, str)
        and expression.this.upper() in ["TO_DECIMAL", "TO_NUMBER", "TO_NUMERIC"]
    ):
        expressions: list[exp.Expression] = expression.expressions

        if len(expressions) > 1 and expressions[1].is_string:
            # see https://docs.snowflake.com/en/sql-reference/functions/to_decimal#arguments
            raise NotImplementedError(f"{expression.this} with format argument")

        precision = expressions[1] if len(expressions) > 1 else exp.Literal(this="38", is_string=False)
        scale = expressions[2] if len(expressions) > 2 else exp.Literal(this="0", is_string=False)

        return exp.Cast(
            this=expressions[0],
            to=exp.DataType(this=exp.DataType.Type.DECIMAL, expressions=[precision, scale], nested=False, prefix=False),
        )

    return expression


def to_timestamp(expression: exp.Expression) -> exp.Expression:
    """Convert to_timestamp(seconds) to timestamp without timezone (ie: TIMESTAMP_NTZ).

    See https://docs.snowflake.com/en/sql-reference/functions/to_timestamp
    """

    if isinstance(expression, exp.UnixToTime):
        return exp.Cast(
            this=expression,
            to=exp.DataType(this=exp.DataType.Type.TIMESTAMP, nested=False, prefix=False),
        )
    return expression


def to_timestamp_ntz(expression: exp.Expression) -> exp.Expression:
    """Convert to_timestamp_ntz to to_timestamp (StrToTime).

    Because it's not yet supported by sqlglot, see https://github.com/tobymao/sqlglot/issues/2748
    """

    if isinstance(expression, exp.Anonymous) and (
        isinstance(expression.this, str) and expression.this.upper() == "TO_TIMESTAMP_NTZ"
    ):
        return exp.StrToTime(
            this=expression.expressions[0],
            format=exp.Literal(this="%Y-%m-%d %H:%M:%S", is_string=True),
        )
    return expression


def timestamp_ntz_ns(expression: exp.Expression) -> exp.Expression:
    """Convert timestamp_ntz(9) to timestamp_ntz.

    To compensate for https://github.com/duckdb/duckdb/issues/7980
    """

    if (
        isinstance(expression, exp.DataType)
        and expression.this == exp.DataType.Type.TIMESTAMP
        and exp.DataTypeParam(this=exp.Literal(this="9", is_string=False)) in expression.expressions
    ):
        new = expression.copy()
        del new.args["expressions"]
        return new

    return expression


# sqlglot.parse_one("create table example(date TIMESTAMP_NTZ(9));", read="snowflake")
def semi_structured_types(expression: exp.Expression) -> exp.Expression:
    """Convert OBJECT, ARRAY, and VARIANT types to duckdb compatible types.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("CREATE TABLE table1 (name object)").transform(semi_structured_types).sql()
        "CREATE TABLE table1 (name JSON)"
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if isinstance(expression, exp.DataType) and expression.this in [
        exp.DataType.Type.ARRAY,
        exp.DataType.Type.OBJECT,
        exp.DataType.Type.VARIANT,
    ]:
        new = expression.copy()
        new.args["this"] = exp.DataType.Type.JSON
        return new

    return expression


def upper_case_unquoted_identifiers(expression: exp.Expression) -> exp.Expression:
    """Upper case unquoted identifiers.

    Snowflake represents case-insensitivity using upper-case identifiers in cursor results.
    duckdb uses lowercase. We convert all unquoted identifiers to uppercase to match snowflake.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("select name, name as fname from table1").transform(upper_case_unquoted_identifiers).sql()
        'SELECT NAME, NAME AS FNAME FROM TABLE1'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    if isinstance(expression, exp.Identifier) and not expression.quoted and isinstance(expression.this, str):
        new = expression.copy()
        new.set("this", expression.this.upper())
        return new

    return expression


def values_columns(expression: exp.Expression) -> exp.Expression:
    """Support column1, column2 expressions in VALUES.

    Snowflake uses column1, column2 .. for unnamed columns in VALUES. Whereas duckdb uses col0, col1 ..
    See https://docs.snowflake.com/en/sql-reference/constructs/values#examples
    """

    if (
        isinstance(expression, exp.Values)
        and not expression.alias
        and expression.find_ancestor(exp.Select)
        and (values := expression.find(exp.Tuple))
    ):
        num_columns = len(values.expressions)
        columns = [exp.Identifier(this=f"COLUMN{i + 1}", quoted=True) for i in range(num_columns)]
        expression.set("alias", exp.TableAlias(this=exp.Identifier(this="_", quoted=False), columns=columns))

    return expression


def show_users(expression: exp.Expression) -> exp.Expression:
    """Transform SHOW USERS to a query against the global database's information_schema._fs_users table.

    https://docs.snowflake.com/en/sql-reference/sql/show-users
    """
    if isinstance(expression, exp.Show) and isinstance(expression.this, str) and expression.this.upper() == "USERS":
        return sqlglot.parse_one(f"SELECT * FROM {USERS_TABLE_FQ_NAME}", read="duckdb")

    return expression


def create_user(expression: exp.Expression) -> exp.Expression:
    """Transform CREATE USER to a query against the global database's information_schema._fs_users table.

    https://docs.snowflake.com/en/sql-reference/sql/create-user
    """
    # XXX: this is a placeholder. We need to implement the full CREATE USER syntax, but
    #      sqlglot doesnt yet support Create for snowflake.
    if isinstance(expression, exp.Command) and expression.this == "CREATE":
        sub_exp = expression.expression.strip()
        if sub_exp.upper().startswith("USER"):
            _, name, *ignored = sub_exp.split(" ")
            if ignored:
                raise NotImplementedError(f"`CREATE USER` with {ignored} not yet supported")
            return sqlglot.parse_one(f"INSERT INTO {USERS_TABLE_FQ_NAME} (name) VALUES ('{name}')", read="duckdb")

    return expression

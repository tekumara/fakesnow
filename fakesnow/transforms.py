from __future__ import annotations
from typing import Optional

from sqlglot import exp
import snowflake.connector.errors


def qualified_schema(expression: exp.Expression, database: Optional[str]) -> exp.Expression:
    """Qualify table expressions with their database and schema.

    Database names become a prefix of the schema. Needed to support the use of multiple Snowflake
    databases within in a single duckdb database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT * FROM prod.staging.jaffles").transform(qualified_schema).sql()
        'SELECT * FROM prod_staging.jaffles'
        >>> sqlglot.parse_one("CREATE SCHEMA prod.staging").transform(qualified_schema).sql()
        'CREATE SCHEMA prod_staging'
        >>> sqlglot.parse_one("SELECT * FROM staging.jaffles").transform(qualified_schema, database="prod").sql()
        'SELECT * FROM prod_staging.jaffles'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform_table(node: exp.Table) -> exp.Table:
        # sqlglot catalog = snowflake database
        # sqlglot db = snowflake schema

        if not node.parent:
            raise Exception(f"No parent for table expression {node.sql()}")

        if (kind := node.parent.args.get("kind", None)) and isinstance(kind, str) and kind.upper() == "SCHEMA":
            if "db" not in node.args or node.args["db"] is None:
                # "schema" expression isn't qualified with a database
                if not database:
                    raise snowflake.connector.errors.ProgrammingError(
                        msg=f"Cannot perform {node.parent.key.upper()} SCHEMA. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",
                        errno=90105,
                        sqlstate="22000",
                    )
                return node

            name = node.args["db"].name + "_" + node.args["this"].name
            eid: exp.Identifier = node.args["this"]
            nid = exp.Identifier(**{**eid.args, "this": name})

            return exp.Table(**{**node.args, "db": None, "name": name, "this": nid})

        if "catalog" not in node.args or node.args["catalog"] is None:
            # table expression isn't qualified with a catalog
            if not database and (grandparent := node.parent.parent) and grandparent.key.upper() != "SELECT":
                raise snowflake.connector.errors.ProgrammingError(
                    msg=f"Cannot perform {grandparent.key.upper()} TABLE. This session does not have a current database. Call 'USE DATABASE', or use a qualified name.",
                    errno=90105,
                    sqlstate="22000",
                )
            return node

        new_db_name = node.args["catalog"].name + "_" + node.args["db"].name

        eid: exp.Identifier = node.args["db"]
        nid = exp.Identifier(**{**eid.args, "this": new_db_name})

        return exp.Table(**{**node.args, "catalog": None, "db": nid})

    # transform all table expressions
    # NB: sqlglot treats "identifier" in "create schema identifier" as a part of a table expression
    return expression.transform(
        lambda node: transform_table(node) if isinstance(node, exp.Table) else node,
    )


# TODO: move this into a Dialect as a transpilation
def set_schema(expression: exp.Expression) -> exp.Expression:
    """Transform use schema to set schema.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql()
        'SET schema = foo'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform_use(node: exp.Use) -> exp.Command | exp.Use:
        if (
            (kind := node.args.get("kind", None))
            and isinstance(kind, exp.Var)
            and kind.name
            and kind.name.upper() == "SCHEMA"
        ):
            if not node.this:
                raise Exception(f"No identifier for USE expression {node}")

            name = node.this.name

            return exp.Command(this="SET", expression=exp.Literal.string(f"schema = {name}"))

        return node

    return expression.transform(
        lambda node: transform_use(node) if isinstance(node, exp.Use) else node,
    )


def as_describe(expression: exp.Expression) -> exp.Expression:
    """Prepend describe to the expression.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT name FROM CUSTOMERS").transform(as_describe).sql()
        'describe SELECT name FROM CUSTOMERS'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    return exp.Describe(this=expression)

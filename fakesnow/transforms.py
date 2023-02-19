from __future__ import annotations

import re
from typing import Optional, cast

from sqlglot import exp

MISSING_SCHEMA = "unqualified_and_no_schema_set"


def database_prefix(
    expression: exp.Expression, current_database: Optional[str] = None, schema_set: bool = False
) -> exp.Expression:
    """Prefix schemas with the database used in the expression or the current database if none.

    Needed to support the use of multiple Snowflake databases within in a single duckdb database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix, database="marts").sql()
        'SELECT * FROM marts.jaffles.customers'
        >>> sqlglot.parse_one("SELECT * FROM marts.jaffles.customers").transform(database_prefix).sql()
        'SELECT * FROM marts.jaffles.customers'

        See tests for more examples.
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

        if (parent_kind := node.parent.args.get("kind")) and (
            # "DROP/CREATE SCHEMA"
            (isinstance(parent_kind, str) and parent_kind.upper() == "SCHEMA")
            # "USE SCHEMA"
            or (isinstance(parent_kind, exp.Var) and parent_kind.name.upper() == "SCHEMA")
        ):

            if db := node.args.get("db"):
                db_name = db.name
            else:
                # schema expression isn't qualified with a database
                db_name = current_database or MISSING_SCHEMA

            name = f"{db_name}.{node.args['this'].name}"

            eid: exp.Identifier = node.args["this"]
            nid = exp.Identifier(**{**eid.args, "this": name})

            return exp.Table(**{**node.args, "db": None, "name": name, "this": nid})

        # TABLE expression, eg: "SELECT * FROM identifier", or "CREATE TABLE ..."

        if not (db := node.args.get("db")):
            # no schema
            if node.parent.key == "use" or schema_set:
                # no problem, search is set
                return node
            else:
                return exp.Table(**{**node.args, "catalog": MISSING_SCHEMA})

        if catalog := node.args.get("catalog"):
            catalog_name = catalog.name
        else:
            catalog_name = current_database or MISSING_SCHEMA

        new_db_name = f"{catalog_name}.{db.name}"

        eid: exp.Identifier = node.args["db"]
        nid = exp.Identifier(**{**eid.args, "this": new_db_name})

        return exp.Table(**{**node.args, "catalog": None, "db": nid})

    # transform all table expressions
    return expression.transform(
        lambda node: transform_table(node) if isinstance(node, exp.Table) else node,
    )


# TODO: move this into a Dialect as a transpilation
def set_schema(expression: exp.Expression) -> exp.Expression:
    """Transform use schema to set schema.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("USE SCHEMA foo").transform(set_schema).sql()
        'USE foo'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform_use(node: exp.Use) -> exp.Use:
        if (
            (kind := node.args.get("kind"))
            and isinstance(kind, exp.Var)
            and kind.name
            and kind.name.upper() == "SCHEMA"
        ):
            assert node.this, f"No identifier for USE expression {node}"

            args = {k: v for k, v in node.args.items() if k != "kind"}
            return exp.Use(**args)

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


# TODO: move this into a Dialect as a transpilation
def create_database(expression: exp.Expression) -> exp.Expression:
    """Transform create database to attach database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("CREATE database foo").transform(create_database).sql()
        'ATTACH database ':memory:' as foo'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform_create_db(node: exp.Command) -> exp.Command:
        if match := re.search("create database (\\w+)", cast(str, node.this), flags=re.IGNORECASE):
            db_name = match[1]
            return exp.Command(
                this="ATTACH", expression=exp.Literal(this=f"DATABASE ':memory:' AS {db_name}", is_string=True)
            )
        else:
            return node

    return expression.transform(
        lambda node: transform_create_db(node) if isinstance(node, exp.Command) else node,
    )

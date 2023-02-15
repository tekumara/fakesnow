from __future__ import annotations

from typing import Optional

from sqlglot import exp

MISSING_DATABASE = "unqualified_and_no_current_database"

MISSING_SCHEMA = "unqualified_and_no_current_schema"


def database_prefix(
    expression: exp.Expression, current_database: Optional[str] = None, current_schema: Optional[str] = None
) -> exp.Expression:
    """Prefix schemas with the database used in the expression or the current database if none.

    Needed to support the use of multiple Snowflake databases within in a single duckdb database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT * FROM jaffles.customers").transform(database_prefix, database="marts").sql()
        'SELECT * FROM marts_jaffles.customers'
        >>> sqlglot.parse_one("SELECT * FROM marts.jaffles.customers").transform(database_prefix).sql()
        'SELECT * FROM marts_jaffles.customers'

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
                db_name = current_database or MISSING_DATABASE

            name = f"{db_name}_{node.args['this'].name}"

            eid: exp.Identifier = node.args["this"]
            nid = exp.Identifier(**{**eid.args, "this": name})

            return exp.Table(**{**node.args, "db": None, "name": name, "this": nid})

        # TABLE expression, eg: "SELECT * FROM identifier", or "CREATE TABLE ..."

        if not (db := node.args.get("db")):
            # no schema
            if node.parent.key == "use" or (current_database and current_schema):
                # no problem, search path contains current database
                return node
            elif not current_database:
                return exp.Table(**{**node.args, "catalog": MISSING_DATABASE})
            else:
                return exp.Table(**{**node.args, "catalog": MISSING_SCHEMA})

        if catalog := node.args.get("catalog"):
            catalog_name = catalog.name
        else:
            catalog_name = current_database or MISSING_DATABASE

        # TODO: use quoted . like snowflake does
        new_db_name = f"{catalog_name}_{db.name}"

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
        'SET schema = foo'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform_use(node: exp.Use) -> exp.Command | exp.Use:
        if (
            (kind := node.args.get("kind"))
            and isinstance(kind, exp.Var)
            and kind.name
            and kind.name.upper() == "SCHEMA"
        ):
            assert node.this, f"No identifier for USE expression {node}"

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

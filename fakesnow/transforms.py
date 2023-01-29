from sqlglot import parse_one, exp


def database_prefix(expression: exp.Expression) -> exp.Expression:
    """Replace references to databases with a prefixed version of the table or schema.

    Needed to support use of multiple Snowflake databases in duckdb.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT * FROM staging.jaffles").transform(database_prefix).sql()
        'SELECT * FROM staging_jaffles'
        >>> sqlglot.parse_one("SELECT * FROM staging.jaffles AS jaffs").transform(database_prefix).sql()
        'SELECT * FROM staging_jaffles'
        >>> sqlglot.parse_one("CREATE SCHEMA IF NOT EXISTS staging.jaffles").transform(database_prefix).sql()
        'CREATE SCHEMA IF NOT EXISTS staging_jaffles'
    Args:
        expression (exp.Expression): the expression that will be transformed.

    Returns:
        exp.Expression: The transformed expression.
    """

    def transform(node: exp.Table) -> exp.Table:
        if "db" not in node.args or node.args['db'] is None:
            # table expression isn't qualified with a database
            return node

        name = node.args["db"].name + "_" + node.args["this"].name

        eid: exp.Identifier = node.args["this"]
        nid = exp.Identifier(**{**eid.args, "this": name})
        # nid.set()

        return exp.Table(**{**node.args, "db": None, "name": name, "this": nid})

    # transform all table expressions
    # NB: sqlglot treats "identifier" in "create schema identifier" as a part of a table expression
    return expression.transform(
        lambda node: transform(node) if isinstance(node, exp.Table) else node,
    )

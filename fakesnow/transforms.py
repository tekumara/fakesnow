from sqlglot import exp


def database_as_schema(expression: exp.Expression) -> exp.Expression:
    """Replace references to databases with a prefixed version of the schema.

    Needed to support the use of multiple Snowflake databases within in a single duckdb database.

    Example:
        >>> import sqlglot
        >>> sqlglot.parse_one("SELECT * FROM prod.staging.jaffles").transform(database_prefix).sql()
        'SELECT * FROM prod_staging.jaffles'
        >>> sqlglot.parse_one("CREATE SCHEMA prod.staging").transform(database_prefix).sql()
        'CREATE SCHEMA prod_staging'
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

        if "kind" in node.parent.args and node.parent.args['kind'].upper() == "SCHEMA":
            if "db" not in node.args or node.args['db'] is None:
                # "schema" expression isn't qualified with a database
                return node

            name = node.args["db"].name + "_" + node.args["this"].name
            eid: exp.Identifier = node.args["this"]
            nid = exp.Identifier(**{**eid.args, "this": name})

            return exp.Table(**{**node.args, "db": None, "name": name, "this": nid})

        if "catalog" not in node.args or node.args["catalog"] is None:
            # table expression isn't qualified with a catalog
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

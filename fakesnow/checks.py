from __future__ import annotations

from sqlglot import Expr, exp
from sqlglot.optimizer.scope import traverse_scope


def is_unqualified_table_expression(expression: Expr) -> tuple[bool, bool]:
    """Check whether any real table reference is missing a database or schema qualifier.

    For CTEs, we want to validate the underlying base tables, not the outer CTE
    alias itself.
    """

    no_database = False
    no_schema = False

    for table in _relevant_tables(expression):
        missing_database, missing_schema = _missing_qualifiers(table)
        no_database |= missing_database
        no_schema |= missing_schema

    return no_database, no_schema


def _relevant_tables(expression: Expr) -> list[exp.Table]:
    """Collect real tables from query scopes and statement-specific table args."""

    # `traverse_scope` lets us see the actual sources for each SELECT/CTE scope,
    # so `WITH cte AS (SELECT * FROM db.schema.table) SELECT * FROM cte` checks
    # `db.schema.table` rather than the logical `cte` name. Use `sources`
    # rather than `selected_sources`: table functions like
    # `TABLE(FLATTEN(...))` can produce anonymous sources that make
    # `selected_sources` raise `OptimizeError("Alias already used: ")`.
    tables = [
        source
        for scope in traverse_scope(expression)
        for source in scope.sources.values()
        if isinstance(source, exp.Table)
    ]

    # Non-SELECT statements like CREATE TABLE, DROP TABLE, USE SCHEMA, and SHOW
    # keep important table references in direct AST args rather than query scopes.
    this = expression.args.get("this")
    if isinstance(this, exp.Table):
        tables.append(this)
    elif isinstance(this, exp.Schema) and isinstance(this.this, exp.Table):
        tables.append(this.this)

    if isinstance(scope := expression.args.get("scope"), exp.Table):
        tables.append(scope)

    # `USING` may point at either a real table or a CTE alias. Skip CTE names
    # here because they are logical query sources, not missing qualifiers.
    if isinstance(using := expression.args.get("using"), exp.Table) and using.name.upper() not in _cte_names(
        expression
    ):
        tables.append(using)

    return tables


def _cte_names(expression: Expr) -> set[str]:
    with_ = expression.args.get("with_")
    if not isinstance(with_, exp.With):
        return set()

    return {cte.alias_or_name.upper() for cte in with_.expressions}


def _missing_qualifiers(node: exp.Table) -> tuple[bool, bool]:
    """Apply the existing statement-specific qualification rules to one table."""

    assert node.parent, f"No parent for table expression {node.sql()}"

    if (parent_kind := node.parent.args.get("kind")) and isinstance(parent_kind, str):
        if parent_kind.upper() == "DATABASE":
            # "CREATE/DROP DATABASE"
            no_database = False
            no_schema = False
        elif parent_kind.upper() == "SCHEMA":
            # "CREATE/DROP SCHEMA"
            no_database = not node.args.get("catalog")
            no_schema = False
        elif parent_kind.upper() in {"TABLE", "VIEW", "STAGE", "TAG", "SEQUENCE"}:
            # "CREATE/DROP TABLE/VIEW/STAGE/TAG"
            no_database = not node.args.get("catalog")
            no_schema = not node.args.get("db")
        else:
            raise AssertionError(f"Unexpected parent kind: {parent_kind}")

    elif (
        node.parent.key == "use"
        and (parent_kind := node.parent.args.get("kind"))
        and isinstance(parent_kind, exp.Var)
        and parent_kind.name
    ):
        if parent_kind.name.upper() == "DATABASE":
            # "USE DATABASE"
            no_database = False
            no_schema = False
        elif parent_kind.name.upper() == "SCHEMA":
            # "USE SCHEMA"
            no_database = not node.args.get("db")
            no_schema = False
        else:
            raise AssertionError(f"Unexpected parent kind: {parent_kind.name}")

    elif node.parent.key == "show":
        # don't require a database or schema for SHOW
        # TODO: make this more nuanced
        no_database = False
        no_schema = False
    else:
        no_database = not node.args.get("catalog")
        no_schema = not node.args.get("db")

    return no_database, no_schema


def equal(left: exp.Identifier, right: exp.Identifier) -> bool:
    # as per https://docs.snowflake.com/en/sql-reference/identifiers-syntax#label-identifier-casing
    lid = left.this if left.quoted else left.this.upper()
    rid = right.this if right.quoted else right.this.upper()

    return lid == rid

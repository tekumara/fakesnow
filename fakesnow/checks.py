from __future__ import annotations

from collections.abc import Callable, Collection, Mapping

from sqlglot import Expr, exp
from sqlglot.optimizer.scope import Scope, traverse_scope


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

    if isinstance(expression, exp.Drop):
        tables.extend(t for t in expression.args.get("tables") or [] if isinstance(t, exp.Table))

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

    if node.parent.key == "join":
        return not node.args.get("catalog"), not node.args.get("db")

    if (parent_kind := node.parent.args.get("kind")) and isinstance(parent_kind, str):
        if parent_kind.upper() == "DATABASE":
            # "CREATE/DROP DATABASE"
            no_database = False
            no_schema = False
        elif parent_kind.upper() == "SCHEMA":
            # "CREATE/DROP SCHEMA"
            no_database = not node.args.get("catalog")
            no_schema = False
        elif parent_kind.upper() in {"TABLE", "VIEW", "STAGE", "TAG", "SEQUENCE", "FILE FORMAT"}:
            # "CREATE/DROP TABLE/VIEW/STAGE/TAG/FILE FORMAT"
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
            # "USE ROLE/WAREHOUSE" and friends name a role or warehouse rather than a table,
            # so there's nothing to qualify
            no_database = False
            no_schema = False

    elif node.parent.key == "show":
        # don't require a database or schema for SHOW
        # TODO: make this more nuanced
        no_database = False
        no_schema = False
    else:
        no_database = not node.args.get("catalog")
        no_schema = not node.args.get("db")

    return no_database, no_schema


def referenced_tables(expression: Expr) -> list[exp.Table]:
    """Tables a statement reads or writes, which must already exist under the name given.

    Query sources (including joins and CTE bodies) and the target of INSERT, UPDATE, DELETE and
    MERGE. Not the object a CREATE, DROP, ALTER or SHOW names, nor table functions, stages, CTE
    names or INFORMATION_SCHEMA.
    """

    tables = [
        source
        for scope in traverse_scope(expression)
        for source in scope.sources.values()
        if isinstance(source, exp.Table)
    ]

    if isinstance(expression, (exp.Insert, exp.Update, exp.Delete, exp.Merge)):
        this = expression.args.get("this")
        if isinstance(this, exp.Schema):
            this = this.this
        if isinstance(this, exp.Table):
            tables.append(this)
        if isinstance(using := expression.args.get("using"), exp.Table) and using.name.upper() not in _cte_names(
            expression
        ):
            tables.append(using)

    return [
        t
        for t in tables
        if isinstance(t.this, exp.Identifier)
        and not (isinstance(db := t.args.get("db"), exp.Identifier) and db.name.upper() == "INFORMATION_SCHEMA")
    ]


def miscased_column(expression: Expr, columns_of: Callable[[exp.Table], Collection[str] | None]) -> exp.Expr | None:
    """The first column a statement names in a case that matches none of its sources' columns.

    `columns_of` gives a table's column names, exactly as stored, or None when they aren't known.
    A column is only judged when every source it could come from is known: the tables, CTEs,
    subqueries and laterals in its scope, and the select's own aliases. So a name is refused only
    when it can't be anything but a column in the wrong case.
    """

    for scope in traverse_scope(expression):
        sources = {name: _source_columns(source, columns_of) for name, source in scope.sources.items()}
        if isinstance(scope.expression, exp.Select):
            # Snowflake lets WHERE, GROUP BY, HAVING and QUALIFY name a select's aliases
            sources[""] = {e.alias for e in scope.expression.expressions if isinstance(e, exp.Alias)}
        if found := _miscased(scope.columns, sources):
            return found

    if isinstance(expression, (exp.Update, exp.Delete, exp.Merge)):
        # their SET, WHERE and ON clauses aren't part of any scope
        sources = {
            source.alias_or_name: columns_of(source) if isinstance(source, exp.Table) else None
            for source in expression.find_all(exp.Table, exp.Subquery)
            if not source.find_ancestor(exp.Select)
        }
        columns = [c for c in expression.find_all(exp.Column) if not c.find_ancestor(exp.Select)]
        if found := _miscased(columns, sources):
            return found

    if (
        isinstance(expression, exp.Insert)
        and isinstance(target := expression.this, exp.Schema)
        and isinstance(target.this, exp.Table)
    ):
        identifiers = [i for i in target.expressions if isinstance(i, exp.Identifier)]
        if found := _miscased(identifiers, {"": columns_of(target.this)}):
            return found

    return None


def _source_columns(
    source: exp.Table | Scope, columns_of: Callable[[exp.Table], Collection[str] | None]
) -> set[str] | None:
    if isinstance(source, exp.Table):
        known = columns_of(source)
        return set(known) if known is not None else None
    names = set(source.expression.named_selects) if isinstance(source.expression, exp.Query) else set()
    # a star, or a source sqlglot can't name the columns of (eg: TABLE(FLATTEN(...)))
    return names if names and "*" not in names else None


def _miscased(
    columns: Collection[exp.Column | exp.Identifier], sources: Mapping[str, Collection[str] | None]
) -> exp.Column | exp.Identifier | None:
    for column in columns:
        if isinstance(column, exp.Column):
            if not isinstance(column.this, exp.Identifier):
                continue
            candidates = [sources.get(column.table)] if column.table else list(sources.values())
        else:
            candidates = list(sources.values())
        if not candidates or any(c is None for c in candidates):
            continue
        names = {name for c in candidates for name in c or ()}
        if column.name not in names and column.name.upper() in {name.upper() for name in names}:
            return column
    return None


def equal(left: exp.Identifier, right: exp.Identifier) -> bool:
    # as per https://docs.snowflake.com/en/sql-reference/identifiers-syntax#label-identifier-casing
    lid = left.this if left.quoted else left.this.upper()
    rid = right.this if right.quoted else right.this.upper()

    return lid == rid

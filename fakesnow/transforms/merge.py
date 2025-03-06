import sqlglot
from sqlglot import exp

from fakesnow import checks

# Implements snowflake's MERGE INTO functionality in duckdb (https://docs.snowflake.com/en/sql-reference/sql/merge).


def merge(merge_expr: exp.Expression) -> list[exp.Expression]:
    if not isinstance(merge_expr, exp.Merge):
        return [merge_expr]

    return [_create_merge_candidates(merge_expr), *_mutations(merge_expr), _counts(merge_expr)]


def _create_merge_candidates(merge_expr: exp.Merge) -> exp.Expression:
    """
    Given a merge statement, produce a temporary table that joins together the target and source tables.
    The merge_op column identifies which merge clause applies to the row.
    """
    target_tbl = merge_expr.this

    source = merge_expr.args.get("using")
    assert isinstance(source, exp.Expression)
    source_id = (alias := source.args.get("alias")) and alias.this if isinstance(source, exp.Subquery) else source.this
    assert isinstance(source_id, exp.Identifier)

    join_expr = merge_expr.args.get("on")
    assert isinstance(join_expr, exp.Binary)

    case_when_clauses: list[str] = []
    values: set[str] = set()

    # extract keys that reference the source table from the join expression
    # so they can be used by the mutation statements for joining
    # will include the source table identifier
    values.update(
        map(
            str,
            {
                c
                for c in join_expr.find_all(exp.Column)
                if (table := c.args.get("table"))
                and isinstance(table, exp.Identifier)
                and checks.equal(table, source_id)
            },
        )
    )

    # Iterate through the WHEN clauses to build up the CASE WHEN clauses
    for w_idx, w in enumerate(merge_expr.args["whens"]):
        assert isinstance(w, exp.When), f"Expected When expression, got {w}"

        predicate = join_expr.copy()
        matched = w.args.get("matched")
        then = w.args.get("then")
        condition = w.args.get("condition")

        if matched:
            # matchedClause see https://docs.snowflake.com/en/sql-reference/sql/merge#matchedclause-for-updates-or-deletes
            if condition:
                # Combine the top level ON expression with the AND condition
                # from this specific WHEN into a subquery, we use to target rows.
                # Eg. MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
                #       WHEN MATCHED AND t2.marked = 1 THEN DELETE
                predicate = exp.And(this=predicate, expression=condition)

            if isinstance(then, exp.Update):
                case_when_clauses.append(f"WHEN {predicate} THEN {w_idx}")
                values.update([str(c.expression) for c in then.expressions if isinstance(c.expression, exp.Column)])
            elif isinstance(then, exp.Var) and then.args.get("this") == "DELETE":
                case_when_clauses.append(f"WHEN {predicate} THEN {w_idx}")
            else:
                raise AssertionError(f"Expected 'Update' or 'Delete', got {then}")
        else:
            # notMatchedClause see https://docs.snowflake.com/en/sql-reference/sql/merge#notmatchedclause-for-inserts
            assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
            insert_values = then.expression.expressions
            values.update([str(c) for c in insert_values if isinstance(c, exp.Column)])
            predicate = f"AND {condition}" if condition else ""
            case_when_clauses.append(f"WHEN {target_tbl}.rowid is NULL {predicate} THEN {w_idx}")

    sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
    SELECT
        {", ".join(sorted(values))},
        CASE
            {" ".join(case_when_clauses)}
            ELSE NULL
        END AS MERGE_OP
    FROM {target_tbl}
    FULL OUTER JOIN {source} ON {join_expr.sql()}
    WHERE MERGE_OP IS NOT NULL
    """

    return sqlglot.parse_one(sql)


def _mutations(merge_expr: exp.Merge) -> list[exp.Expression]:
    """
    Given a merge statement, produce a list of delete, update and insert statements that use the
    merge_candidates and source table to update the target target.
    """
    target_tbl = merge_expr.this
    source = merge_expr.args.get("using")
    source_tbl = source.alias if isinstance(source, exp.Subquery) else source
    join_expr = merge_expr.args.get("on")

    statements: list[exp.Expression] = []

    # Iterate through the WHEN clauses to generate delete/update/insert statements
    for w_idx, w in enumerate(merge_expr.args["whens"]):
        assert isinstance(w, exp.When), f"Expected When expression, got {w}"

        matched = w.args.get("matched")
        then = w.args.get("then")

        if matched:
            if isinstance(then, exp.Var) and then.args.get("this") == "DELETE":
                delete_sql = f"""
                    DELETE FROM {target_tbl}
                    USING merge_candidates AS {source_tbl}
                    WHERE {join_expr}
                    AND {source_tbl}.merge_op = {w_idx}
                """
                statements.append(sqlglot.parse_one(delete_sql))
            elif isinstance(then, exp.Update):
                # when the update statement has a table alias, duckdb doesn't support the alias in the set
                # column name, so we use e.this.this to get just the column name without its table prefix
                set_clauses = ", ".join(
                    [f"{e.this.this} = {e.expression.sql()}" for e in then.args.get("expressions", [])]
                )
                update_sql = f"""
                    UPDATE {target_tbl}
                    SET {set_clauses}
                    FROM merge_candidates AS {source_tbl}
                    WHERE {join_expr}
                    AND {source_tbl}.merge_op = {w_idx}
                """
                statements.append(sqlglot.parse_one(update_sql))
            else:
                raise AssertionError(f"Expected 'Update' or 'Delete', got {then}")
        else:
            assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
            cols = [str(c) for c in then.this.expressions] if then.this else []
            columns = f"({', '.join(cols)})" if cols else ""
            values = ", ".join(map(str, then.expression.expressions))
            insert_sql = f"""
                INSERT INTO {target_tbl} {columns}
                SELECT {values}
                FROM merge_candidates AS {source_tbl}
                WHERE {source_tbl}.merge_op = {w_idx}
            """
            statements.append(sqlglot.parse_one(insert_sql))

    return statements


def _counts(merge_expr: exp.Merge) -> exp.Expression:
    """
    Given a merge statement, derive the a SQL statement which produces the following columns using the merge_candidates
    table:

    - "number of rows inserted"
    - "number of rows updated"
    - "number of rows deleted"

    Only columns relevant to the merge operation are included, eg: if no rows are deleted, the "number of rows deleted"
    column is not included.
    """

    # Initialize dictionaries to store operation types and their corresponding indices
    operations = {"inserted": [], "updated": [], "deleted": []}

    # Iterate through the WHEN clauses to categorize operations
    for w_idx, w in enumerate(merge_expr.args["whens"]):
        assert isinstance(w, exp.When), f"Expected When expression, got {w}"

        matched = w.args.get("matched")
        then = w.args.get("then")

        if matched:
            if isinstance(then, exp.Update):
                operations["updated"].append(w_idx)
            elif isinstance(then, exp.Var) and then.args.get("this") == "DELETE":
                operations["deleted"].append(w_idx)
            else:
                raise AssertionError(f"Expected 'Update' or 'Delete', got {then}")
        else:
            assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
            operations["inserted"].append(w_idx)

    count_statements = [
        f"""COUNT_IF(merge_op in ({",".join(map(str, indices))})) as \"number of rows {op}\""""
        for op, indices in operations.items()
        if indices
    ]
    sql = f"""
    SELECT {", ".join(count_statements)}
    FROM merge_candidates
    """

    return sqlglot.parse_one(sql)

import sqlglot
from sqlglot import exp

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
    source_tbl = merge_expr.args.get("using")
    join_expr = merge_expr.args.get("on")
    assert isinstance(join_expr, exp.Binary)

    case_when_clauses: list[str] = []
    values: set[str] = set()

    # Iterate through the WHEN clauses to build up the CASE WHEN clauses
    for w_idx, w in enumerate(merge_expr.expressions):
        assert isinstance(w, exp.When), f"Expected When expression, got {w}"

        predicate = join_expr.copy()

        # Combine the top level ON expression with the AND condition
        # from this specific WHEN into a subquery, we use to target rows.
        # Eg. MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
        #       WHEN MATCHED AND t2.marked = 1 THEN DELETE
        if condition := w.args.get("condition"):
            predicate = exp.And(this=predicate, expression=condition)

        matched = w.args.get("matched")
        then = w.args.get("then")

        if matched:
            if isinstance(then, exp.Update):
                case_when_clauses.append(f"WHEN {predicate.sql()} THEN {w_idx}")
                values.update([str(c.expression) for c in then.expressions if isinstance(c.expression, exp.Column)])
            elif isinstance(then, exp.Var) and then.args.get("this") == "DELETE":
                case_when_clauses.append(f"WHEN {predicate.sql()} THEN {w_idx}")
            else:
                raise AssertionError(f"Expected 'Update' or 'Delete', got {then}")
        else:
            assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
            insert_values = then.expression.expressions
            values.update([str(c) for c in insert_values if isinstance(c, exp.Column)])
            case_when_clauses.append(f"WHEN {target_tbl}.rowid is NULL THEN {w_idx}")

    sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
    SELECT
        {', '.join(sorted(values))},
        CASE
            {' '.join(case_when_clauses)}
            ELSE NULL
        END AS MERGE_OP
    FROM {target_tbl}
    FULL OUTER JOIN {source_tbl} ON {join_expr.sql()}
    WHERE MERGE_OP IS NOT NULL
    """

    return sqlglot.parse_one(sql)


def _mutations(merge_expr: exp.Merge) -> list[exp.Expression]:
    """
    Given a merge statement, produce a list of delete, update and insert statements that use the
    merge_candidates and source table to update the target target.
    """
    target_tbl = merge_expr.this
    source_tbl = merge_expr.args.get("using")
    join_expr = merge_expr.args.get("on")

    statements: list[exp.Expression] = []

    # Iterate through the WHEN clauses to generate delete/update/insert statements
    for w_idx, w in enumerate(merge_expr.expressions):
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
                set_clauses = ", ".join(
                    [f"{e.alias or e.this} = {e.expression.sql()}" for e in then.args.get("expressions", [])]
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

    Only columns relevant to the merge operation are included, eg: if no rows are deleted, the "number of rows deleted" column
    is not included.
    """

    # Initialize dictionaries to store operation types and their corresponding indices
    operations = {"inserted": [], "updated": [], "deleted": []}

    # Iterate through the WHEN clauses to categorize operations
    for w_idx, w in enumerate(merge_expr.expressions):
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
        f"""COUNT_IF(merge_op in ({','.join(map(str, indices))})) as \"number of rows {op}\""""
        for op, indices in operations.items()
        if indices
    ]
    sql = f"""
    SELECT {', '.join(count_statements)}
    FROM merge_candidates
    """

    return sqlglot.parse_one(sql)

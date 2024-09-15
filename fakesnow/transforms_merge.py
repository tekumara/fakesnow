import sqlglot
from sqlglot import exp

# Implements snowflake's MERGE INTO functionality in duckdb (https://docs.snowflake.com/en/sql-reference/sql/merge).

TEMP_MERGE_UPDATED_DELETES = "temp_merge_updates_deletes"
TEMP_MERGE_INSERTS = "temp_merge_inserts"


def _target_table(merge_expr: exp.Expression) -> exp.Expression:
    target_table = merge_expr.this
    if target_table is None:
        raise ValueError("Target table expression is None")
    return target_table


def _source_table(merge_expr: exp.Expression) -> exp.Expression:
    source_table = merge_expr.args.get("using")
    if source_table is None:
        raise ValueError("Source table expression is None")
    return source_table


def _merge_on_expr(merge_expr: exp.Expression) -> exp.Expression:
    # Get the ON expression from the merge operation (Eg. MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key)
    # which is applied to filter the source and target tables.
    on_expr = merge_expr.args.get("on")
    if on_expr is None:
        raise ValueError("Merge ON expression is None")
    return on_expr


def _create_temp_tables() -> list[exp.Expression]:
    # Creates temp tables to store the source rows and the modifications to apply to those rows.
    return [
        # Create temp table for update and delete operations
        sqlglot.parse_one(
            f"CREATE OR REPLACE TEMP TABLE {TEMP_MERGE_UPDATED_DELETES} "
            + "(target_rowid INTEGER, when_id INTEGER, type CHAR(1))"
        ),
        # Create temp table for insert operations
        sqlglot.parse_one(f"CREATE OR REPLACE TEMP TABLE {TEMP_MERGE_INSERTS} (source_rowid INTEGER, when_id INTEGER)"),
    ]


def _generate_final_expression_set(
    temp_table_inserts: list[exp.Expression], output_expressions: list[exp.Expression]
) -> list[exp.Expression]:
    # Collects all of the operations together to be executed in a single transaction.
    # Operate in a transaction to freeze rowids https://duckdb.org/docs/sql/statements/select#row-ids
    begin_transaction_exp = sqlglot.parse_one("BEGIN TRANSACTION")
    end_transaction_exp = sqlglot.parse_one("COMMIT")

    # Add modifications results outcome query
    results_exp = sqlglot.parse_one(f"""
    WITH merge_update_deletes AS (
        select count_if(type == 'U')::int AS "updates", count_if(type == 'D')::int as "deletes"
        from {TEMP_MERGE_UPDATED_DELETES}
    ), merge_inserts AS (select count() AS "inserts" from {TEMP_MERGE_INSERTS})
    SELECT mi.inserts as "number of rows inserted",
        mud.updates as "number of rows updated",
        mud.deletes as "number of rows deleted"
    from merge_update_deletes mud, merge_inserts mi
    """)

    return [
        begin_transaction_exp,
        *temp_table_inserts,
        *output_expressions,
        end_transaction_exp,
        results_exp,
    ]


def _insert_temp_merge_operation(
    op_type: str, when_idx: int, subquery: exp.Expression, target_table: exp.Expression
) -> exp.Expression:
    assert op_type in {
        "U",
        "D",
    }, f"Expected 'U' or 'D', got merge op_type: {op_type}"  # Updates/Deletes
    return exp.insert(
        into=TEMP_MERGE_UPDATED_DELETES,
        expression=exp.select("rowid", exp.Literal.number(when_idx), exp.Literal.string(op_type))
        .from_(target_table)
        .where(subquery),
    )


def _remove_table_alias(eq_exp: exp.Condition) -> exp.Condition:
    eq_exp.set("table", None)
    return eq_exp


def merge(merge_expr: exp.Expression) -> list[exp.Expression]:
    if not isinstance(merge_expr, exp.Merge):
        return [merge_expr]

    return [_create_merge_candidates(merge_expr), *_mutations(merge_expr)]


def _create_merge_candidates(merge_expr: exp.Merge) -> exp.Expression:
    """
    Given a merge statement, produce a temporary table that joins together the target and source tables.
    The merge_op column identifies which merge clause applies to the row.
    See https://docs.snowflake.com/en/sql-reference/sql/merge.html
    """
    target_tbl = merge_expr.this
    source_tbl = merge_expr.args.get("using")
    join_expr = merge_expr.args.get("on")
    assert isinstance(join_expr, exp.Binary)

    whens = merge_expr.expressions
    case_when_clauses: list[str] = []
    values: set[str] = set()

    for w_idx, w in enumerate(whens):
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
    merge candidate and source table to update the target target.
    """
    target_tbl = merge_expr.this
    source_tbl = merge_expr.args.get("using")
    join_expr = merge_expr.args.get("on")

    statements: list[exp.Expression] = []

    whens = merge_expr.expressions
    for w_idx, w in enumerate(whens):
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
            values = ", ".join([str(c) for (c) in then.expression.expressions])
            insert_sql = f"""
                INSERT INTO {target_tbl} {columns}
                SELECT {values}
                FROM merge_candidates AS {source_tbl}
                WHERE {source_tbl}.merge_op = {w_idx}
            """
            statements.append(sqlglot.parse_one(insert_sql))

    return statements

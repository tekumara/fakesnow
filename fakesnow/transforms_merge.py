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

    return [_create_merge_candidates(merge_expr)]


def _create_merge_candidates(merge_expr: exp.Merge) -> exp.Expression:
    """
    Given a merge statement, produce a temporary table that joins together the target and source tables.
    The merge_op column identifies which merge clause, if any, applies to the row.
    See https://docs.snowflake.com/en/sql-reference/sql/merge.html
    """
    target_tbl = merge_expr.this
    source_tbl = merge_expr.args.get("using")
    join_expr = merge_expr.args.get("on")
    assert isinstance(join_expr, exp.EQ), "Expected EQ expression, got {join_expr}"

    whens = merge_expr.expressions
    case_when_clauses = []
    columns = []

    for w_idx, w in enumerate(whens):
        assert isinstance(w, exp.When), f"Expected When expression, got {w}"

        # Combine the top level ON expression with the AND condition
        # from this specific WHEN into a subquery, we use to target rows.
        # Eg. # MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
        #           WHEN MATCHED AND t2.marked = 1 THEN DELETE
        condition = w.args.get("condition")

        predicate = join_expr.copy()
        if condition:
            predicate = exp.And(this=predicate, expression=condition)

        matched = w.args.get("matched")
        then = w.args.get("then")
        assert then

        if matched:
            if isinstance(then, exp.Update) or (isinstance(then, exp.Var) and then.args.get("this") == "DELETE"):
                case_when_clauses.append(f"WHEN {predicate.sql()} THEN {w_idx}")
            else:
                raise ValueError(f"Expected 'Update' or 'Delete', got {then}")
        else:
            assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
            case_when_clauses.append(f"WHEN {join_expr.sql()} THEN {w_idx}")

        # Collect all columns from both tables
        if isinstance(then, exp.Insert):
            icolumns = then.args.get("this")
            assert isinstance(icolumns, exp.Tuple), f"Expected Tuple, got {then.args.get('this')}"
            ivalues = then.args.get("expression")
            assert isinstance(ivalues, exp.Tuple), f"Expected Tuple, got {then.args.get('expression')}"

            columns.extend([(target_tbl, col) for col in icolumns.expressions])
            columns.extend([(source_tbl, col) for col in ivalues.args.get("expressions")])

        elif isinstance(then, exp.Update):
            exprs = then.args.get("expressions")
            assert exprs
            for expr in exprs:
                assert isinstance(expr, exp.EQ), f"Expected EQ expression, got {expr}"
                columns.extend(
                    (
                        (target_tbl, expr.this),
                        (source_tbl, expr.expression),
                    )
                )

    # Remove duplicates and sort
    columns = sorted(set(columns))

    # Construct the final SQL
    sql = f"""
    CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
    SELECT
        {', '.join(f"{table}.{col} AS {table}_{col}" for table,col in columns)},
        CASE
            {' '.join(case_when_clauses)}
            ELSE NULL
        END AS MERGE_OP
    FROM {target_tbl}
    FULL OUTER JOIN {source_tbl} ON {join_expr.sql()}
    WHERE MERGE_OP IS NOT NULL;
    """

    return sqlglot.parse_one(sql)

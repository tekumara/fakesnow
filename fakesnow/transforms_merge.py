import sqlglot
from sqlglot import exp


class MergeTransform:
    def __init__(self, expression: exp.Expression) -> None:
        self._orig_expr = expression
        self._variables = {}

    def transform(self) -> list[exp.Expression]:
        """Create multiple compatible duckdb statements to be functionally equivalent to Snowflake's MERGE INTO.
        Snowflake's MERGE INTO: See https://docs.snowflake.com/en/sql-reference/sql/merge.html
        """

        # Insert into a temp table the source rows (rowid is stable in a transaction: https://duckdb.org/docs/sql/statements/select.html#row-ids)
        # and which modification to apply.
        # Error if attempting to update the same source row multiple times (based on a config in the doco).
        # Perform each when based on the temp table rowid and modification index.
        if isinstance(self._orig_expr, exp.Merge):
            temp_table_inserts = []
            output_expressions = []

            target_table = self._orig_expr.this
            source_table = self._orig_expr.args.get("using")
            on_expression = self._orig_expr.args.get("on")

            # Create temp table for update and delete operations
            temp_table_inserts.append(
                sqlglot.parse_one(
                    "CREATE OR REPLACE TEMP TABLE temp_merge_updates_deletes "
                    + "(target_rowid INTEGER, when_id INTEGER, type CHAR(1))"
                )
            )
            # Create temp table for insert operations
            temp_table_inserts.append(
                sqlglot.parse_one(
                    "CREATE OR REPLACE TEMP TABLE temp_merge_inserts " + "(source_rowid INTEGER, when_id INTEGER)"
                )
            )

            whens = self._orig_expr.expressions
            for w_idx, w in enumerate(whens):
                assert isinstance(w, exp.When), f"Expected When expression, got {w}"

                and_condition = w.args.get("condition")
                subquery_on_expression = on_expression.copy()
                if and_condition:
                    subquery_on_expression = exp.And(this=subquery_on_expression, expression=and_condition)

                matched = w.args.get("matched")
                then = w.args.get("then")
                if matched:
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=target_table),
                        expressions=[
                            exp.select("target_rowid")
                            .from_("temp_merge_updates_deletes")
                            .where(exp.EQ(this="when_id", expression=exp.Literal(this=f"{w_idx}", is_string=False)))
                            .where(exp.EQ(this="target_rowid", expression=exp.Column(this="rowid", table=target_table)))
                        ],
                    )
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(1)
                            .from_("temp_merge_updates_deletes")
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=target_table),
                                    expression=exp.Column(this="target_rowid"),
                                )
                            )
                        )
                    )
                    subquery_ignoring_temp_table = exp.Exists(
                        this=exp.select(1).from_(source_table).where(subquery_on_expression)
                    )
                    subquery = exp.And(this=subquery_ignoring_temp_table, expression=not_in_temp_table_subquery)

                    def insert_temp_merge_operation(
                        op_type: str, w_idx: int = w_idx, subquery: exp.Expression = subquery
                    ) -> exp.Expression:
                        assert op_type in [
                            "U",
                            "D",
                        ], f"Expected 'U' or 'D', got merge op_type: {op_type}"  # Updates/Deletes
                        return exp.insert(
                            into="temp_merge_updates_deletes",
                            expression=exp.select("rowid", w_idx, exp.Literal(this=op_type, is_string=True))
                            .from_(target_table)
                            .where(subquery),
                        )

                    if isinstance(then, exp.Update):
                        temp_table_inserts.append(insert_temp_merge_operation("U"))

                        def remove_source_alias(eq_exp: exp.EQ) -> exp.EQ:
                            eq_exp.args.get("this").set("table", None)
                            return eq_exp

                        then.set("this", target_table)
                        then.set(
                            "expressions",
                            exp.Set(expressions=[remove_source_alias(e) for e in then.args.get("expressions")]),
                        )
                        then.set("from", exp.From(this=source_table))
                        then.set(
                            "where",
                            exp.Where(this=exp.And(this=subquery_on_expression, expression=rowid_in_temp_table_expr)),
                        )
                        output_expressions.append(then)
                    # Var(this=DELETE) when processing WHEN MATCHED THEN DELETE.
                    elif then.args.get("this") == "DELETE":
                        temp_table_inserts.append(insert_temp_merge_operation("D"))
                        delete_from_temp = exp.delete(table=target_table, where=rowid_in_temp_table_expr)
                        output_expressions.append(delete_from_temp)
                    else:
                        assert isinstance(then, (exp.Update, exp.Delete)), f"Expected 'Update' or 'Delete', got {then}"
                else:
                    assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=source_table),
                        expressions=[
                            exp.select("source_rowid")
                            .from_("temp_merge_inserts")
                            .where(exp.EQ(this="when_id", expression=exp.Literal(this=f"{w_idx}", is_string=False)))
                            .where(exp.EQ(this="source_rowid", expression=exp.Column(this="rowid", table=source_table)))
                        ],
                    )
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(1)
                            .from_("temp_merge_inserts")
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=source_table),
                                    expression=exp.Column(this="source_rowid"),
                                )
                            )
                        )
                    )
                    subquery_ignoring_temp_table = exp.Exists(this=exp.select(1).from_(target_table)
                                                              .where(on_expression))
                    subquery = exp.And(this=subquery_ignoring_temp_table, expression=not_in_temp_table_subquery)

                    not_exists_subquery = exp.Not(this=subquery)
                    if and_condition:
                        temp_match_where = exp.And(this=and_condition, expression=not_exists_subquery)
                    else:
                        temp_match_where = not_exists_subquery
                    temp_match_expr = exp.insert(
                        into="temp_merge_inserts",
                        expression=exp.select("rowid", w_idx).from_(source_table).where(temp_match_where),
                    )
                    temp_table_inserts.append(temp_match_expr)

                    def remove_table_alias(eq_exp: exp.Column) -> exp.Column:
                        eq_exp.set("table", None)
                        return eq_exp

                    columns = [remove_table_alias(e) for e in then.args.get("this").expressions]
                    statement = exp.insert(
                        into=target_table,
                        columns=[c.this for c in columns],
                        expression=exp.select(*(then.args.get("expression").args.get("expressions")))
                        .from_(source_table)
                        .where(rowid_in_temp_table_expr),
                    )
                    output_expressions.append(statement)

            # Operate in a transaction to freeze rowids https://duckdb.org/docs/sql/statements/select#row-ids
            begin_transaction_exp = sqlglot.parse_one("BEGIN TRANSACTION")
            end_transaction_exp = sqlglot.parse_one("COMMIT")
            # Add modifications results outcome query
            results_exp = sqlglot.parse_one("""
    WITH merge_update_deletes AS (
        select count_if(type == 'U')::int AS "updates", count_if(type == 'D')::int as "deletes"
        from temp_merge_updates_deletes
    ), merge_inserts AS (select count() AS "inserts" from temp_merge_inserts)
    SELECT mi.inserts as "number of rows inserted",
        mud.updates as "number of rows updated",
        mud.deletes as "number of rows deleted"
    from merge_update_deletes mud, merge_inserts mi
    """)
            expressions = [
                begin_transaction_exp,
                *temp_table_inserts,
                *output_expressions,
                end_transaction_exp,
                results_exp,
            ]
            print(*expressions, sep="\n")
            return expressions
        else:
            return [self._orig_expr]

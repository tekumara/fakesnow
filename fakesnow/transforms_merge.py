import sqlglot
from sqlglot import exp


# Implements snowflake's MERGE INTO functionality in duckdb (https://docs.snowflake.com/en/sql-reference/sql/merge).
class MergeTransform:
    TEMP_MERGE_UPDATED_DELETES = "temp_merge_updates_deletes"
    TEMP_MERGE_INSERTS = "temp_merge_inserts"

    def __init__(self, expression: exp.Expression) -> None:
        self._orig_expr = expression
        self._variables = {}
        self._temp_table_inserts = []
        self._output_expressions = []

    def _target_table(self) -> exp.Expression:
        target_table = self._orig_expr.this
        if target_table is None:
            raise ValueError("Target table expression is None")
        return target_table

    def _source_table(self) -> exp.Expression:
        source_table = self._orig_expr.args.get("using")
        if source_table is None:
            raise ValueError("Source table expression is None")
        return source_table

    # Get the ON expression from the merge operation (Eg. MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key)
    # which is applied to filter the source and target tables.
    def _merge_on_expr(self) -> exp.Expression:
        on_expr = self._orig_expr.args.get("on")
        if on_expr is None:
            raise ValueError("Merge ON expression is None")
        return on_expr

    # Creates temp tables to store the source rows and the modifications to apply to those rows.
    def _create_temp_tables(self) -> None:
        # Create temp table for update and delete operations
        self._temp_table_inserts.append(
            sqlglot.parse_one(
                f"CREATE OR REPLACE TEMP TABLE {self.TEMP_MERGE_UPDATED_DELETES} "
                + "(target_rowid INTEGER, when_id INTEGER, type CHAR(1))"
            )
        )
        # Create temp table for insert operations
        self._temp_table_inserts.append(
            sqlglot.parse_one(
                f"CREATE OR REPLACE TEMP TABLE {self.TEMP_MERGE_INSERTS} " + "(source_rowid INTEGER, when_id INTEGER)"
            )
        )

    # Collects all of the operations together to be executed in a single transaction.
    def _generate_final_expression_set(self) -> list[exp.Expression]:
        # Operate in a transaction to freeze rowids https://duckdb.org/docs/sql/statements/select#row-ids
        begin_transaction_exp = sqlglot.parse_one("BEGIN TRANSACTION")
        end_transaction_exp = sqlglot.parse_one("COMMIT")

        # Add modifications results outcome query
        results_exp = sqlglot.parse_one(f"""
WITH merge_update_deletes AS (
    select count_if(type == 'U')::int AS "updates", count_if(type == 'D')::int as "deletes"
    from {self.TEMP_MERGE_UPDATED_DELETES}
), merge_inserts AS (select count() AS "inserts" from {self.TEMP_MERGE_INSERTS})
SELECT mi.inserts as "number of rows inserted",
    mud.updates as "number of rows updated",
    mud.deletes as "number of rows deleted"
from merge_update_deletes mud, merge_inserts mi
""")
        expressions = [
            begin_transaction_exp,
            *self._temp_table_inserts,
            *self._output_expressions,
            end_transaction_exp,
            results_exp,
        ]
        # Useful for debugging
        # print(*expressions, sep="\n")
        return expressions

    def _insert_temp_merge_operation(self, op_type: str, when_idx: int, subquery: exp.Expression) -> None:
        assert op_type in [
            "U",
            "D",
        ], f"Expected 'U' or 'D', got merge op_type: {op_type}"  # Updates/Deletes
        self._temp_table_inserts.append(
            exp.insert(
                into=self.TEMP_MERGE_UPDATED_DELETES,
                expression=exp.select("rowid", exp.Literal.number(when_idx), exp.Literal.string(op_type))
                .from_(self._target_table())
                .where(subquery),
            )
        )

    def transform(self) -> list[exp.Expression]:
        """Create multiple compatible duckdb statements to be functionally equivalent to Snowflake's MERGE INTO.
        Snowflake's MERGE INTO: See https://docs.snowflake.com/en/sql-reference/sql/merge.html
        """
        # Breaking down how we implement MERGE:
        # Insert into a temp table the source rows (rowid is stable in a transaction: https://duckdb.org/docs/sql/statements/select.html#row-ids)
        # and which modification to apply to those source rows (insert, update, delete).
        #
        # Then apply the modifications to the target table based on the rowids in the temp tables.

        # TODO: Error if attempting to update the same source row multiple times (based on a config in the doco).
        if isinstance(self._orig_expr, exp.Merge):
            self._create_temp_tables()

            whens = self._orig_expr.expressions
            # Loop through each WHEN clause
            for w_idx, w in enumerate(whens):
                assert isinstance(w, exp.When), f"Expected When expression, got {w}"

                # Combine the top level ON expression with the AND condition
                # from this specific WHEN into a subquery, we use to target rows.
                # Eg. # MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
                #           WHEN MATCHED AND t2.marked = 1 THEN DELETE
                and_condition = w.args.get("condition")
                subquery_on_expression = self._merge_on_expr().copy()
                if and_condition:
                    subquery_on_expression = exp.And(this=subquery_on_expression, expression=and_condition)

                matched = w.args.get("matched")
                then = w.args.get("then")
                assert then is not None, "then is None"
                # Handling WHEN MATCHED AND <Condition> THEN DELETE / UPDATE SET <Updates>
                if matched:
                    # Ensuring rows already exist in temporary table for
                    # previous WHEN clauses are not added again
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(exp.Literal.number(1))
                            .from_(self.TEMP_MERGE_UPDATED_DELETES)
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=self._target_table()),
                                    expression=exp.Column(this="target_rowid"),
                                )
                            )
                        )
                    )

                    # Query finding rows that match the original ON condition and this WHEN condition
                    subquery_ignoring_temp_table = exp.Exists(
                        this=exp.select(exp.Literal.number(1)).from_(self._source_table()).where(subquery_on_expression)
                    )
                    #  Include both of the above subqueries in the final subquery
                    subquery = exp.And(this=subquery_ignoring_temp_table, expression=not_in_temp_table_subquery)

                    # Select the rowids of the rows to apply the operation to
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=self._target_table()),
                        expressions=[
                            exp.select("target_rowid")
                            .from_(self.TEMP_MERGE_UPDATED_DELETES)
                            .where(exp.EQ(this="when_id", expression=exp.Literal.number(w_idx)))
                            .where(
                                exp.EQ(
                                    this="target_rowid", expression=exp.Column(this="rowid", table=self._target_table())
                                )
                            )
                        ],
                    )
                    if isinstance(then, exp.Update):
                        # Insert into the temp table the rowids of the rows that match the WHEN condition
                        self._insert_temp_merge_operation("U", w_idx, subquery)

                        # Build the UPDATE statement to apply to the target table for this specific WHEN clause sourcing
                        # its target rows from the temp table that we just inserted into.
                        then.set("this", self._target_table())
                        then_exprs = then.args.get("expressions")
                        assert then_exprs is not None, "then_exprs is None"
                        then.set(
                            "expressions",
                            exp.Set(expressions=[self._remove_table_alias(e) for e in then_exprs]),
                        )
                        then.set("from", exp.From(this=self._source_table()))
                        then.set(
                            "where",
                            exp.Where(this=exp.And(this=subquery_on_expression, expression=rowid_in_temp_table_expr)),
                        )
                        self._output_expressions.append(then)

                    # Handling WHEN MATCHED AND <Condition> THEN DELETE
                    elif then.args.get("this") == "DELETE":
                        # Insert into the temp table the rowids of the rows that match the WHEN condition
                        self._insert_temp_merge_operation("D", w_idx, subquery)

                        # Build the DELETE statement to apply to the target table for this specific WHEN clause sourcing
                        # its target rows from the temp table that we just inserted into.
                        delete_from_temp = exp.delete(table=self._target_table(), where=rowid_in_temp_table_expr)
                        self._output_expressions.append(delete_from_temp)
                    else:
                        assert isinstance(then, (exp.Update, exp.Delete)), f"Expected 'Update' or 'Delete', got {then}"

                # Handling WHEN NOT MATCHED THEN INSERT (<Columns>) VALUES (<Values>)
                else:
                    assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
                    # Query ensuring rows already exist in the temporary table for
                    # previous WHEN clauses are not added again
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(exp.Literal.number(1))
                            .from_(self.TEMP_MERGE_INSERTS)
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=self._source_table()),
                                    expression=exp.Column(this="source_rowid"),
                                )
                            )
                        )
                    )
                    # Query finding rows that match the original ON condition and this WHEN condition
                    subquery_ignoring_temp_table = exp.Exists(
                        this=exp.select(exp.Literal.number(1)).from_(self._target_table()).where(self._merge_on_expr())
                    )
                    subquery = exp.And(this=subquery_ignoring_temp_table, expression=not_in_temp_table_subquery)

                    not_exists_subquery = exp.Not(this=subquery)
                    if and_condition:
                        temp_match_where = exp.And(this=and_condition, expression=not_exists_subquery)
                    else:
                        temp_match_where = not_exists_subquery
                    temp_match_expr = exp.insert(
                        into=self.TEMP_MERGE_INSERTS,
                        expression=exp.select("rowid", exp.Literal.number(w_idx))
                        .from_(self._source_table())
                        .where(temp_match_where),
                    )
                    # Insert into the temp table the rowids of the rows that match the WHEN condition
                    self._temp_table_inserts.append(temp_match_expr)

                    # Select the rowids of the rows to apply the operation to
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=self._source_table()),
                        expressions=[
                            exp.select("source_rowid")
                            .from_(self.TEMP_MERGE_INSERTS)
                            .where(exp.EQ(this="when_id", expression=exp.Literal.number(w_idx)))
                            .where(
                                exp.EQ(
                                    this="source_rowid", expression=exp.Column(this="rowid", table=self._source_table())
                                )
                            )
                        ],
                    )

                    this_expr = then.args.get("this")
                    assert this_expr is not None, "this_expr is None"
                    then_exprs = then.args.get("expression")
                    assert then_exprs is not None, "then_exprs is None"
                    columns = [self._remove_table_alias(e) for e in this_expr.expressions]
                    # The INSERT statement to apply to the target table for targeted rowids
                    statement = exp.insert(
                        into=self._target_table(),
                        columns=[c.this for c in columns],
                        expression=exp.select(*(then_exprs.args.get("expressions")))
                        .from_(self._source_table())
                        .where(rowid_in_temp_table_expr),
                    )
                    self._output_expressions.append(statement)

            return self._generate_final_expression_set()
        else:
            return [self._orig_expr]

    # helpers
    def _remove_table_alias(self, eq_exp: exp.Condition) -> exp.Condition:
        eq_exp.set("table", None)
        return eq_exp

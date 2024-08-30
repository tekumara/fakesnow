import sqlglot
from sqlglot import exp


class MergeTransform:
    TEMP_MERGE_UPDATED_DELETES = "temp_merge_updates_deletes"
    TEMP_MERGE_INSERTS = "temp_merge_inserts"

    def __init__(self, expression: exp.Expression) -> None:
        self._orig_expr = expression
        self._variables = {}
        self._temp_table_inserts = []
        self._output_expressions = []

    def _target_table(self) -> exp.Expression:
        return self._orig_expr.this

    def _source_table(self) -> exp.Expression:
        return self._orig_expr.args.get("using")

    def _merge_on_expr(self) -> exp.Expression:
        return self._orig_expr.args.get("on")

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
        print(*expressions, sep="\n")
        return expressions


    def transform(self) -> list[exp.Expression]:
        """Create multiple compatible duckdb statements to be functionally equivalent to Snowflake's MERGE INTO.
        Snowflake's MERGE INTO: See https://docs.snowflake.com/en/sql-reference/sql/merge.html
        """

        # Insert into a temp table the source rows (rowid is stable in a transaction: https://duckdb.org/docs/sql/statements/select.html#row-ids)
        # and which modification to apply.
        # Error if attempting to update the same source row multiple times (based on a config in the doco).
        # Perform each when based on the temp table rowid and modification index.
        if isinstance(self._orig_expr, exp.Merge):
            self._create_temp_tables()

            whens = self._orig_expr.expressions
            for w_idx, w in enumerate(whens):
                assert isinstance(w, exp.When), f"Expected When expression, got {w}"

                and_condition = w.args.get("condition")
                subquery_on_expression = self._merge_on_expr().copy()
                if and_condition:
                    subquery_on_expression = exp.And(this=subquery_on_expression, expression=and_condition)

                matched = w.args.get("matched")
                then = w.args.get("then")
                if matched:
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=self._target_table()),
                        expressions=[
                            exp.select("target_rowid")
                            .from_(self.TEMP_MERGE_UPDATED_DELETES)
                            .where(exp.EQ(this="when_id", expression=exp.Literal(this=f"{w_idx}", is_string=False)))
                            .where(exp.EQ(this="target_rowid",
                                          expression=exp.Column(this="rowid", table=self._target_table())))
                        ],
                    )
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(1)
                            .from_(self.TEMP_MERGE_UPDATED_DELETES)
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=self._target_table()),
                                    expression=exp.Column(this="target_rowid"),
                                )
                            )
                        )
                    )
                    subquery_ignoring_temp_table = exp.Exists(
                        this=exp.select(1).from_(self._source_table()).where(subquery_on_expression)
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
                            into=self.TEMP_MERGE_UPDATED_DELETES,
                            expression=exp.select("rowid", w_idx, exp.Literal(this=op_type, is_string=True))
                            .from_(self._target_table())
                            .where(subquery),
                        )

                    if isinstance(then, exp.Update):
                        self._temp_table_inserts.append(insert_temp_merge_operation("U"))

                        then.set("this", self._target_table())
                        then.set(
                            "expressions",
                            exp.Set(expressions=[self._remove_table_alias(e) for e in then.args.get("expressions")]),
                        )
                        then.set("from", exp.From(this=self._source_table()))
                        then.set(
                            "where",
                            exp.Where(this=exp.And(this=subquery_on_expression, expression=rowid_in_temp_table_expr)),
                        )
                        self._output_expressions.append(then)
                    # Var(this=DELETE) when processing WHEN MATCHED THEN DELETE.
                    elif then.args.get("this") == "DELETE":
                        self._temp_table_inserts.append(insert_temp_merge_operation("D"))
                        delete_from_temp = exp.delete(table=self._target_table(), where=rowid_in_temp_table_expr)
                        self._output_expressions.append(delete_from_temp)
                    else:
                        assert isinstance(then, (exp.Update, exp.Delete)), f"Expected 'Update' or 'Delete', got {then}"
                else:
                    assert isinstance(then, exp.Insert), f"Expected 'Insert', got {then}"
                    rowid_in_temp_table_expr = exp.In(
                        this=exp.Column(this="rowid", table=self._source_table()),
                        expressions=[
                            exp.select("source_rowid")
                            .from_(self.TEMP_MERGE_INSERTS)
                            .where(exp.EQ(this="when_id", expression=exp.Literal(this=f"{w_idx}", is_string=False)))
                            .where(exp.EQ(this="source_rowid",
                                          expression=exp.Column(this="rowid", table=self._source_table())))
                        ],
                    )
                    not_in_temp_table_subquery = exp.Not(
                        this=exp.Exists(
                            this=exp.select(1)
                            .from_(self.TEMP_MERGE_INSERTS)
                            .where(
                                exp.EQ(
                                    this=exp.Column(this="rowid", table=self._source_table()),
                                    expression=exp.Column(this="source_rowid"),
                                )
                            )
                        )
                    )
                    subquery_ignoring_temp_table = exp.Exists(this=exp.select(1).from_(self._target_table())
                                                              .where(self._merge_on_expr()))
                    subquery = exp.And(this=subquery_ignoring_temp_table, expression=not_in_temp_table_subquery)

                    not_exists_subquery = exp.Not(this=subquery)
                    if and_condition:
                        temp_match_where = exp.And(this=and_condition, expression=not_exists_subquery)
                    else:
                        temp_match_where = not_exists_subquery
                    temp_match_expr = exp.insert(
                        into=self.TEMP_MERGE_INSERTS,
                        expression=exp.select("rowid", w_idx).from_(self._source_table()).where(temp_match_where),
                    )
                    self._temp_table_inserts.append(temp_match_expr)

                    columns = [self._remove_table_alias(e) for e in then.args.get("this").expressions]
                    statement = exp.insert(
                        into=self._target_table(),
                        columns=[c.this for c in columns],
                        expression=exp.select(*(then.args.get("expression").args.get("expressions")))
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

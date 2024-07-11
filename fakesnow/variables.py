import re

import snowflake.connector.errors
from sqlglot import exp


# Implements snowflake variables: https://docs.snowflake.com/en/sql-reference/session-variables#using-variables-in-sql
# [ ] Add support for setting multiple variables in a single statement
class Variables:
    @classmethod
    def is_variable_modifier(cls, expr: exp.Expression) -> bool:
        return cls._is_set_expression(expr) or cls._is_unset_expression(expr)

    @classmethod
    def _is_set_expression(cls, expr: exp.Expression) -> bool:
        if isinstance(expr, exp.Set):
            is_set = not expr.args.get("unset")
            if is_set:  # SET varname = value;
                set_expressions = expr.args.get("expressions")
                assert set_expressions, "SET without values in expression(s) is unexpected."
                # Avoids mistakenly setting variables for statements that use SET in a different context.
                # (eg. WHEN MATCHED THEN UPDATE SET x=7)
                return isinstance(set_expressions[0], exp.SetItem)
        return False

    @classmethod
    def _is_unset_expression(cls, expr: exp.Expression) -> bool:
        if isinstance(expr, exp.Alias):
            this_expr = expr.this.args.get("this")
            return isinstance(this_expr, exp.Expression) and this_expr.this == "UNSET"
        return False

    def __init__(self) -> None:
        self._variables = {}

    def update_variables(self, expr: exp.Expression) -> None:
        if isinstance(expr, exp.Set):
            is_set = not expr.args.get("unset")
            if is_set:  # SET varname = value;
                set_expressions = expr.args.get("expressions")
                assert set_expressions, "SET without values in expression(s) is unexpected."
                eq = set_expressions[0].this
                name = eq.this.sql()
                value = eq.args.get("expression").sql()
                self._set(name, value)
            else:
                # Haven't been able to produce this in tests yet due to UNSET being parsed as an Alias expression.
                raise NotImplementedError("UNSET not supported yet")
        elif self._is_unset_expression(expr):  # Unfortunately UNSET varname; is parsed as an Alias expression :(
            alias = expr.args.get("alias")
            assert alias, "UNSET without value in alias attribute is unexpected."
            name = alias.this
            self._unset(name)

    def _set(self, name: str, value: str) -> None:
        self._variables[name] = value

    def _unset(self, name: str) -> None:
        self._variables.pop(name)

    def inline_variables(self, sql: str) -> str:
        for name, value in self._variables.items():
            sql = re.sub(rf"\${name}", value, sql, flags=re.IGNORECASE)

        if remaining_variables := re.search(r"(?<!\$)\$\w+", sql):
            raise snowflake.connector.errors.ProgrammingError(
                msg=f"Session variable '{remaining_variables.group().upper()}' does not exist"
            )
        return sql

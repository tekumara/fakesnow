import sqlglot

from fakesnow.params import index_of_placeholder


def test_index_of_placeholder_found():
    expression = sqlglot.parse_one(
        """COPY INTO identifier(?)
            FROM (SELECT id FROM ?)
            ON_ERROR = ?""",
        read="snowflake",
    )
    identifier_ph = expression.this.this.expressions[0]
    from_ph = expression.args["files"][0].this.args["from"].this.this
    on_error_ph = expression.args["params"][0].expression

    assert index_of_placeholder(expression, identifier_ph) == 0
    assert index_of_placeholder(expression, from_ph) == 1
    assert index_of_placeholder(expression, on_error_ph) == 2

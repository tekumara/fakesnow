
# ruff: noqa: E501

from __future__ import annotations

import snowflake.connector
import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms import (
    flatten,
    flatten_value_cast_as_varchar,
)
from tests.utils import strip


def test_flatten_transform() -> None:
    assert sqlglot.parse_one(
        """
            select t.id, flat.value:fruit from
            (
                select 1, parse_json('[{"fruit":"banana"}]')
                union
                select 2, parse_json('[{"fruit":"coconut"}, {"fruit":"durian"}]')
            ) as t(id, fruits), lateral flatten(input => t.fruits) AS flat
            """,
        read="snowflake",
    ).transform(flatten).sql(dialect="duckdb") == strip("""
            SELECT
                t.id,
                flat.value -> '$.fruit'
            FROM
                (SELECT
                    1,
                    JSON('[{"fruit":"banana"}]')
                UNION
                SELECT
                    2,
                    JSON('[{"fruit":"coconut"}, {"fruit":"durian"}]')) AS t(id, fruits),
                (SELECT
                        UNNEST(CAST(t.fruits AS JSON[])) AS VALUE,
                        GENERATE_SUBSCRIPTS(CAST(t.fruits AS JSON[]), 1) - 1 AS INDEX) AS flat
            """)


def test_flatten(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select t.id, flat.value:fruit from
        (
            select 1, parse_json('[{"fruit":"banana"}]')
            union
            select 2, parse_json('[{"fruit":"coconut"}, {"fruit":"durian"}]')
        ) as t(id, fruits), lateral flatten(input => t.fruits) AS flat
        order by id
        """
        # duckdb lateral join order is non-deterministic so order by id
        # within an id the order of fruits should match the json array
    )
    assert cur.fetchall() == [(1, '"banana"'), (2, '"coconut"'), (2, '"durian"')]


def test_flatten_index(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select id, f.value::varchar as v, f.index as i
        from (select column1 as id, column2 as col from (values (1, 's1,s3,s2'), (2, 's2,s1'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id;
        """
    )
    assert cur.fetchall() == [(1, "s1", 0), (1, "s3", 1), (1, "s2", 2), (2, "s2", 0), (2, "s1", 1)]


def test_flatten_value_cast_as_varchar() -> None:
    assert sqlglot.parse_one(
        """
            SELECT ID , F.VALUE::varchar as V
            FROM TEST AS T
            , LATERAL FLATTEN(input => SPLIT(T.COL, ',')) AS F;
            """,
        read="snowflake",
    ).transform(flatten_value_cast_as_varchar).sql(dialect="duckdb") == strip("""
            SELECT
                ID,
                F.VALUE ->> '$' AS V
            FROM
                TEST AS T,  CROSS JOIN UNNEST(input => STR_SPLIT(T.COL, ',')) AS F(SEQ, KEY, PATH, INDEX, VALUE, THIS)
            """)




def test_flatten_value_cast_as_varchar_transform(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select id, f.value::varchar as v
        from (select column1 as id, column2 as col from (values (1, 's1,s2,s3'), (2, 's1,s2'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id
        """
    )
    # should be raw string not json string with double quotes
    assert cur.fetchall() == [(1, "s1"), (1, "s2"), (1, "s3"), (2, "s1"), (2, "s2")]

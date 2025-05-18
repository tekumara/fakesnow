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


def test_transform_lateral_flatten() -> None:
    expected = strip("""
        SELECT
            ID,
            CAST(F.VALUE AS TEXT) AS V
        FROM
            TEST AS T,
            _FS_FLATTEN(STR_SPLIT(T.COL, ',')) AS F(SEQ, KEY, PATH, INDEX, VALUE, THIS)
    """)

    # sqlglot introduces the identifiers SEQ, KEY, PATH, INDEX, VALUE, THIS
    # for lineage tracking see https://github.com/tobymao/sqlglot/pull/2417
    assert (
        sqlglot.parse_one(
            """
            SELECT ID, F.VALUE::varchar as V
            FROM TEST AS T, LATERAL FLATTEN(input => SPLIT(T.COL, ',')) AS F;
            """,
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )


def test_transform_table_flatten() -> None:
    # table flatten is the same as lateral flatten
    # except sqlglot doesn't add identifiers for lineage tracking
    expected = strip("""
        SELECT
            ID,
            CAST(F.VALUE AS TEXT) AS V
        FROM
            TEST AS T,
            _FS_FLATTEN(STR_SPLIT(T.COL, ',')) AS F
    """)

    assert (
        sqlglot.parse_one(
            """
            SELECT ID, F.VALUE::varchar as V
            FROM TEST AS T, TABLE(FLATTEN(input => SPLIT(T.COL, ','))) AS F;
            """,
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )

    # position arg (no input =>)
    assert (
        sqlglot.parse_one(
            """
            SELECT ID, F.VALUE::varchar as V
            FROM TEST AS T, TABLE(FLATTEN(SPLIT(T.COL, ','))) AS F;
            """,
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )


def test_flatten_alias_rename(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    sql = "SELECT * FROM table(flatten([1, 2])) as rename (a, b, c)"
    assert sqlglot.parse_one(
        sql,
        read="snowflake",
    ).transform(flatten).sql(dialect="duckdb") == strip("SELECT * FROM _FS_FLATTEN([1, 2]) AS rename(a, b, c)")
    cur.execute(sql)
    assert [d.name for d in cur.description] == ["A", "B", "C", "INDEX", "VALUE", "THIS"]


def test_flatten_json(cur: snowflake.connector.cursor.SnowflakeCursor):
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
    cur.execute("""
        select id, f.value::varchar as v, f.index as i
        from (select column1 as id, column2 as col from (values (1, 's1,s3,s2'), (2, 's2,s1'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id;
        """)

    assert cur.fetchall() == [(1, "s1", 0), (1, "s3", 1), (1, "s2", 2), (2, "s2", 0), (2, "s1", 1)]


def test_flatten_value_cast_as_varchar_transform() -> None:
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


def test_flatten_value_cast_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select id, f.value::varchar as v
        from (select column1 as id, column2 as col from (values (1, 's1,s2,s3'), (2, 's1,s2'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id
        """
    )
    # should be raw string not json string with double quotes
    assert cur.fetchall() == [(1, "s1"), (1, "s2"), (1, "s3"), (2, "s1"), (2, "s2")]

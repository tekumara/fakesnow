from __future__ import annotations

import snowflake.connector
import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms import flatten
from tests.matchers import IsResultMetadata
from tests.utils import strip


def test_transform_lateral_flatten() -> None:
    # sqlglot introduces the identifiers SEQ, KEY, PATH, INDEX, VALUE, THIS
    # for lineage tracking see https://github.com/tobymao/sqlglot/pull/2417
    expected = strip("SELECT * FROM _FS_FLATTEN([1, 2]) AS F(SEQ, KEY, PATH, INDEX, VALUE, THIS)")

    assert (
        sqlglot.parse_one(
            "SELECT * FROM LATERAL FLATTEN(input => [1,2]) AS F",
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )


def test_transform_table_flatten() -> None:
    # table flatten is the same as lateral flatten
    # except sqlglot doesn't add identifiers for lineage tracking
    expected = strip("SELECT * FROM _FS_FLATTEN([1, 2]) AS F")

    assert (
        sqlglot.parse_one(
            "SELECT * FROM TABLE(FLATTEN(input => [1,2])) AS F",
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )

    # position arg (no input =>)
    assert (
        sqlglot.parse_one(
            "SELECT * FROM TABLE(FLATTEN([1,2])) AS F",
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == expected
    )


def test_flatten_alias_none(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    sql = "SELECT * FROM table(flatten([1, 2]))"
    assert sqlglot.parse_one(
        sql,
        read="snowflake",
    ).transform(flatten).sql(dialect="duckdb") == strip("SELECT * FROM _FS_FLATTEN([1, 2])")
    cur.execute(sql)
    # check order, names and types of cols
    assert cur.description == [
        IsResultMetadata(name="SEQ", type_code=0),
        IsResultMetadata(name="KEY", type_code=2),
        IsResultMetadata(name="PATH", type_code=2),
        IsResultMetadata(name="INDEX", type_code=0),
        IsResultMetadata(name="VALUE", type_code=5),
        IsResultMetadata(name="THIS", type_code=5),
    ]


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


def test_flatten_value_cast_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT VALUE::VARCHAR FROM LATERAL FLATTEN(input => ['a','b'])")
    # should be raw string not json string with double quotes
    assert cur.fetchall() == [("a",), ("b",)]

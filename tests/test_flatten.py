from __future__ import annotations

import json

import snowflake.connector
import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms import flatten
from tests.matchers import IsResultMetadata
from tests.utils import strip


def test_transform_lateral_flatten() -> None:
    # sqlglot introduces the identifiers SEQ, KEY, PATH, INDEX, VALUE, THIS
    # for lineage tracking see https://github.com/tobymao/sqlglot/pull/2417
    # standalone LATERAL FLATTEN (no cross join) uses default order (no reverse_order)
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
        order by id, index
        """
        # unlike Snowflake, duckdb cross join doesn't preserve order of json array
        # so we need to order by index (after id) to preserve the order of fruits
    )
    assert cur.fetchall() == [(1, '"banana"'), (2, '"coconut"'), (2, '"durian"')]


def test_flatten_index(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(
        """
        select id, f.value::varchar as v, f.index as i
        from (select column1 as id, column2 as col from (values (1, 's1,s3,s2'), (2, 's2,s1'))) as t
        , lateral flatten(input => split(t.col, ',')) as f order by id, index
        """
        # as above we need to define order to get a natural ordering
    )

    assert cur.fetchall() == [(1, "s1", 0), (1, "s3", 1), (1, "s2", 2), (2, "s2", 0), (2, "s1", 1)]


def test_flatten_value_cast_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT VALUE::VARCHAR FROM LATERAL FLATTEN(input => ['a','b'])")
    # should be raw string not json string with double quotes
    # standalone LATERAL FLATTEN (no cross join) uses default order (ascending)
    assert cur.fetchall() == [("a",), ("b",)]


def test_flatten_object_empty(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT * FROM LATERAL FLATTEN(input => parse_json('{}'))")
    assert cur.fetchall() == []


def test_flatten_object(cur: snowflake.connector.cursor.SnowflakeCursor):
    """Flatten a JSON object produces rows with KEY and VALUE."""
    cur.execute(
        """
        SELECT key, value
        FROM LATERAL FLATTEN(input => parse_json('{"rocket":"2","anvil":"32"}'))
        ORDER BY key
        """
    )
    assert cur.fetchall() == [("anvil", '"32"'), ("rocket", '"2"')]


def test_flatten_object_key_value_types(cur: snowflake.connector.cursor.SnowflakeCursor):
    """KEY is populated and INDEX is NULL when flattening an object."""
    cur.execute(
        """
        SELECT key, index, path
        FROM LATERAL FLATTEN(input => parse_json('{"a":1,"b":2}'))
        ORDER BY key
        """
    )
    assert cur.fetchall() == [("a", None, "a"), ("b", None, "b")]


def test_flatten_object_cross_join(cur: snowflake.connector.cursor.SnowflakeCursor):
    """Flatten a JSON object column via cross join (the toon_pass pattern)."""
    cur.execute("CREATE TABLE events (user_id INT, gifts VARIANT)")
    cur.execute("""INSERT INTO events SELECT 1, parse_json('{"rocket":"2","anvil":"32"}')""")
    cur.execute("""INSERT INTO events SELECT 2, parse_json('{"coin":"100"}')""")

    cur.execute(
        """
        SELECT
            e.user_id,
            f.key AS item_name,
            f.value::varchar AS amount
        FROM events e, LATERAL FLATTEN(input => e.gifts) AS f
        ORDER BY e.user_id, f.key
        """
    )
    assert cur.fetchall() == [(1, "anvil", "32"), (1, "rocket", "2"), (2, "coin", "100")]


def test_flatten_object_nested_values(cur: snowflake.connector.cursor.SnowflakeCursor):
    """Flatten an object whose values are themselves objects."""
    cur.execute(
        """
        SELECT key, value
        FROM LATERAL FLATTEN(input => parse_json('{"a":{"x":1},"b":{"y":2}}'))
        ORDER BY key
        """
    )
    assert cur.fetchall() == [("a", '{"x":1}'), ("b", '{"y":2}')]


def test_flatten_seq(cur: snowflake.connector.cursor.SnowflakeCursor):
    """SEQ is the same for all rows from the same input, different across inputs."""
    cur.execute("CREATE TABLE t (id INT, arr VARIANT)")
    cur.execute("INSERT INTO t SELECT 1, parse_json('[10,20]')")
    cur.execute("INSERT INTO t SELECT 2, parse_json('[30,40,50]')")

    cur.execute(
        """
        SELECT t.id, f.seq, f.index
        FROM t, LATERAL FLATTEN(input => t.arr) AS f
        ORDER BY t.id, f.index
        """
    )
    rows = cur.fetchall()

    # All rows from id=1 share the same SEQ
    seqs_id1 = {r[1] for r in rows if r[0] == 1}
    assert len(seqs_id1) == 1, f"Expected 1 unique SEQ for id=1, got {seqs_id1}"

    # All rows from id=2 share the same SEQ
    seqs_id2 = {r[1] for r in rows if r[0] == 2}
    assert len(seqs_id2) == 1, f"Expected 1 unique SEQ for id=2, got {seqs_id2}"

    assert seqs_id1 != seqs_id2, "SEQ should differ across different inputs"
    assert all(r[1] is not None for r in rows), "SEQ should not be NULL"


def test_flatten_object_null_values(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""
        SELECT key, value, value IS NULL as is_null
        FROM LATERAL FLATTEN(input => parse_json('{"a":null,"b":1}'))
        ORDER BY key
    """)
    # Note: JSON null becomes string "null", not SQL NULL
    assert cur.fetchall() == [("a", "null", False), ("b", "1", False)]


def test_flatten_object_this_column(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""
        SELECT key, this
        FROM LATERAL FLATTEN(input => parse_json('{"a":1, "b":2}'))
    """)
    rows = cur.fetchall()
    assert json.loads(rows[1][1]) == {"a": 1, "b": 2}

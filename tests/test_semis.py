# ruff: noqa: E501

from __future__ import annotations

import json

import snowflake.connector
import snowflake.connector.cursor

from tests.utils import dindent, indent


def test_array_construct(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT ARRAY_CONSTRUCT(PARSE_JSON('null'), 'hello', 3.01, 4, 5);")
    assert indent(cur.fetchall()) == [('[\n  null,\n  "hello",\n  3.01,\n  4,\n  5\n]',)]


def test_array_construct_compact(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("SELECT ARRAY_CONSTRUCT_COMPACT(1, 2, NULL, 3, NULL, 4)")
    assert indent(cur.fetchall()) == [("[\n  1,\n  2,\n  3,\n  4\n]",)]


def test_get_path_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""select parse_json('{"fruit":"banana"}'):fruit""")
    assert cur.fetchall() == [('"banana"',)]

    # converting json to varchar returns unquoted string
    cur.execute("""select parse_json('{"fruit":"banana"}'):fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    # nested json
    cur.execute("""select get_path(parse_json('{"food":{"fruit":"banana"}}'), 'food.fruit')::varchar""")
    assert cur.fetchall() == [("banana",)]

    cur.execute("""select parse_json('{"food":{"fruit":"banana"}}'):food.fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    cur.execute("""select parse_json('{"food":{"fruit":"banana"}}'):food:fruit::varchar""")
    assert cur.fetchall() == [("banana",)]

    # json number is varchar
    cur.execute("""select parse_json('{"count":42}'):count""")
    assert cur.fetchall() == [("42",)]

    # lower/upper converts to varchar (ie: no quotes) ¯\_(ツ)_/¯
    cur.execute("""select upper(parse_json('{"fruit":"banana"}'):fruit)""")
    assert cur.fetchall() == [("BANANA",)]

    cur.execute("""select lower(parse_json('{"fruit":"banana"}'):fruit)""")
    assert cur.fetchall() == [("banana",)]

    # lower/upper converts json number to varchar too
    cur.execute("""select upper(parse_json('{"count":"42"}'):count)""")
    assert cur.fetchall() == [("42",)]


def test_get_path_as_number(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE TABLE example (j VARIANT)")
    dcur.execute("""INSERT INTO example SELECT PARSE_JSON('{"str": "100", "num" : 200}')""")

    dcur.execute("SELECT j:str::varchar as j_str_varchar, j:num::varchar as j_num_varchar FROM example")
    assert dcur.fetchall() == [{"J_STR_VARCHAR": "100", "J_NUM_VARCHAR": "200"}]

    dcur.execute("SELECT j:str::number as j_str_number, j:num::number as j_num_number FROM example")
    assert dcur.fetchall() == [{"J_STR_NUMBER": 100, "J_NUM_NUMBER": 200}]


def test_get_path_precedence(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("select {'K1': {'K2': 1}} as col where col:K1:K2 > 0")
    assert indent(cur.fetchall()) == [('{\n  "K1": {\n    "K2": 1\n  }\n}',)]

    cur.execute(
        """select parse_json('{"K1": "a", "K2": "b"}') as col, case when col:K1::VARCHAR = 'a' and col:K2::VARCHAR = 'b' then 'yes' end"""
    )
    assert indent(cur.fetchall()) == [('{\n  "K1": "a",\n  "K2": "b"\n}', "yes")]


def test_indices_cast_as_varchar(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("""select parse_json('["banana", "coconut"]')[0]::varchar""")
    assert cur.fetchall() == [("banana",)]


def test_object_construct(conn: snowflake.connector.SnowflakeConnection):
    with conn.cursor() as cur:
        cur.execute("SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB', 'c',null)")

        # TODO: strip null within duckdb via python UDF
        def strip_none_values(d: dict) -> dict:
            return {k: v for k, v in d.items() if v}

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1,\n  "b": "BBBB"\n}')

    with conn.cursor() as cur:
        cur.execute("SELECT OBJECT_CONSTRUCT('a', 1, null, 'nulkeyed') as col")

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[0])) == json.loads('{\n  "a": 1\n}')

    with conn.cursor() as cur:
        cur.execute(
            "SELECT NULL as col, OBJECT_CONSTRUCT( 'k1', 'v1', 'k2', CASE WHEN ZEROIFNULL(col) + 1 >= 2 THEN 'v2' ELSE NULL END, 'k3', 'v3')"
        )

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[1])) == json.loads('{\n  "k1": "v1",\n  "k3": "v3"\n}')

    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 as col, OBJECT_CONSTRUCT( 'k1', 'v1', 'k2', CASE WHEN ZEROIFNULL(col) + 1 >= 2 THEN 'v2' ELSE NULL END, 'k3', 'v3')"
        )

        result = cur.fetchone()
        assert isinstance(result, tuple)
        assert strip_none_values(json.loads(result[1])) == json.loads(
            '{\n  "k1": "v1",\n  "k2": "v2",\n  "k3": "v3"\n}'
        )


def test_semi_structured_types(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("create or replace table semis (emails array, names object, notes variant)")
    cur.execute(
        """insert into semis(emails, names, notes) SELECT ['A', 'B'], OBJECT_CONSTRUCT('k','v1'), ARRAY_CONSTRUCT('foo')::VARIANT"""
    )
    cur.execute(
        """insert into semis(emails, names, notes) SELECT ['C','D'], parse_json('{"k": "v2"}'), parse_json('{"b": "ar"}')"""
    )

    # results are returned as strings, because the underlying type is JSON (duckdb) / VARIANT (snowflake)

    cur.execute("select emails from semis")
    assert indent(cur.fetchall()) == [('[\n  "A",\n  "B"\n]',), ('[\n  "C",\n  "D"\n]',)]

    cur.execute("select emails[0] from semis")
    assert cur.fetchall() == [('"A"',), ('"C"',)]

    cur.execute("select names['k'] from semis")
    assert cur.fetchall() == [('"v1"',), ('"v2"',)]

    cur.execute("select notes[0] from semis")
    assert cur.fetchall() == [('"foo"',), (None,)]

    cur.execute(
        """
            SELECT OBJECT_CONSTRUCT('key_1', 'one', 'key_2', NULL) AS WITHOUT_KEEP_NULL,
                   OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', 'key_2', NULL) AS KEEP_NULL_1,
                   OBJECT_CONSTRUCT_KEEP_NULL('key_1', 'one', NULL, 'two') AS KEEP_NULL_2
        """
    )
    assert indent(cur.fetchall()) == [
        ('{\n  "key_1": "one"\n}', '{\n  "key_1": "one",\n  "key_2": null\n}', '{\n  "key_1": "one"\n}')
    ]


def test_try_parse_json(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("""SELECT TRY_PARSE_JSON('{"first":"foo", "last":"bar"}') AS j""")
    assert dindent(dcur.fetchall()) == [{"J": '{\n  "first": "foo",\n  "last": "bar"\n}'}]

    dcur.execute("""SELECT TRY_PARSE_JSON('{invalid: ,]') AS j""")
    assert dcur.fetchall() == [{"J": None}]

from pathlib import Path

import sqlglot
from sqlglot import exp

from fakesnow.transforms import (
    SUCCESS_NOP,
    array_size,
    create_database,
    describe_table,
    drop_schema_cascade,
    extract_comment_on_columns,
    extract_comment_on_table,
    extract_text_length,
    flatten,
    float_to_double,
    identifier,
    indices_to_json_extract,
    information_schema_fs_columns_snowflake,
    information_schema_fs_tables_ext,
    integer_precision,
    json_extract_cased_as_varchar,
    json_extract_cast_as_varchar,
    object_construct,
    parse_json,
    random,
    regex_replace,
    regex_substr,
    sample,
    semi_structured_types,
    set_schema,
    show_objects_tables,
    show_schemas,
    tag,
    timestamp_ntz_ns,
    to_date,
    to_decimal,
    to_timestamp,
    to_timestamp_ntz,
    upper_case_unquoted_identifiers,
    values_columns,
)


def test_array_size() -> None:
    assert (
        sqlglot.parse_one("""select array_size(parse_json('["a","b"]'))""").transform(array_size).sql(dialect="duckdb")
        == """SELECT CASE WHEN JSON_ARRAY_LENGTH(JSON('["a","b"]')) THEN JSON_ARRAY_LENGTH(JSON('["a","b"]')) END"""
    )


def test_create_database() -> None:
    e = sqlglot.parse_one("create database foobar").transform(create_database)
    assert e.sql() == "ATTACH DATABASE ':memory:' AS foobar"
    assert e.args["create_db_name"] == "foobar"

    assert (
        sqlglot.parse_one("create database foobar").transform(create_database, db_path=Path("/tmp")).sql()
        == "ATTACH DATABASE '/tmp/foobar.db' AS foobar"
    )

    assert (
        sqlglot.parse_one("create database foobar").transform(create_database, db_path=Path(".databases/")).sql()
        == "ATTACH DATABASE '.databases/foobar.db' AS foobar"
    )


def test_describe_table() -> None:
    assert "SELECT" in sqlglot.parse_one("describe table db1.schema1.table1").transform(describe_table).sql()


def test_drop_schema_cascade() -> None:
    assert (
        sqlglot.parse_one("drop schema schema1").transform(drop_schema_cascade).sql() == "DROP schema schema1 CASCADE"
    )


def test_extract_comment_on_columns() -> None:
    e = sqlglot.parse_one("ALTER TABLE ingredients ALTER amount COMMENT 'tablespoons'").transform(
        extract_comment_on_columns
    )
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["col_comments"] == [("amount", "tablespoons")]

    # TODO
    # e = sqlglot.parse_one(
    #     "ALTER TABLE ingredients ALTER name DROP DEFAULT ALTER amount COMMENT 'tablespoons'"
    # ).transform(extract_comment_on_columns)
    # assert e.sql() == "ALTER TABLE ingredients ALTER name DROP DEFAULT"
    # assert e.args["col_comments"] == [("amount", "tablespoons")]


def test_extract_comment_on_table() -> None:
    table1 = exp.Table(this=exp.Identifier(this="table1", quoted=False))

    e = sqlglot.parse_one("create table table1(id int) comment = 'foo bar'").transform(extract_comment_on_table)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == (table1, "foo bar")

    e = sqlglot.parse_one("create table table1(id int) comment = foobar").transform(extract_comment_on_table)
    assert e.sql() == "CREATE TABLE table1 (id INT)"
    assert e.args["table_comment"] == (table1, "foobar")

    e = sqlglot.parse_one("COMMENT ON TABLE table1 IS 'comment1'").transform(extract_comment_on_table)
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["table_comment"] == (table1, "comment1")

    e = sqlglot.parse_one("COMMENT ON TABLE table1 IS $$comment2$$", read="snowflake").transform(
        extract_comment_on_table
    )
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["table_comment"] == (table1, "comment2")

    e = sqlglot.parse_one("ALTER TABLE table1 SET COMMENT = 'comment1'", read="snowflake").transform(
        extract_comment_on_table
    )
    assert e.sql() == "SELECT 'Statement executed successfully.'"
    assert e.args["table_comment"] == (table1, "comment1")


def test_extract_text_length() -> None:
    sql = "CREATE TABLE table1 (t1 VARCHAR, t2 VARCHAR(10), t3 TEXT(20), i1 BIGINT)"
    e = sqlglot.parse_one(sql).transform(extract_text_length)
    assert e.sql() == sql
    assert e.args["text_lengths"] == [("t1", 16777216), ("t2", 10), ("t3", 20)]


def test_flatten() -> None:
    assert (
        sqlglot.parse_one(
            """
            select t.id, flat.value:fruit from
            (
                select 1, parse_json('[{"fruit":"banana"}]')
                union
                select 2, parse_json('[{"fruit":"coconut"}, {"fruit":"durian"}]')
            ) as t(id, fruits), lateral flatten(input => t.fruits) AS flat
            """,
            read="snowflake",
        )
        .transform(flatten)
        .sql(dialect="duckdb")
        == """SELECT t.id, flat.value -> '$.fruit' FROM (SELECT 1, JSON('[{"fruit":"banana"}]') UNION SELECT 2, JSON('[{"fruit":"coconut"}, {"fruit":"durian"}]')) AS t(id, fruits), LATERAL UNNEST(CAST(t.fruits AS JSON[])) AS flat(VALUE)"""  # noqa: E501
    )


def test_float_to_double() -> None:
    assert (
        sqlglot.parse_one("create table example (f float, f4 float4, f8 float8, d double, r real)")
        .transform(float_to_double)
        .sql()
        == "CREATE TABLE example (f DOUBLE, f4 DOUBLE, f8 DOUBLE, d DOUBLE, r DOUBLE)"
    )


def test_identifier() -> None:
    assert (
        sqlglot.parse_one("select * from identifier('example')").transform(identifier).sql() == "SELECT * FROM example"
    )


def test_indices_to_object() -> None:
    assert (
        sqlglot.parse_one("SELECT myarray[0] FROM table1").transform(indices_to_json_extract).sql()
        == "SELECT JSON_EXTRACT(myarray, '$[0]') FROM table1"
    )

    assert (
        sqlglot.parse_one("SELECT name['k'] FROM semi").transform(indices_to_json_extract).sql(dialect="duckdb")
        == "SELECT name -> '$.k' FROM semi"
    )


def test_integer_precision() -> None:
    assert (
        sqlglot.parse_one(
            """
                create table example (
                    XNUMBER82 NUMBER(8, 2), XNUMBER NUMBER,  XDECIMAL DECIMAL, XNUMERIC NUMERIC,
                    XINT INT, XINTEGER INTEGER, XBIGINT BIGINT, XSMALLINT SMALLINT, XTINYINT TINYINT, XBYTEINT BYTEINT
                )
            """,
            read="snowflake",
        )
        .transform(integer_precision)
        .sql(dialect="duckdb")
        == "CREATE TABLE example (XNUMBER82 DECIMAL(8, 2), XNUMBER BIGINT, XDECIMAL BIGINT, XNUMERIC BIGINT, XINT BIGINT, XINTEGER BIGINT, XBIGINT BIGINT, XSMALLINT BIGINT, XTINYINT BIGINT, XBYTEINT BIGINT)"  # noqa: E501
    )


def test_information_schema_fs_columns_snowflake() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.COLUMNS")
        .transform(information_schema_fs_columns_snowflake)
        .sql()
        == "SELECT * FROM INFORMATION_SCHEMA._FS_COLUMNS_SNOWFLAKE"
    )


def test_information_schema_fs_tables_ext() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM INFORMATION_SCHEMA.TABLES").transform(information_schema_fs_tables_ext).sql()
        == "SELECT * FROM INFORMATION_SCHEMA.TABLES LEFT JOIN information_schema._fs_tables_ext ON tables.table_catalog = _fs_tables_ext.ext_table_catalog AND tables.table_schema = _fs_tables_ext.ext_table_schema AND tables.table_name = _fs_tables_ext.ext_table_name"  # noqa: E501
    )


def test_json_extract_cased_as_varchar() -> None:
    assert (
        sqlglot.parse_one(
            """select upper(parse_json('{"fruit":"banana"}'):fruit)""",
            read="snowflake",
        )
        .transform(json_extract_cased_as_varchar)
        .sql(dialect="duckdb")
        == """SELECT UPPER(JSON('{"fruit":"banana"}') ->> '$.fruit')"""
    )
    assert (
        sqlglot.parse_one(
            """select lower(parse_json('{"fruit":"banana"}'):fruit)""",
            read="snowflake",
        )
        .transform(json_extract_cased_as_varchar)
        .sql(dialect="duckdb")
        == """SELECT LOWER(JSON('{"fruit":"banana"}') ->> '$.fruit')"""
    )


def test_json_extract_cast_as_varchar() -> None:
    assert (
        sqlglot.parse_one(
            """select parse_json('{"fruit":"banana"}'):fruit::varchar""",
            read="snowflake",
        )
        .transform(json_extract_cast_as_varchar)
        .sql(dialect="duckdb")
        == """SELECT JSON('{"fruit":"banana"}') ->> '$.fruit'"""
    )


def test_object_construct() -> None:
    assert (
        sqlglot.parse_one(
            "SELECT OBJECT_CONSTRUCT('a',1,'b','BBBB','c',null,'d',PARSE_JSON('NULL'), null, 'foo')",
            read="snowflake",
        )
        .transform(object_construct)
        .sql(dialect="duckdb")
        == "SELECT TO_JSON({'a': 1, 'b': 'BBBB', 'd': JSON('NULL')})"
    )


def test_parse_json() -> None:
    assert (
        sqlglot.parse_one("""insert into table1 (name) select parse_json('{"first":"foo", "last":"bar"}')""")
        .transform(parse_json)
        .sql(dialect="duckdb")
        == """INSERT INTO table1 (name) SELECT JSON('{"first":"foo", "last":"bar"}')"""
    )


def test_random() -> None:
    e = sqlglot.parse_one("select random(420)").transform(random)

    assert e.sql(dialect="duckdb") == """SELECT CAST(((RANDOM() - 0.5) * 9223372036854775807) AS BIGINT)"""
    assert e.args["seed"] == "420/2147483647-0.5"


def test_regex_replace() -> None:
    assert (
        sqlglot.parse_one("SELECT regexp_replace('abc123', '\\\\D', '')").transform(regex_replace).sql()
        == "SELECT REGEXP_REPLACE('abc123', '\\D', '', 'g')"
    )


def test_regex_substr() -> None:
    assert (
        sqlglot.parse_one("SELECT regexp_substr(string1, 'the\\\\W+\\\\w+')", read="snowflake")
        .transform(regex_substr)
        .sql(dialect="duckdb")
        == "SELECT REGEXP_EXTRACT_ALL(string1[1 : ], 'the\\W+\\w+', 0, '')[1]"
    )


def test_sample() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM example USING SAMPLE (50) SEED (420)", read="snowflake")
        .transform(sample)
        .sql(dialect="duckdb")
        == "SELECT * FROM example USING SAMPLE BERNOULLI (50 PERCENT) REPEATABLE (420)"
    )


def test_semi_structured_types() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name object)").transform(semi_structured_types).sql()
        == "CREATE TABLE table1 (name JSON)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name array)").transform(semi_structured_types).sql(dialect="duckdb")
        == "CREATE TABLE table1 (name JSON)"
    )

    assert (
        sqlglot.parse_one("CREATE TABLE table1 (name variant)").transform(semi_structured_types).sql()
        == "CREATE TABLE table1 (name JSON)"
    )


def test_show_objects_tables() -> None:
    assert (
        sqlglot.parse_one("show terse objects in database db1 limit 10", read="snowflake")
        .transform(show_objects_tables)
        .sql()
        == """SELECT CAST(UNIX_TO_TIME(0) AS TIMESTAMPTZ) AS "created_on", table_name AS "name", CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS "kind", table_catalog AS "database_name", table_schema AS "schema_name" FROM information_schema.tables WHERE NOT (table_schema = 'information_schema' AND table_name LIKE '_fs_%%') AND table_catalog = 'db1' LIMIT 10"""  # noqa: E501
    )
    assert (
        sqlglot.parse_one("show terse objects in db1.schema1", read="snowflake").transform(show_objects_tables).sql()
        == """SELECT CAST(UNIX_TO_TIME(0) AS TIMESTAMPTZ) AS "created_on", table_name AS "name", CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS "kind", table_catalog AS "database_name", table_schema AS "schema_name" FROM information_schema.tables WHERE NOT (table_schema = 'information_schema' AND table_name LIKE '_fs_%%') AND table_catalog = 'db1' AND table_schema = 'schema1'"""  # noqa: E501
    )
    assert (
        sqlglot.parse_one("show terse objects in database", read="snowflake").transform(show_objects_tables).sql()
        == """SELECT CAST(UNIX_TO_TIME(0) AS TIMESTAMPTZ) AS "created_on", table_name AS "name", CASE WHEN table_type = 'BASE TABLE' THEN 'TABLE' ELSE table_type END AS "kind", table_catalog AS "database_name", table_schema AS "schema_name" FROM information_schema.tables WHERE NOT (table_schema = 'information_schema' AND table_name LIKE '_fs_%%')"""  # noqa: E501
    )


def test_show_schemas() -> None:
    assert (
        sqlglot.parse_one("show terse schemas in database db1", read="snowflake").transform(show_schemas).sql()
        == """SELECT CAST(UNIX_TO_TIME(0) AS TIMESTAMPTZ) AS "created_on", schema_name AS "name", NULL AS "kind", catalog_name AS "database_name", NULL AS "schema_name" FROM information_schema.schemata WHERE NOT catalog_name IN ('memory', 'system', 'temp') AND NOT schema_name IN ('main', 'pg_catalog') AND catalog_name = 'db1'"""  # noqa: E501
    )


def test_tag() -> None:
    assert sqlglot.parse_one("ALTER TABLE table1 SET TAG foo='bar'", read="snowflake").transform(tag) == SUCCESS_NOP


def test_timestamp_ntz_ns() -> None:
    assert (
        sqlglot.parse_one("CREATE TABLE table1(ts TIMESTAMP_NTZ(9))", read="snowflake")
        .transform(timestamp_ntz_ns)
        .sql(dialect="duckdb")
        == "CREATE TABLE table1 (ts TIMESTAMP)"
    )


def test_to_date() -> None:
    assert (
        sqlglot.parse_one("SELECT to_date(to_timestamp(0))").transform(to_date).sql()
        == "SELECT CAST(TO_TIMESTAMP(0) AS DATE)"
    )


def test_to_decimal() -> None:
    assert (
        sqlglot.parse_one("SELECT to_decimal('1.245',10,2)").transform(to_decimal).sql()
        == "SELECT CAST('1.245' AS DECIMAL(10, 2))"
    )


def test_to_timestamp() -> None:
    assert (
        sqlglot.parse_one("SELECT to_timestamp(0)", read="snowflake").transform(to_timestamp).sql(dialect="duckdb")
        == "SELECT CAST(TO_TIMESTAMP(0) AS TIMESTAMP)"
    )


def test_to_timestamp_ntz() -> None:
    assert sqlglot.parse_one("SELECT to_timestamp_ntz('2013-04-05 01:02:03')", read="snowflake").transform(
        to_timestamp_ntz
    ) == sqlglot.parse_one("SELECT to_timestamp('2013-04-05 01:02:03')", read="snowflake")


def test_use() -> None:
    assert (
        sqlglot.parse_one("use database marts").transform(set_schema, current_database=None).sql()
        == "SET schema = 'marts.main'"
    )

    assert (
        sqlglot.parse_one("USE SCHEMA foo").transform(set_schema, current_database=None).sql()
        == "SET schema = 'missing_database.foo'"
    )

    assert (
        sqlglot.parse_one("use schema bar").transform(set_schema, current_database="foo").sql()
        == "SET schema = 'foo.bar'"
    )

    assert (
        sqlglot.parse_one("use schema foo.bar").transform(set_schema, current_database="marts").sql()
        == "SET schema = 'foo.bar'"
    )


def test_upper_case_unquoted_identifiers() -> None:
    assert (
        sqlglot.parse_one("select name, name as fname from table1").transform(upper_case_unquoted_identifiers).sql()
        == "SELECT NAME, NAME AS FNAME FROM TABLE1"
    )


def test_values_columns() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM VALUES ('Amsterdam', 1)").transform(values_columns).sql()
        == """SELECT * FROM (VALUES ('Amsterdam', 1)) AS _("COLUMN1", "COLUMN2")"""
    )

    # values without select aren't transformed
    assert (
        sqlglot.parse_one("INSERT INTO cities VALUES ('Amsterdam', 1)").transform(values_columns).sql()
        == "INSERT INTO cities VALUES ('Amsterdam', 1)"
    )

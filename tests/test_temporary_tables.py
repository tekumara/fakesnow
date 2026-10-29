import pytest
import snowflake.connector
import snowflake.connector.cursor


def test_create_qualified_temporary_table_as_select(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # the statement dbt's unit tests generate
    cur.execute('create or replace temporary table "DB1"."SCHEMA1"."MODEL__dbt_tmp" as (select 1 as x)')

    cur.execute('select * from "DB1"."SCHEMA1"."MODEL__dbt_tmp"')
    assert cur.fetchall() == [(1,)]


def test_insert_into_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t (x int)")

    cur.execute("insert into db1.schema1.t values (1)")

    cur.execute("select * from t")
    assert cur.fetchall() == [(1,)]


def test_insert_select_within_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t as select 1 as x")

    cur.execute("insert into db1.schema1.t select x + 1 from db1.schema1.t")

    cur.execute("select * from t order by x")
    assert cur.fetchall() == [(1,), (2,)]


def test_create_temporary_table_in_missing_schema(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("create temp table db1.missing.t (x int)")

    assert str(excinfo.value).startswith(
        "002003 (02000): SQL compilation error:\nSchema 'DB1.MISSING' does not exist or not authorized."
    )


def test_use_schema_named_like_a_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create schema t")
    cur.execute("use schema schema1")
    cur.execute("create temp table t (x int)")

    cur.execute("use schema t")

    cur.execute("select current_schema()")
    assert cur.fetchall() == [("T",)]


def test_create_stage_named_like_a_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t (x int)")

    cur.execute("create stage t")

    assert cur.fetchall() == [("Stage area T successfully created.",)]


def test_drop_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t (x int)")

    # the statement dbt's unit tests use to drop their temporary table
    cur.execute("drop table if exists db1.schema1.t cascade")

    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("select * from t")


def test_describe_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t (x number(10, 2), s varchar(10))")

    cur.execute("describe table db1.schema1.t")
    assert [(name, type_) for name, type_, *_ in cur.fetchall()] == [("X", "NUMBER(10,2)"), ("S", "VARCHAR(10)")]


def test_temporary_table_shadows_permanent_table_in_its_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table db1.schema1.t as select 'permanent' as x")
    cur.execute("create temp table db1.schema1.t as select 'temporary' as x")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [("temporary",)]


def test_temporary_table_does_not_shadow_table_in_another_schema(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create schema db1.other")
    cur.execute("create table db1.other.t as select 'other' as x")
    cur.execute("use schema schema1")
    cur.execute("create temp table db1.schema1.t as select 'temporary' as x")

    cur.execute("select * from db1.other.t")
    assert cur.fetchall() == [("other",)]


def test_dropping_temporary_table_reveals_permanent_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table db1.schema1.t as select 'permanent' as x")
    cur.execute("create temp table db1.schema1.t as select 'temporary' as x")

    cur.execute("drop table db1.schema1.t")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [("permanent",)]


def test_temporary_table_text_length_does_not_change_permanent_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table t (s varchar(20))")
    cur.execute("create temp table t (s varchar(10))")
    cur.execute("drop table db1.schema1.t")

    cur.execute("describe table t")
    assert [(name, type_) for name, type_, *_ in cur.fetchall()] == [("S", "VARCHAR(20)")]


def test_unqualified_name_from_another_schema_does_not_find_temporary_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create temp table t as select 1 as x")
    cur.execute("create schema db1.other")
    cur.execute("use schema db1.other")

    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
        cur.execute("select * from t")


def test_qualified_name_from_another_schema_finds_temporary_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create temp table t as select 1 as x")
    cur.execute("create schema db1.other")
    cur.execute("use schema db1.other")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [(1,)]


@pytest.mark.parametrize(
    "create", ["create temp table", "create or replace temp table", "create temp table if not exists"]
)
def test_same_temporary_table_name_in_another_schema_is_rejected(
    cur: snowflake.connector.cursor.SnowflakeCursor, create: str
) -> None:
    # fakesnow deviation: duckdb holds one temporary table per name, snowflake would create a second
    cur.execute("create temp table t as select 1 as x")
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")

    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="SCHEMA1"):
        cur.execute(f"{create} db1.other.t as select 2 as x")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [(1,)]


def test_clone_into_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create table source as select 1 as x")

    cur.execute("create temp table db1.schema1.t clone source")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [(1,)]


def test_cloned_temporary_table_is_not_visible_to_another_session(_fakesnow: None) -> None:
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as first_conn,
        snowflake.connector.connect(database="db1", schema="schema1") as second_conn,
        first_conn.cursor() as first,
        second_conn.cursor() as second,
    ):
        first.execute("create table source as select 1 as x")
        first.execute("create temp table db1.schema1.t clone source")

        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="does not exist"):
            second.execute("select * from db1.schema1.t")


def test_update_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t as select 1 as x")

    cur.execute("update db1.schema1.t set x = 2")

    cur.execute("select * from t")
    assert cur.fetchall() == [(2,)]


def test_delete_from_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t as select 1 as x")

    cur.execute("delete from db1.schema1.t")

    cur.execute("select * from t")
    assert cur.fetchall() == []


def test_merge_into_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t as select 1 as x")

    cur.execute("merge into db1.schema1.t using (select 1 as x) s on t.x = s.x when matched then update set x = 2")

    cur.execute("select * from t")
    assert cur.fetchall() == [(2,)]


def test_insert_overwrite_qualified_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("create temp table t as select 'old' as x")

    cur.execute("insert overwrite into db1.schema1.t select 'new' as x")

    cur.execute("select * from t")
    assert cur.fetchall() == [("new",)]


def test_insert_overwrite_shadowing_temporary_table_keeps_permanent_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("create table db1.schema1.t as select 'permanent' as x")
    cur.execute("create temp table db1.schema1.t as select 'temporary' as x")

    cur.execute("insert overwrite into db1.schema1.t select 'new' as x")
    cur.execute("drop table db1.schema1.t")

    cur.execute("select * from db1.schema1.t")
    assert cur.fetchall() == [("permanent",)]


def test_temporary_table_name_in_another_case_is_rejected(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    # fakesnow deviation: duckdb matches temporary table names ignoring case, snowflake would create a second table
    cur.execute('create temp table "t" as select 1 as x')
    cur.execute("create schema db1.other")
    cur.execute("use schema schema1")

    with pytest.raises(snowflake.connector.errors.ProgrammingError, match="SCHEMA1"):
        cur.execute("create or replace temp table db1.other.T as select 2 as x")

    cur.execute('select * from db1.schema1."t"')
    assert cur.fetchall() == [(1,)]

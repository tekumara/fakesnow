# pyright: reportOptionalMemberAccess=false

import datetime
from typing import cast

import pytest
import pytz
import snowflake.connector.cursor


def test_show_databases(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show databases")
    assert dcur.fetchall() == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "DB1",
            "is_default": "N",
            "is_current": "N",
            "origin": "",
            "owner": "SYSADMIN",
            "comment": None,
            "options": "",
            "retention_time": 1,
            "kind": "STANDARD",
            "budget": None,
            "owner_role_type": "ROLE",
            "object_visibility": None,
        }
    ]
    # test describe
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "is_default",
        "is_current",
        "origin",
        "owner",
        "comment",
        "options",
        "retention_time",
        "kind",
        "budget",
        "owner_role_type",
        "object_visibility",
    ]


@pytest.mark.xfail(
    reason="only partial supports exists to support sqlalchemy, see test_reflect",
)
def test_show_keys(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE TABLE test_table (id INT PRIMARY KEY, name TEXT UNIQUE)")
    dcur.execute("CREATE TABLE test_table2 (id INT, other_id INT, FOREIGN KEY (other_id) REFERENCES test_table(id))")

    dcur.execute("SHOW PRIMARY KEYS")
    primary_keys = dcur.fetchall()
    assert primary_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "TEST_TABLE",
            "column_name": "ID",
            "key_sequence": 1,
            "constraint_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_ID_pk",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW UNIQUE KEYS")
    unique_keys = dcur.fetchall()
    assert unique_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "TEST_TABLE",
            "column_name": "NAME",
            "key_sequence": 1,
            "constraint_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_NAME_uk",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW IMPORTED KEYS")
    foreign_keys = dcur.fetchall()
    assert foreign_keys == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "pk_database_name": "DB1",
            "pk_schema_name": "SCHEMA1",
            "pk_table_name": "TEST_TABLE",
            "pk_column_name": "ID",
            "fk_database_name": "DB1",
            "fk_schema_name": "SCHEMA1",
            "fk_table_name": "TEST_TABLE2",
            "fk_column_name": "OTHER_ID",
            "key_sequence": 1,
            "update_rule": "NO ACTION",
            "delete_rule": "NO ACTION",
            "fk_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE2_OTHER_ID_fk",
            "pk_name": "SYS_CONSTRAINT_DB1_SCHEMA1_TEST_TABLE_ID_pk",
            "deferrability": "NOT DEFERRABLE",
            "rely": "false",
            "comment": None,
        }
    ]

    dcur.execute("SHOW PRIMARY KEYS IN SCHEMA")
    assert dcur.fetchall() == primary_keys

    dcur.execute("SHOW PRIMARY KEYS IN DATABASE")
    assert dcur.fetchall() == primary_keys


def test_show_primary_keys(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE TABLE example (id int, name varchar, PRIMARY KEY (id, name))")

    dcur.execute("show primary keys")
    result = dcur.fetchall()

    assert result == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "EXAMPLE",
            "column_name": "ID",
            "key_sequence": 1,
            "constraint_name": "db1_schema1_example_pkey",
            "rely": "false",
            "comment": None,
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
            "table_name": "EXAMPLE",
            "column_name": "NAME",
            "key_sequence": 1,
            "constraint_name": "db1_schema1_example_pkey",
            "rely": "false",
            "comment": None,
        },
    ]

    dcur.execute("show primary keys in schema db1.schema1")
    result2 = dcur.fetchall()
    assert result == result2

    # Assertion to sanity check that the above "in schema" filter isn't wrong, and in fact filters
    dcur.execute("show primary keys in schema db1.information_schema")
    result3 = dcur.fetchall()
    assert result3 == []


def test_show_primary_keys_from_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute(
        """
        CREATE TABLE test_table (
            ID varchar,
            VERSION varchar,
            PRIMARY KEY (ID, VERSION)
        )
        """
    )

    cur.execute("SHOW PRIMARY KEYS IN test_table")
    pk_result = cur.fetchall()

    pk_columns = [result[4] for result in pk_result]
    assert pk_columns == ["ID", "VERSION"]


def test_show_objects(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create schema schema2")
    dcur.execute("create view schema2.view1 as select * from schema1.example")

    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "kind": "TABLE",
            "name": "EXAMPLE",
            "schema_name": "SCHEMA1",
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "kind": "VIEW",
            "name": "VIEW1",
            "schema_name": "SCHEMA2",
        },
    ]

    dcur.execute("show terse objects in db1.schema1")
    assert dcur.fetchall() == [objects[0]]

    dcur.execute("show terse objects in database")
    rows: list[dict] = cast(list[dict], dcur.fetchall())
    assert [r for r in rows if r["schema_name"] != "information_schema"] == objects

    assert [r.name for r in dcur.description] == ["created_on", "name", "kind", "database_name", "schema_name"]

    dcur.execute("show objects").fetchall()
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "comment",
        # TODO: include these columns
        # "cluster_by",
        # "rows",
        # "bytes",
        # "owner",
        # "retention_time",
        # "owner_role_type",
        # "budget"
    ]


def test_show_schemas(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show terse schemas in database db1 limit 100")
    assert dcur.fetchall() == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "kind": None,
            "name": "SCHEMA1",
            "schema_name": None,
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "kind": None,
            "name": "information_schema",
            "schema_name": None,
        },
    ]
    assert [r.name for r in dcur.description] == ["created_on", "name", "kind", "database_name", "schema_name"]


def test_show_tables(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create view view1 as select * from example")
    dcur.execute("show terse tables")
    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "database_name": "DB1",
            "kind": "TABLE",
            "name": "EXAMPLE",
            "schema_name": "SCHEMA1",
        },
    ]
    # assert dcur.fetchall() == objects
    dcur.execute("show terse tables in db1.schema1")
    assert dcur.fetchall() == objects
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]

    dcur.execute("show tables in db1.schema1")
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "comment",
        # TODO: include these columns
        # "cluster_by",
        # "rows",
        # "bytes",
        # "owner",
        # "retention_time",
        # "automatic_clustering",
        # "change_tracking",
        # "search_optimization",
        # "search_optimization_progress",
        # "search_optimization_bytes",
        # "is_external",
        # "enable_schema_evolution",
        # "owner_role_type",
        # "is_event",
        # "budget",
        # "is_hybrid",
        # "is_iceberg",
    ]


def test_show_functions(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show functions")
    result = dcur.fetchall()

    assert len(result) == 1

    # Check for expected column names in description
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "schema_name",
        "is_builtin",
        "is_aggregate",
        "is_ansi",
        "min_num_arguments",
        "max_num_arguments",
        "arguments",
        "description",
        "catalog_name",
        "is_table_function",
        "valid_for_clustering",
        "is_secure",
        "secrets",
        "external_access_integrations",
        "is_external_function",
        "language",
        "is_memoizable",
        "is_data_metric",
    ]

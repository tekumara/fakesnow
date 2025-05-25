# ruff: noqa: E501
# pyright: reportOptionalMemberAccess=false

import datetime
from typing import cast

import pytest
import pytz
import snowflake.connector.cursor


def test_show_columns(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("""create or replace table example (
            XBOOLEAN BOOLEAN, XINT INT, XFLOAT FLOAT, XDECIMAL DECIMAL(10,2),
            XVARCHAR VARCHAR, XVARCHAR20 VARCHAR(20),
            XDATE DATE, XTIME TIME, XTIMESTAMP TIMESTAMP_TZ, XTIMESTAMP_NTZ TIMESTAMP_NTZ,
            XBINARY BINARY, /* XARRAY ARRAY, XOBJECT OBJECT, */ XVARIANT VARIANT)
        """)
    dcur.execute("create view view1 as select xboolean from example")

    common_fields = {
        "table_name": "EXAMPLE",
        "schema_name": "SCHEMA1",
        "null?": "true",
        "default": "",
        "kind": "COLUMN",
        "expression": "",
        "comment": "",
        "database_name": "DB1",
        "autoincrement": "",
        "schema_evolution_record": None,
    }

    # fmt: off
    example1_cols = [
        {**common_fields, "column_name": "XBOOLEAN", "data_type": '{"type":"BOOLEAN","nullable":true}'},
        {**common_fields, "column_name": "XINT", "data_type": '{"type":"FIXED","precision":38,"scale":0,"nullable":true}'},
        {**common_fields, "column_name": "XFLOAT", "data_type": '{"type":"REAL","nullable":true}'},
        {**common_fields, "column_name": "XDECIMAL", "data_type": '{"type":"FIXED","precision":10,"scale":2,"nullable":true}'},
        {**common_fields, "column_name": "XVARCHAR", "data_type": '{"type":"TEXT","length":16777216,"byteLength":16777216,"nullable":true,"fixed":false}'},
        {**common_fields, "column_name": "XVARCHAR20", "data_type": '{"type":"TEXT","length":20,"byteLength":80,"nullable":true,"fixed":false}'},
        {**common_fields, "column_name": "XDATE", "data_type": '{"type":"DATE","nullable":true}'},
        {**common_fields, "column_name": "XTIME", "data_type": '{"type":"TIME","precision":0,"scale":9,"nullable":true}'},
        {**common_fields, "column_name": "XTIMESTAMP", "data_type": '{"type":"TIMESTAMP_TZ","precision":0,"scale":9,"nullable":true}'},
        {**common_fields, "column_name": "XTIMESTAMP_NTZ", "data_type": '{"type":"TIMESTAMP_NTZ","precision":0,"scale":9,"nullable":true}'},
        {**common_fields, "column_name": "XBINARY", "data_type": '{"type":"BINARY","length":8388608,"byteLength":8388608,"nullable":true,"fixed":true}'},
        {**common_fields, "column_name": "XVARIANT", "data_type": '{"type":"VARIANT","nullable":true}'},
    ]
    # fmt: on

    view1_cols = [
        {
            "table_name": "VIEW1",
            "schema_name": "SCHEMA1",
            "column_name": "XBOOLEAN",
            "data_type": '{"type":"BOOLEAN","nullable":true}',
            "null?": "true",
            "default": "",
            "kind": "COLUMN",
            "expression": "",
            "comment": "",
            "database_name": "DB1",
            "autoincrement": "",
            "schema_evolution_record": None,
        }
    ]

    dcur.execute("SHOW COLUMNS IN example")
    assert dcur.fetchall() == example1_cols

    dcur.execute("SHOW COLUMNS IN SCHEMA db1.schema1")
    assert dcur.fetchall() == example1_cols + view1_cols

    dcur.execute("SHOW COLUMNS IN ACCOUNT")
    assert dcur.fetchall() == example1_cols + view1_cols

    assert [r.name for r in dcur.description] == [
        "table_name",
        "schema_name",
        "column_name",
        "data_type",
        "null?",
        "default",
        "kind",
        "expression",
        "comment",
        "database_name",
        "autoincrement",
        "schema_evolution_record",
    ]


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


def test_show_excludes_fs(_fakesnow: None):
    # when connecting without a database or schema there should be no objects
    with snowflake.connector.connect() as conn, conn.cursor() as cur:
        cur.execute("show objects in account")
        assert cur.fetchall() == []

        cur.execute("show schemas in account")
        assert cur.fetchall() == []


def test_show_objects(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create schema schema2")
    dcur.execute("create view schema2.view1 as select * from schema1.example")

    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "EXAMPLE",
            "kind": "TABLE",
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "VIEW1",
            "kind": "VIEW",
            "database_name": "DB1",
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
        "cluster_by",
        "rows",
        "bytes",
        "owner",
        "retention_time",
        "owner_role_type",
        "budget",
        "is_hybrid",
        "is_dynamic",
    ]


def test_show_schemas(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create database db2")
    dcur.execute("create schema db2.schema2")
    dcur.execute("show terse schemas in database db1 limit 100")
    assert dcur.fetchall() == [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "SCHEMA1",
            "kind": None,
            "database_name": "DB1",
            "schema_name": None,
        },
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "information_schema",
            "kind": None,
            "database_name": "DB1",
            "schema_name": None,
        },
    ]
    assert [r.name for r in dcur.description] == ["created_on", "name", "kind", "database_name", "schema_name"]


def test_show_tables(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table table1(x int)")
    dcur.execute("create view view1 as select * from table1")
    dcur.execute("create database db2")
    dcur.execute("create schema db2.schema2")
    dcur.execute("create table db2.schema2.table2(x int)")
    dcur.execute("create schema schema3")
    dcur.execute("create table schema3.table3(x int)")

    table1 = {
        "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
        "name": "TABLE1",
        "kind": "TABLE",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
    }
    table2 = {
        "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
        "name": "TABLE2",
        "kind": "TABLE",
        "database_name": "DB2",
        "schema_name": "SCHEMA2",
    }
    table3 = {
        "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
        "name": "TABLE3",
        "kind": "TABLE",
        "database_name": "DB1",
        "schema_name": "SCHEMA3",
    }
    foo = {
        "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
        "name": "FOO",
        "kind": "TABLE",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
    }

    # show in current db/schema
    dcur.execute("show terse tables")
    assert dcur.fetchall() == [table1]

    # in qualified schema
    dcur.execute("show terse tables in db1.schema1")
    assert dcur.fetchall() == [table1]
    dcur.execute("show terse tables in schema db1.schema1")
    assert dcur.fetchall() == [table1]

    # in qualified database
    dcur.execute("show terse tables in database db2")
    assert dcur.fetchall() == [table2]

    # using like
    dcur.execute("create table foo(x int)")
    dcur.execute("show terse tables like 'table%'")
    # should not match show foo
    assert dcur.fetchall() == [table1]

    # in account - ordered by database, schema, name
    dcur.execute("show terse tables in account")
    assert dcur.fetchall() == [
        foo,  # db1
        table1,  # db1
        table3,  # db1
        table2,  # db2
    ]

    # non-terse has all the columns
    dcur.execute("show tables in db1.schema1")
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
        "comment",
        "cluster_by",
        "rows",
        "bytes",
        "owner",
        "retention_time",
        "automatic_clustering",
        "change_tracking",
        "search_optimization",
        "search_optimization_progress",
        "search_optimization_bytes",
        "is_external",
        "enable_schema_evolution",
        "owner_role_type",
        "is_event",
        "budget",
        "is_hybrid",
        "is_iceberg",
        "is_dynamic",
        "is_immutable",
    ]


def test_show_functions(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show functions")
    dcur.fetchall()

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


def test_show_procedures(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show procedures")
    dcur.fetchall()

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
    ]


def test_show_views(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("create table example(x int)")
    dcur.execute("create view view1 as select * from example")
    dcur.execute("create schema schema2")
    dcur.execute("show terse views")
    objects = [
        {
            "created_on": datetime.datetime(1970, 1, 1, 0, 0, tzinfo=pytz.utc),
            "name": "VIEW1",
            "kind": "VIEW",
            "database_name": "DB1",
            "schema_name": "SCHEMA1",
        },
    ]
    assert dcur.fetchall() == objects
    dcur.execute("show terse views in db1.schema1")
    assert dcur.fetchall() == objects
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "kind",
        "database_name",
        "schema_name",
    ]

    dcur.execute("show terse views in account")
    assert dcur.fetchall() == objects

    dcur.execute("show terse views in schema schema2")
    assert dcur.fetchall() == []

    dcur.execute("show views in db1.schema1")
    assert [r.name for r in dcur.description] == [
        "created_on",
        "name",
        "reserved",
        "database_name",
        "schema_name",
        "owner",
        "comment",
        "text",
        "is_secure",
        "is_materialized",
        "owner_role_type",
        "change_tracking",
    ]


def test_show_warehouses(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("show warehouses")
    dcur.fetchall()

    # Check for expected column names in description
    assert [r.name for r in dcur.description] == [
        "name",
        "state",
        "type",
        "size",
        "min_cluster_count",
        "max_cluster_count",
        "started_clusters",
        "running",
        "queued",
        "is_default",
        "is_current",
        "auto_suspend",
        "auto_resume",
        "available",
        "provisioning",
        "quiescing",
        "other",
        "created_on",
        "resumed_on",
        "updated_on",
        "owner",
        "comment",
        "enable_query_acceleration",
        "query_acceleration_max_scale_factor",
        "resource_monitor",
        "actives",
        "pendings",
        "failed",
        "suspended",
        "uuid",
        "scaling_policy",
        "budget",
        "owner_role_type",
        "resource_constraint",
    ]

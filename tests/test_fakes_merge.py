from __future__ import annotations

import snowflake.connector
import pytest

# TODO: Also consider nondeterministic config for throwing errors when multiple source criteria match a target row
# https://docs.snowflake.com/en/sql-reference/sql/merge#nondeterministic-results-for-update-and-delete
def test_merge(conn: snowflake.connector.SnowflakeConnection):
    *_, dcur = conn.execute_string(
        """
        CREATE OR REPLACE TABLE t1 (
            t1Key INT PRIMARY KEY,
            val VARCHAR(50),
            status VARCHAR(20)
        );

        CREATE OR REPLACE TABLE t2 (
            t2Key INT PRIMARY KEY,
            newVal VARCHAR(50),
            newStatus VARCHAR(20),
            isNewStatus INT,
            marked INT
        );

        INSERT INTO t1 (t1Key, val, status) VALUES
        (1, 'Old Value 1', 'Old Status 1'),
        (2, 'Old Value 2', 'Old Status 2'),
        (3, 'Old Value 3', 'Old Status 3'),
        (4, 'Old Value 4', 'Old Status 4');

        INSERT INTO t2 (t2Key, newVal, newStatus, isNewStatus, marked) VALUES
        (1, 'New Value 1', 'New Status 1', 1, 0),  -- Case: WHEN MATCHED AND t2.isNewStatus = 1 THEN UPDATE
        (2, 'New Value 2', 'New Status 2', 0, 1),  -- Case: WHEN MATCHED AND t2.marked = 1 THEN DELETE
        (3, 'New Value 3', 'New Status 3', 0, 0),  -- Case: WHEN MATCHED THEN UPDATE
        (5, 'New Value 5', 'New Status 5', 0, 0);  -- Case: WHEN NOT MATCHED THEN INSERT

        MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
            WHEN MATCHED AND t2.marked = 1 THEN DELETE
            WHEN MATCHED AND t2.isNewStatus = 1 THEN UPDATE SET val = t2.newVal, status = t2.newStatus
            WHEN MATCHED THEN UPDATE SET val = t2.newVal
            WHEN NOT MATCHED THEN INSERT (t1Key, val, status) VALUES (t2.t2Key, t2.newVal, t2.newStatus);
        """,
        cursor_class=snowflake.connector.cursor.DictCursor,  # type: ignore see https://github.com/snowflakedb/snowflake-connector-python/issues/1984
    )

    assert dcur.fetchall() == [{"number of rows inserted": 1, "number of rows updated": 2, "number of rows deleted": 1}]

    dcur.execute("select * from t1 order by t1key")
    res = dcur.fetchall()
    assert res == [
        {"T1KEY": 1, "VAL": "New Value 1", "STATUS": "New Status 1"},
        {"T1KEY": 3, "VAL": "New Value 3", "STATUS": "Old Status 3"},
        {"T1KEY": 4, "VAL": "Old Value 4", "STATUS": "Old Status 4"},
        {"T1KEY": 5, "VAL": "New Value 5", "STATUS": "New Status 5"},
    ]


def test_merge_not_matched_condition(conn: snowflake.connector.SnowflakeConnection):
    *_, dcur = conn.execute_string(
        """
        CREATE OR REPLACE TABLE t1 (
            t1Key INT PRIMARY KEY,
            val VARCHAR(50),
            status VARCHAR(20)
        );

        CREATE OR REPLACE TABLE t2 (
            t2Key INT PRIMARY KEY,
            newVal VARCHAR(50),
            newStatus VARCHAR(20),
            insertable INT
        );

        INSERT INTO t1 (t1Key, val, status) VALUES
        (1, 'Old Value 1', 'Old Status 1');

        INSERT INTO t2 (t2Key, newVal, newStatus, insertable) VALUES
        (1, 'New Value 1', 'New Status 1', 0),  -- Case: nothing matches
        (2, 'New Value 2', 'New Status 2', 1),  -- Case: WHEN NOT MATCHED AND t2.insertable = 1 THEN INSERT
        (3, 'New Value 3', 'New Status 3', 0);  -- Case: nothing matches

        MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
            WHEN NOT MATCHED AND t2.insertable = 1 THEN INSERT (t1Key, val, status) VALUES (t2.t2Key, t2.newVal, t2.newStatus);
        """,
        cursor_class=snowflake.connector.cursor.DictCursor,  # type: ignore see https://github.com/snowflakedb/snowflake-connector-python/issues/1984
    )

    # TODO: Check what the python connector actually returns, does include the updates and deletes? Ours currently returns None values for those columns.
    # assert dcur.fetchall() == [{"number of rows inserted": 1}]

    dcur.execute("select * from t1 order by t1key")
    res = dcur.fetchall()
    assert res == [
        {"T1KEY": 1, "VAL": "Old Value 1", "STATUS": "Old Status 1"},
        {"T1KEY": 2, "VAL": "New Value 2", "STATUS": "New Status 2"},
    ]
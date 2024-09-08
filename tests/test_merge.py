# ruff: noqa: E501
from __future__ import annotations

import snowflake.connector
import sqlglot

from fakesnow import transforms


def test_merge_transform() -> None:
    assert [
        e.sql(dialect="duckdb")
        for e in transforms.merge(
            sqlglot.parse_one(
                """
                MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
                    WHEN MATCHED AND t2.marked = 1 THEN DELETE
                    WHEN MATCHED AND t2.isNewStatus = 1 THEN UPDATE SET val = t2.newVal, status = t2.newStatus
                    WHEN MATCHED THEN UPDATE SET val = t2.newVal
                    WHEN NOT MATCHED THEN INSERT (t1Key, val, status) VALUES (t2.t2Key, t2.newVal, t2.newStatus);
                """
            )
        )
    ] == [
        "BEGIN",
        "CREATE OR REPLACE TEMPORARY TABLE temp_merge_updates_deletes (target_rowid INT, when_id INT, type TEXT(1))",
        "CREATE OR REPLACE TEMPORARY TABLE temp_merge_inserts (source_rowid INT, when_id INT)",
        "INSERT INTO temp_merge_updates_deletes SELECT rowid, 0, 'D' FROM t1 WHERE EXISTS(SELECT 1 FROM t2 WHERE t1.t1Key = t2.t2Key AND t2.marked = 1) AND NOT EXISTS(SELECT 1 FROM temp_merge_updates_deletes WHERE t1.rowid = target_rowid)",
        "INSERT INTO temp_merge_updates_deletes SELECT rowid, 1, 'U' FROM t1 WHERE EXISTS(SELECT 1 FROM t2 WHERE t1.t1Key = t2.t2Key AND t2.isNewStatus = 1) AND NOT EXISTS(SELECT 1 FROM temp_merge_updates_deletes WHERE t1.rowid = target_rowid)",
        "INSERT INTO temp_merge_updates_deletes SELECT rowid, 2, 'U' FROM t1 WHERE EXISTS(SELECT 1 FROM t2 WHERE t1.t1Key = t2.t2Key) AND NOT EXISTS(SELECT 1 FROM temp_merge_updates_deletes WHERE t1.rowid = target_rowid)",
        "INSERT INTO temp_merge_inserts SELECT rowid, 3 FROM t2 WHERE NOT EXISTS(SELECT 1 FROM t1 WHERE t1.t1Key = t2.t2Key) AND NOT EXISTS(SELECT 1 FROM temp_merge_inserts WHERE t2.rowid = source_rowid)",
        "DELETE FROM t1 WHERE t1.rowid IN (SELECT target_rowid FROM temp_merge_updates_deletes WHERE when_id = 0 AND target_rowid = t1.rowid)",
        "UPDATE t1 SET val = t2.newVal, status = t2.newStatus FROM t2 WHERE t1.t1Key = t2.t2Key AND t2.isNewStatus = 1 AND t1.rowid IN (SELECT target_rowid FROM temp_merge_updates_deletes WHERE when_id = 1 AND target_rowid = t1.rowid)",
        "UPDATE t1 SET val = t2.newVal FROM t2 WHERE t1.t1Key = t2.t2Key AND t1.rowid IN (SELECT target_rowid FROM temp_merge_updates_deletes WHERE when_id = 2 AND target_rowid = t1.rowid)",
        "INSERT INTO t1 (t1Key, val, status) SELECT t2.t2Key, t2.newVal, t2.newStatus FROM t2 WHERE t2.rowid IN (SELECT source_rowid FROM temp_merge_inserts WHERE when_id = 3 AND source_rowid = t2.rowid)",
        "COMMIT",
        'WITH merge_update_deletes AS (SELECT CAST(COUNT_IF(type = \'U\') AS INT) AS "updates", CAST(COUNT_IF(type = \'D\') AS INT) AS "deletes" FROM temp_merge_updates_deletes), merge_inserts AS (SELECT COUNT() AS "inserts" FROM temp_merge_inserts) SELECT mi.inserts AS "number of rows inserted", mud.updates AS "number of rows updated", mud.deletes AS "number of rows deleted" FROM merge_update_deletes AS mud, merge_inserts AS mi',
    ]


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

    # TODO: Check what the python connector actually returns, does include the updates and deletes?
    # Ours currently returns None values for those columns.
    # assert dcur.fetchall() == [{"number of rows inserted": 1}]

    dcur.execute("select * from t1 order by t1key")
    res = dcur.fetchall()
    assert res == [
        {"T1KEY": 1, "VAL": "Old Value 1", "STATUS": "Old Status 1"},
        {"T1KEY": 2, "VAL": "New Value 2", "STATUS": "New Status 2"},
    ]

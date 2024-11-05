# ruff: noqa: E501
from __future__ import annotations

import datetime

import snowflake.connector
import sqlglot

from fakesnow import transforms
from tests.utils import strip


def test_transform_merge() -> None:
    assert [
        e.sql(dialect="duckdb")
        for e in transforms.merge(
            sqlglot.parse_one(
                """
                MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
                    WHEN MATCHED AND t2.marked = 1 THEN DELETE
                    WHEN MATCHED AND t2.isNewStatus = 1 THEN UPDATE SET t1.val = t2.newVal, status = 'new'
                    WHEN MATCHED THEN UPDATE SET val = t2.newVal
                    WHEN NOT MATCHED THEN INSERT (t1Key, val, status) VALUES (t2.t2Key, 'newVal', t2.newStatus);
                """
            )
        )
    ] == [
        strip("""
            CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
            SELECT t2.newStatus, t2.newVal, t2.t2Key,
                CASE
                    WHEN t1.t1Key = t2.t2Key AND t2.marked = 1 THEN 0
                    WHEN t1.t1Key = t2.t2Key AND t2.isNewStatus = 1 THEN 1
                    WHEN t1.t1Key = t2.t2Key THEN 2
                    WHEN t1.rowid IS NULL THEN 3
                    ELSE NULL
                END AS MERGE_OP
                FROM t1
            FULL OUTER JOIN t2 ON t1.t1Key = t2.t2Key
            WHERE NOT MERGE_OP IS NULL"""),
        strip("""
            DELETE FROM t1
            USING merge_candidates AS t2
            WHERE t1.t1Key = t2.t2Key
            AND t2.merge_op = 0"""),
        strip("""
            UPDATE t1
            SET val = t2.newVal, status = 'new'
            FROM merge_candidates AS t2
            WHERE t1.t1Key = t2.t2Key
            AND t2.merge_op = 1"""),
        strip("""
            UPDATE t1
            SET val = t2.newVal
            FROM merge_candidates AS t2
            WHERE t1.t1Key = t2.t2Key
            AND t2.merge_op = 2"""),
        strip("""
            INSERT INTO t1 (t1Key, val, status)
            SELECT t2.t2Key, 'newVal', t2.newStatus
            FROM merge_candidates AS t2
            WHERE t2.merge_op = 3"""),
        strip("""
            SELECT
              COUNT_IF(merge_op IN (3)) AS "number of rows inserted",
              COUNT_IF(merge_op IN (1, 2)) AS "number of rows updated",
              COUNT_IF(merge_op IN (0)) AS "number of rows deleted"
            FROM merge_candidates"""),
    ]


def test_transform_merge_not_matched_condition() -> None:
    assert [
        e.sql(dialect="duckdb")
        for e in transforms.merge(
            sqlglot.parse_one(
                """
                MERGE INTO t1 USING t2 ON t1.t1Key = t2.t2Key
                    WHEN NOT MATCHED AND t2.insertable = 1 THEN INSERT (t1Key, val, status) VALUES (t2.t2Key, t2.newVal, t2.newStatus);
                """
            )
        )
    ] == [
        strip("""
            CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
            SELECT t2.newStatus, t2.newVal, t2.t2Key,
                CASE
                    WHEN t1.rowid IS NULL AND t2.insertable = 1 THEN 0
                    ELSE NULL
                END AS MERGE_OP
                FROM t1
            FULL OUTER JOIN t2 ON t1.t1Key = t2.t2Key
            WHERE NOT MERGE_OP IS NULL"""),
        strip("""
            INSERT INTO t1 (t1Key, val, status)
            SELECT t2.t2Key, t2.newVal, t2.newStatus
            FROM merge_candidates AS t2
            WHERE t2.merge_op = 0"""),
        strip("""
            SELECT
              COUNT_IF(merge_op IN (0)) AS "number of rows inserted"
            FROM merge_candidates"""),
    ]


def test_transform_merge_complex_join_keys() -> None:
    assert [
        e.sql(dialect="duckdb")
        for e in transforms.merge(
            sqlglot.parse_one(
                # two join keys
                # same join key names in target and source
                # no insert values columns
                """
                MERGE INTO t1 USING t2 ON t1.id = t2.id AND t1.name = t2.name
                    WHEN MATCHED AND t1.status = 'old' THEN DELETE
                    WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.name, 'new');
                """
            )
        )
    ] == [
        strip("""
            CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
            SELECT t2.id, t2.name,
                CASE
                    WHEN t1.id = t2.id AND t1.name = t2.name AND t1.status = 'old' THEN 0
                    WHEN t1.rowid IS NULL THEN 1
                    ELSE NULL
                END AS MERGE_OP
                FROM t1
            FULL OUTER JOIN t2 ON t1.id = t2.id AND t1.name = t2.name
            WHERE NOT MERGE_OP IS NULL
               """),
        strip("""
            DELETE FROM t1
            USING merge_candidates AS t2
            WHERE t1.id = t2.id AND t1.name = t2.name
            AND t2.merge_op = 0"""),
        strip("""
            INSERT INTO t1
            SELECT t2.id, t2.name, 'new'
            FROM merge_candidates AS t2
            WHERE t2.merge_op = 1"""),
        strip("""
            SELECT
              COUNT_IF(merge_op IN (1)) AS "number of rows inserted",
              COUNT_IF(merge_op IN (0)) AS "number of rows deleted"
            FROM merge_candidates"""),
    ]


def test_transform_merge_source_subquery() -> None:
    assert [
        e.sql(dialect="duckdb")
        for e in transforms.merge(
            sqlglot.parse_one(
                # source is a subquery
                # join key is not used in set clause
                """
                MERGE INTO LINE tgt USING (
                    SELECT BATCH_NUMBER, ID, ACTIVE_STATUS FROM HEADER WHERE ACTIVE_STATUS = 1
                ) src
                ON tgt.BATCH_NUMBER = src.BATCH_NUMBER AND tgt.ID = src.ID
                WHEN MATCHED THEN UPDATE
                    SET tgt.ACTIVE_STATUS = src.ACTIVE_STATUS, tgt.END_DATE = NULL
                """
            )
        )
    ] == [
        strip("""
            CREATE OR REPLACE TEMPORARY TABLE merge_candidates AS
            SELECT src.ACTIVE_STATUS, src.BATCH_NUMBER, src.ID,
                CASE
                    WHEN tgt.BATCH_NUMBER = src.BATCH_NUMBER AND tgt.ID = src.ID THEN 0
                    ELSE NULL
                END AS MERGE_OP
                FROM LINE AS tgt
            FULL OUTER JOIN (SELECT BATCH_NUMBER, ID, ACTIVE_STATUS FROM HEADER WHERE ACTIVE_STATUS = 1)
              AS src ON tgt.BATCH_NUMBER = src.BATCH_NUMBER AND tgt.ID = src.ID
            WHERE NOT MERGE_OP IS NULL
               """),
        strip("""
            UPDATE LINE AS tgt
            SET ACTIVE_STATUS = src.ACTIVE_STATUS, END_DATE = NULL
            FROM merge_candidates AS src
            WHERE tgt.BATCH_NUMBER = src.BATCH_NUMBER AND tgt.ID = src.ID
            AND src.merge_op = 0"""),
        strip("""
            SELECT
              COUNT_IF(merge_op IN (0)) AS "number of rows updated"
            FROM merge_candidates"""),
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


def test_merge_complex_join_keys(conn: snowflake.connector.SnowflakeConnection):
    *_, dcur = conn.execute_string(
        """
        CREATE OR REPLACE TABLE t1 (
            id INT,
            name VARCHAR(50),
            status VARCHAR(20)
        );

        CREATE OR REPLACE TABLE t2 (
            id INT,
            name VARCHAR(50),
            status VARCHAR(20)
        );

        INSERT INTO t1 VALUES
        (1, 'Apple', 'old'),
        (1, 'Banana', 'keep');

        INSERT INTO t2 VALUES
        (1, 'Apple', 'older'),
        (2, 'Kiwi', 'insert me');

        MERGE INTO t1 USING t2 ON t1.id = t2.id AND t1.name = t2.name
            WHEN MATCHED AND t1.status = 'old' THEN DELETE
            WHEN NOT MATCHED THEN INSERT VALUES (t2.id, t2.name, 'new');
        """,
        cursor_class=snowflake.connector.cursor.DictCursor,  # type: ignore see https://github.com/snowflakedb/snowflake-connector-python/issues/1984
    )

    assert dcur.fetchall() == [{"number of rows inserted": 1, "number of rows deleted": 1}]

    dcur.execute("select * from t1 order by id")
    res = dcur.fetchall()
    assert res == [{"ID": 1, "NAME": "Banana", "STATUS": "keep"}, {"ID": 2, "NAME": "Kiwi", "STATUS": "new"}]


def test_merge_with_source_subquery(conn: snowflake.connector.SnowflakeConnection):
    *_, dcur = conn.execute_string(
        """
        CREATE OR REPLACE TABLE LINE (
            ID INT PRIMARY KEY,
            BATCH_NUMBER INT,
            ACTIVE_STATUS INT,
            END_DATE DATE
        );

        CREATE OR REPLACE TABLE HEADER (
            ID INT PRIMARY KEY,
            BATCH_NUMBER INT,
            ACTIVE_STATUS INT
        );

        INSERT INTO LINE (ID, BATCH_NUMBER, ACTIVE_STATUS, END_DATE) VALUES
        (1, 1, 1, DATE '2001-01-01'),
        (2, 2, 2, DATE '2002-02-02');

        INSERT INTO HEADER (ID, BATCH_NUMBER, ACTIVE_STATUS) VALUES
        (2, 2, 1),  -- Case: match
        (3, 3, NULL);  -- Case: no match

        MERGE INTO LINE tgt USING (
            SELECT BATCH_NUMBER, ID, ACTIVE_STATUS FROM HEADER WHERE ACTIVE_STATUS = 1
        ) src
        ON tgt.BATCH_NUMBER = src.BATCH_NUMBER AND tgt.ID = src.ID
        AND tgt.ID = src.ID
        WHEN MATCHED THEN UPDATE
            SET tgt.ACTIVE_STATUS = src.ACTIVE_STATUS, tgt.END_DATE = NULL;
        """,
        cursor_class=snowflake.connector.cursor.DictCursor,  # type: ignore see https://github.com/snowflakedb/snowflake-connector-python/issues/1984
    )

    # TODO: Check what the python connector actually returns, does include the updates and deletes?
    # Ours currently returns None values for those columns.
    # assert dcur.fetchall() == [{"number of rows inserted": 1}]

    dcur.execute("select * from line order by id")
    res = dcur.fetchall()
    assert res == [
        {"ID": 1, "BATCH_NUMBER": 1, "ACTIVE_STATUS": 1, "END_DATE": datetime.date(2001, 1, 1)},
        {"ID": 2, "BATCH_NUMBER": 2, "ACTIVE_STATUS": 1, "END_DATE": None},
    ]

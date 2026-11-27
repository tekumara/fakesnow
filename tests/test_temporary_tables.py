from __future__ import annotations

from datetime import UTC, datetime

import pytest
import snowflake.connector
import snowflake.connector.cursor
import snowflake.connector.errors


def test_qualified_temporary_create_and_select(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE db1.other_schema.t1 (id INT, label VARCHAR)")
    assert cur.fetchall() == [("Table T1 successfully created.",)]

    cur.execute("INSERT INTO db1.other_schema.t1 VALUES (1, 'a'), (2, 'b')")
    cur.execute("SELECT t1.id, other_schema.t1.label FROM db1.other_schema.t1 ORDER BY t1.id")
    assert cur.fetchall() == [(1, "a"), (2, "b")]


def test_qualified_temporary_create_in_missing_schema_errors(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("CREATE TEMP TABLE db1.missing_schema.t1 (id INT)")
    assert excinfo.value.errno == 2003


def test_qualified_temporary_dml_and_truncate(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE other_schema.target (id INT, val VARCHAR)")
    cur.execute("CREATE TEMP TABLE other_schema.src (id INT, val VARCHAR)")
    cur.execute("INSERT INTO other_schema.target VALUES (1, 'old'), (2, 'drop')")
    cur.execute("INSERT INTO other_schema.src VALUES (1, 'new'), (2, 'drop')")

    cur.execute(
        "UPDATE other_schema.target SET val = src.val "
        "FROM other_schema.src WHERE other_schema.target.id = other_schema.src.id AND target.id = 1"
    )
    cur.execute("DELETE FROM other_schema.target USING other_schema.src WHERE target.id = src.id AND src.id = 2")
    cur.execute("SELECT id, val FROM other_schema.target")
    assert cur.fetchall() == [(1, "new")]

    cur.execute("TRUNCATE TABLE other_schema.target")
    cur.execute("SELECT * FROM other_schema.target")
    assert cur.fetchall() == []


def test_merge_with_qualified_temporary_tables(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE other_schema.target (id INT, val VARCHAR)")
    cur.execute("CREATE TEMP TABLE other_schema.source (id INT, val VARCHAR)")
    cur.execute("INSERT INTO other_schema.target VALUES (1, 'a')")
    cur.execute("INSERT INTO other_schema.source VALUES (1, 'b'), (2, 'c')")

    cur.execute(
        """
        MERGE INTO other_schema.target AS t
        USING other_schema.source AS s
        ON t.id = s.id
        WHEN MATCHED THEN UPDATE SET val = s.val
        WHEN NOT MATCHED THEN INSERT (id, val) VALUES (s.id, s.val)
        """
    )
    cur.execute("SELECT id, val FROM other_schema.target ORDER BY id")
    assert cur.fetchall() == [(1, "b"), (2, "c")]

    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'MERGE_CANDIDATES'")
    assert cur.fetchall() == []


def test_same_named_temporary_tables_in_different_schemas(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE schema1.shared (id INT, origin VARCHAR)")
    cur.execute("CREATE TEMP TABLE other_schema.shared (id INT, origin VARCHAR)")
    cur.execute("INSERT INTO schema1.shared VALUES (1, 's1')")
    cur.execute("INSERT INTO other_schema.shared VALUES (1, 's2')")

    cur.execute(
        """
        SELECT schema1.shared.origin AS o1, other_schema.shared.origin AS o2
        FROM schema1.shared
        JOIN other_schema.shared ON schema1.shared.id = other_schema.shared.id
        """
    )
    assert cur.fetchall() == [("s1", "s2")]


def test_temporary_table_shadows_permanent_and_drop_reveals_it(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE TABLE shadowed (val VARCHAR)")
    cur.execute("INSERT INTO shadowed VALUES ('perm')")
    cur.execute("CREATE TEMP TABLE shadowed (val VARCHAR)")
    cur.execute("INSERT INTO shadowed VALUES ('temp1')")

    cur.execute("SELECT val FROM db1.schema1.shadowed")
    assert cur.fetchall() == [("temp1",)]

    # Self-referential replacement reads the old temp table and swaps atomically.
    cur.execute("CREATE OR REPLACE TEMP TABLE shadowed AS SELECT val || '_v2' AS val FROM shadowed")
    cur.execute("SELECT val FROM shadowed")
    assert cur.fetchall() == [("temp1_v2",)]

    cur.execute("DROP TABLE shadowed")
    assert cur.fetchall() == [("SHADOWED successfully dropped.",)]

    cur.execute("SELECT val FROM shadowed")
    assert cur.fetchall() == [("perm",)]


def test_create_permanent_table_while_temporary_shadows_same_name(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE TEMP TABLE both_kinds (val VARCHAR)")
    cur.execute("INSERT INTO both_kinds VALUES ('temp')")

    cur.execute("CREATE TABLE both_kinds (val VARCHAR)")
    cur.execute("SELECT val FROM both_kinds")
    assert cur.fetchall() == [("temp",)]

    cur.execute("DROP TABLE both_kinds")
    cur.execute("SELECT val FROM both_kinds")
    assert cur.fetchall() == []


def test_quoted_case_sensitive_temporary_table_names(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute('CREATE TEMP TABLE "mixed_t" (v INT)')
    cur.execute("CREATE TEMP TABLE mixed_t (v INT)")
    cur.execute('INSERT INTO "mixed_t" VALUES (1)')
    cur.execute("INSERT INTO mixed_t VALUES (2)")

    cur.execute('SELECT v FROM "mixed_t"')
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT v FROM mixed_t")
    assert cur.fetchall() == [(2,)]


def test_duplicate_temporary_table_without_replace_errors(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE TEMP TABLE dup_t (id INT)")
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("CREATE TEMP TABLE dup_t (id INT)")
    assert excinfo.value.errno == 2002
    assert "DUP_T" in str(excinfo.value)


def test_create_temporary_table_if_not_exists_preserves_existing(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE TEMP TABLE keep_t (id INT) COMMENT = 'original'")
    cur.execute("INSERT INTO keep_t VALUES (42)")
    cur.execute("CREATE TEMP TABLE IF NOT EXISTS keep_t (id INT) COMMENT = 'ignored'")

    cur.execute("SELECT id FROM keep_t")
    assert cur.fetchall() == [(42,)]
    cur.execute("SELECT comment FROM information_schema.tables WHERE table_name = 'KEEP_T'")
    assert cur.fetchall() == [("original",)]


def test_unqualified_lookup_uses_current_schema_between_non_public_schemas(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE scoped_t (id INT)")
    cur.execute("INSERT INTO scoped_t VALUES (7)")

    cur.execute("USE SCHEMA other_schema")
    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        cur.execute("SELECT * FROM scoped_t")
    assert excinfo.value.errno == 2003

    cur.execute("SELECT id FROM schema1.scoped_t")
    assert cur.fetchall() == [(7,)]


def test_unqualified_lookup_falls_back_to_public_temporary_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE SCHEMA db1.public")
    cur.execute("CREATE TEMP TABLE db1.public.pub_t (id INT)")
    cur.execute("INSERT INTO db1.public.pub_t VALUES (11)")

    cur.execute("USE SCHEMA db1.schema1")
    cur.execute("SELECT id FROM pub_t")
    assert cur.fetchall() == [(11,)]


def test_current_schema_permanent_table_beats_public_temporary_table(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE SCHEMA db1.public")
    cur.execute("CREATE TEMP TABLE db1.public.precedence_t (src VARCHAR)")
    cur.execute("INSERT INTO db1.public.precedence_t VALUES ('public_temp')")

    cur.execute("USE SCHEMA db1.schema1")
    cur.execute("CREATE TABLE db1.schema1.precedence_t (src VARCHAR)")
    cur.execute("INSERT INTO db1.schema1.precedence_t VALUES ('current_perm')")

    cur.execute("SELECT src FROM precedence_t")
    assert cur.fetchall() == [("current_perm",)]


def test_temporary_table_isolated_between_sessions(_fakesnow: None) -> None:
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as conn1,
        snowflake.connector.connect(database="db1", schema="schema1") as conn2,
        conn1.cursor() as cur1,
        conn2.cursor() as cur2,
    ):
        cur1.execute("CREATE TEMP TABLE sess_t (v INT)")
        cur1.execute("INSERT INTO sess_t VALUES (1)")

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur2.execute("SELECT * FROM db1.schema1.sess_t")
        assert excinfo.value.errno == 2003

        cur2.execute("SELECT table_name FROM information_schema.tables WHERE table_name = 'SESS_T'")
        assert cur2.fetchall() == []

        cur2.execute("CREATE TEMP TABLE sess_t (v INT)")
        cur2.execute("INSERT INTO sess_t VALUES (2)")
        cur1.execute("SELECT v FROM sess_t")
        cur2.execute("SELECT v FROM sess_t")
        assert cur1.fetchall() == [(1,)]
        assert cur2.fetchall() == [(2,)]


def test_cross_session_schema_drop_and_recreate_invalidates_temp_table(_fakesnow: None) -> None:
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as conn1,
        snowflake.connector.connect(database="db1", schema="schema1") as conn2,
        conn1.cursor() as cur1,
        conn2.cursor() as cur2,
    ):
        cur1.execute("CREATE SCHEMA ephemeral")
        cur1.execute("CREATE TEMP TABLE db1.ephemeral.t1 (v INT)")
        cur1.execute("INSERT INTO db1.ephemeral.t1 VALUES (1)")

        cur2.execute("DROP SCHEMA db1.ephemeral")
        cur2.execute("CREATE SCHEMA db1.ephemeral")

        with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
            cur1.execute("SELECT * FROM db1.ephemeral.t1")
        assert excinfo.value.errno == 2003

        cur1.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'EPHEMERAL'")
        assert cur1.fetchall() == []


def test_alter_temporary_table_rename_preserves_data_and_metadata(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE SCHEMA other_schema")
    cur.execute("USE SCHEMA schema1")
    cur.execute("CREATE TEMP TABLE before_t (name VARCHAR(17)) COMMENT = 'kept'")
    cur.execute("INSERT INTO before_t VALUES ('abc')")

    cur.execute("ALTER TABLE before_t RENAME TO other_schema.after_t")
    cur.execute("SELECT name FROM other_schema.after_t")
    assert cur.fetchall() == [("abc",)]

    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("SELECT * FROM schema1.before_t")

    cur.execute(
        """
        SELECT table_schema, table_name, table_type, comment
        FROM information_schema.tables
        WHERE table_name = 'AFTER_T'
        """
    )
    assert cur.fetchall() == [("OTHER_SCHEMA", "AFTER_T", "LOCAL TEMPORARY", "kept")]

    cur.execute(
        """
        SELECT character_maximum_length, character_octet_length
        FROM information_schema.columns
        WHERE table_schema = 'OTHER_SCHEMA' AND table_name = 'AFTER_T' AND column_name = 'NAME'
        """
    )
    assert cur.fetchall() == [(17, 68)]


def test_alter_temporary_table_add_column(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE TEMP TABLE alter_t (id INT)")
    cur.execute("ALTER TABLE alter_t ADD COLUMN note VARCHAR(12)")
    cur.execute("INSERT INTO alter_t VALUES (1, 'ok')")
    cur.execute("SELECT id, note FROM alter_t")
    assert cur.fetchall() == [(1, "ok")]

    cur.execute(
        """
        SELECT column_name, data_type, character_maximum_length, character_octet_length
        FROM information_schema.columns
        WHERE table_name = 'ALTER_T'
        ORDER BY ordinal_position
        """
    )
    assert cur.fetchall() == [
        ("ID", "NUMBER", None, None),
        ("NOTE", "TEXT", 12, 48),
    ]


def test_create_temporary_table_like_and_clone(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE TEMP TABLE base_t (id INT, code VARCHAR(17))")
    cur.execute("INSERT INTO base_t VALUES (1, 'x')")

    cur.execute("CREATE TEMP TABLE like_t LIKE db1.schema1.base_t")
    cur.execute("SELECT * FROM like_t")
    assert cur.fetchall() == []
    cur.execute(
        """
        SELECT character_maximum_length, character_octet_length
        FROM information_schema.columns
        WHERE table_name = 'LIKE_T' AND column_name = 'CODE'
        """
    )
    assert cur.fetchall() == [(17, 68)]

    cur.execute("CREATE TEMP TABLE clone_t CLONE db1.schema1.base_t")
    cur.execute("SELECT id, code FROM clone_t")
    assert cur.fetchall() == [(1, "x")]
    cur.execute("SELECT table_type, is_temporary FROM information_schema.tables WHERE table_name = 'CLONE_T'")
    assert cur.fetchall() == [("LOCAL TEMPORARY", "YES")]


def test_create_temporary_table_with_foreign_key(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE TABLE parent_t (id INT PRIMARY KEY)")
    cur.execute("CREATE TEMP TABLE child_t (id INT REFERENCES parent_t(id))")
    cur.execute("INSERT INTO child_t VALUES (999)")
    cur.execute("SELECT id FROM child_t")
    assert cur.fetchall() == [(999,)]


def test_information_schema_tables_reports_local_temporary_and_comments(
    dcur: snowflake.connector.cursor.DictCursor,
) -> None:
    dcur.execute("CREATE TEMP TABLE meta_t (id INT) COMMENT = 'initial'")
    dcur.execute(
        """
        SELECT table_catalog, table_schema, table_name, table_type, is_temporary, comment, created, last_altered
        FROM information_schema.tables
        WHERE table_name = 'META_T'
        """
    )
    rows = dcur.fetchall()
    assert len(rows) == 1
    row = rows[0]
    assert row["TABLE_CATALOG"] == "DB1"
    assert row["TABLE_SCHEMA"] == "SCHEMA1"
    assert row["TABLE_NAME"] == "META_T"
    assert row["TABLE_TYPE"] == "LOCAL TEMPORARY"
    assert row["IS_TEMPORARY"] == "YES"
    assert row["COMMENT"] == "initial"
    assert isinstance(row["CREATED"], datetime)
    assert row["CREATED"] > datetime(2000, 1, 1, tzinfo=UTC)

    dcur.execute("COMMENT ON TABLE db1.schema1.meta_t IS 'updated'")
    dcur.execute("SELECT comment FROM information_schema.tables WHERE table_name = 'META_T'")
    assert dcur.fetchall() == [{"COMMENT": "updated"}]

    dcur.execute("ALTER TABLE db1.schema1.meta_t SET COMMENT = 'altered'")
    dcur.execute("SELECT comment FROM information_schema.tables WHERE table_name = 'META_T'")
    assert dcur.fetchall() == [{"COMMENT": "altered"}]


def test_describe_and_information_schema_columns_for_temporary_table(
    dcur: snowflake.connector.cursor.DictCursor,
) -> None:
    dcur.execute("CREATE SCHEMA other_schema")
    dcur.execute("USE SCHEMA schema1")
    dcur.execute("CREATE TEMP TABLE other_schema.typed_t (code VARCHAR(17), amount NUMBER(9, 2))")

    dcur.execute("DESCRIBE TABLE other_schema.typed_t")
    assert [(r["name"], r["type"]) for r in dcur.fetchall()] == [
        ("CODE", "VARCHAR(17)"),
        ("AMOUNT", "NUMBER(9,2)"),
    ]

    dcur.execute(
        """
        SELECT table_schema, table_name, column_name, data_type,
               character_maximum_length, character_octet_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = 'OTHER_SCHEMA' AND table_name = 'TYPED_T'
        ORDER BY ordinal_position
        """
    )
    assert dcur.fetchall() == [
        {
            "TABLE_SCHEMA": "OTHER_SCHEMA",
            "TABLE_NAME": "TYPED_T",
            "COLUMN_NAME": "CODE",
            "DATA_TYPE": "TEXT",
            "CHARACTER_MAXIMUM_LENGTH": 17,
            "CHARACTER_OCTET_LENGTH": 68,
            "NUMERIC_PRECISION": None,
            "NUMERIC_SCALE": None,
        },
        {
            "TABLE_SCHEMA": "OTHER_SCHEMA",
            "TABLE_NAME": "TYPED_T",
            "COLUMN_NAME": "AMOUNT",
            "DATA_TYPE": "NUMBER",
            "CHARACTER_MAXIMUM_LENGTH": None,
            "CHARACTER_OCTET_LENGTH": None,
            "NUMERIC_PRECISION": 9,
            "NUMERIC_SCALE": 2,
        },
    ]


def test_internal_temp_catalog_tables_hidden_from_show(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE TEMP TABLE visible_only_in_info_schema (id INT)")

    dcur.execute("SHOW TABLES IN ACCOUNT")
    assert not any(r["name"].startswith("_fs_temp") for r in dcur.fetchall())

    dcur.execute("SHOW OBJECTS IN ACCOUNT")
    assert not any(r["name"].startswith("_fs_temp") for r in dcur.fetchall())

    dcur.execute("SHOW COLUMNS IN ACCOUNT")
    assert not any(r["table_name"].startswith("_fs_temp") for r in dcur.fetchall())


def test_temporary_metadata_does_not_persist_in_shared_tables_after_aborted_close(_fakesnow: None) -> None:
    with snowflake.connector.connect(database="db1", schema="schema1") as conn1, conn1.cursor() as cur1:
        cur1.execute("CREATE TABLE perm_tx (id INT)")
        cur1.execute("CREATE TEMP TABLE temp_meta (name VARCHAR(17)) COMMENT = 'session only'")
        cur1.execute("BEGIN")
        with pytest.raises(snowflake.connector.errors.ProgrammingError):
            cur1.execute("INSERT INTO perm_tx VALUES ('not_an_int')")
        # Close while transaction is aborted; no temp metadata should exist in shared tables.

    with snowflake.connector.connect(database="db1", schema="schema1") as conn2, conn2.cursor() as cur2:
        cur2.execute("SELECT * FROM _fs_global._fs_information_schema._fs_tables_ext")
        assert cur2.fetchall() == []
        cur2.execute("SELECT * FROM _fs_global._fs_information_schema._fs_columns_ext")
        assert cur2.fetchall() == []


def test_create_temp_table_inside_explicit_begin_survives_rollback(
    cur: snowflake.connector.cursor.SnowflakeCursor,
) -> None:
    cur.execute("CREATE TABLE tx_perm (id INT)")
    cur.execute("BEGIN")
    cur.execute("INSERT INTO tx_perm VALUES (1)")
    # Snowflake implicitly commits active transactions before DDL.
    cur.execute("CREATE TEMP TABLE tx_temp AS SELECT 2 AS id")
    cur.execute("ROLLBACK")

    cur.execute("SELECT id FROM tx_perm")
    assert cur.fetchall() == [(1,)]
    cur.execute("SELECT id FROM tx_temp")
    assert cur.fetchall() == [(2,)]


def test_failed_replace_preserves_existing_temporary_table(cur: snowflake.connector.cursor.SnowflakeCursor) -> None:
    cur.execute("CREATE TEMP TABLE atomic_t AS SELECT 1 AS id")
    with pytest.raises(snowflake.connector.errors.ProgrammingError):
        cur.execute("CREATE OR REPLACE TEMP TABLE atomic_t AS SELECT * FROM nonexistent_source")

    cur.execute("SELECT id FROM atomic_t")
    assert cur.fetchall() == [(1,)]

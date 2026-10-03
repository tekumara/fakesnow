import gzip
import os
import re
import tempfile
from datetime import timezone
from pathlib import Path

import pytest
import snowflake.connector.cursor
from dirty_equals import IsDatetime, IsInt, IsNow, IsStr


def test_create_stage(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE DATABASE db2")
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("USE SCHEMA db1.schema1")
    dcur.execute("CREATE SCHEMA schema3")

    dcur.execute("USE SCHEMA db1.schema1")
    dcur.execute("CREATE STAGE stage1")
    assert dcur.fetchall() == [{"status": "Stage area STAGE1 successfully created."}]

    dcur.execute("CREATE TEMP STAGE db2.schema2.stage2")
    dcur.execute("CREATE STAGE schema3.stage3 URL='s3://bucket/path/'")
    # lowercase url
    dcur.execute("CREATE TEMP STAGE stage4 url='s3://bucket/path/'")

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("CREATE STAGE stage1")

    assert str(excinfo.value) == "002002 (42710): SQL compilation error:\nObject 'STAGE1' already exists."

    common_fields = {
        "created_on": IsNow(tz=timezone.utc),
        "has_credentials": "N",
        "has_encryption_key": "N",
        "owner": "SYSADMIN",
        "comment": "",
        "region": None,
        "notification_channel": None,
        "storage_integration": None,
        "endpoint": None,
        "owner_role_type": "ROLE",
        "directory_enabled": "N",
    }

    stage1 = {
        **common_fields,
        "name": "STAGE1",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
        "url": "",
        "type": "INTERNAL",
        "cloud": None,
    }
    stage2 = {
        **common_fields,
        "name": "STAGE2",
        "database_name": "DB2",
        "schema_name": "SCHEMA2",
        "url": "",
        "type": "INTERNAL TEMPORARY",
        "cloud": None,
    }
    stage3 = {
        **common_fields,
        "name": "STAGE3",
        "database_name": "DB1",
        "schema_name": "SCHEMA3",
        "url": "s3://bucket/path/",
        "type": "EXTERNAL",
        "cloud": "AWS",
    }
    stage4 = {
        **common_fields,
        "name": "STAGE4",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
        "url": "s3://bucket/path/",
        "type": "EXTERNAL TEMPORARY",
        "cloud": "AWS",
    }

    dcur.execute("SHOW STAGES")
    assert dcur.fetchall() == [
        stage1,
        stage4,
    ]

    dcur.execute("SHOW STAGES in DATABASE db2")
    assert dcur.fetchall() == [
        stage2,
    ]

    dcur.execute("SHOW STAGES in SCHEMA schema3")
    assert dcur.fetchall() == [
        stage3,
    ]

    dcur.execute("SHOW STAGES in db2.schema2")
    assert dcur.fetchall() == [
        stage2,
    ]

    dcur.execute("SHOW STAGES IN ACCOUNT")
    assert dcur.fetchall() == [
        stage1,
        stage2,
        stage3,
        stage4,
    ]


def test_create_stage_or_replace(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE STAGE stage1")

    dcur.execute("CREATE OR REPLACE STAGE stage1 URL='s3://bucket/path/'")
    assert dcur.fetchall() == [{"status": "Stage area STAGE1 successfully created."}]

    dcur.execute("SHOW STAGES")
    rows = dcur.fetchall()
    assert len(rows) == 1
    assert rows[0]["url"] == "s3://bucket/path/"


def test_create_stage_if_not_exists(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE STAGE IF NOT EXISTS stage1")
    assert dcur.fetchall() == [{"status": "Stage area STAGE1 successfully created."}]

    dcur.execute("CREATE STAGE IF NOT EXISTS stage1")
    assert dcur.fetchall() == [{"status": "STAGE1 already exists, statement succeeded."}]

    dcur.execute("SHOW STAGES")
    assert len(dcur.fetchall()) == 1


def test_create_stage_qmark_quoted(_fakesnow: None):
    with (
        snowflake.connector.connect(database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor(snowflake.connector.cursor.DictCursor) as dcur,
    ):
        dcur.execute("CREATE STAGE identifier(?)", ('"stage1"',))
        assert dcur.fetchall() == [{"status": "Stage area stage1 successfully created."}]


def test_put_list(dcur: snowflake.connector.cursor.DictCursor) -> None:
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        dcur.execute("CREATE STAGE stage4")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage4")
        put_results = dcur.fetchall()
        assert put_results == [
            {
                "source": temp_file_basename,
                "target": f"{temp_file_basename}.gz",
                "source_size": len(data),
                # Snowflake client-side encryption can make internal-stage targets
                # larger; fakesnow stores plain local files.
                "target_size": IsInt(ge=len(data)),
                "source_compression": "NONE",
                "target_compression": "GZIP",
                "status": "UPLOADED",
                "message": "",
            }
        ]

        dcur.execute("LIST @stage4")
        results = dcur.fetchall()
        assert len(results) == 1
        assert results[0] == {
            "name": f"stage4/{temp_file_basename}.gz",
            "size": put_results[0]["target_size"],
            "md5": IsStr(regex=r"^[0-9a-f]{32}$"),
            "last_modified": IsDatetime(
                # string in RFC 7231 date format (e.g. 'Sat, 31 May 2025 08:50:51 GMT')
                format_string="%a, %d %b %Y %H:%M:%S GMT"
            ),
        }

        # fully qualified stage name quoted
        dcur.execute('CREATE STAGE db1.schema1."stage5"')
        dcur.execute(f"PUT 'file://{temp_file_path}' @db1.schema1.\"stage5\"")


def test_put_list_subdirectory(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE STAGE nested_stage")
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        temp_file.write("1,2\n")
        temp_file.flush()
        basename = os.path.basename(temp_file.name)

        dcur.execute(f"PUT 'file://{temp_file.name}' @nested_stage/subdir/deeper")
        dcur.execute("LIST @nested_stage")
        assert [r["name"] for r in dcur.fetchall()] == [f"nested_stage/subdir/deeper/{basename}.gz"]


@pytest.mark.parametrize("bound_target", [False, True], ids=["sql", "bound"])
def test_put_rejects_path_outside_stage(_fakesnow: None, bound_target: bool) -> None:
    with (
        snowflake.connector.connect(database="db1", schema="schema1", paramstyle="qmark") as conn,
        conn.cursor() as cur,
        tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file,
    ):
        temp_file.write("1,2\n")
        temp_file.flush()
        cur.execute("CREATE STAGE source_stage")
        cur.execute("CREATE STAGE other_stage")
        target = "@source_stage/../OTHER_STAGE"

        with pytest.raises(snowflake.connector.errors.OperationalError) as excinfo:
            if bound_target:
                cur.execute(f"PUT 'file://{temp_file.name}' ?", (target,))
            else:
                cur.execute(f"PUT 'file://{temp_file.name}' {target}")

        assert excinfo.value.errno == 253003
        assert str(excinfo.value) == IsStr(
            regex=re.escape(
                "253003: While putting file(s) there was an error: 'HTTPError('403 Client Error: Forbidden for url: "
            )
            + r".*/DB1/SCHEMA1/SOURCE_STAGE/\.\./OTHER_STAGE"
            + re.escape("')', this might be caused by your access to the blob storage provider, or by Snowflake.")
        )


@pytest.mark.parametrize("cursor_fixture", ["dcur", "sdcur"])
def test_put_list_shadowed_table_stage(request: pytest.FixtureRequest, cursor_fixture: str, tmp_path: Path) -> None:
    cur = request.getfixturevalue(cursor_fixture)
    cur.execute("CREATE TABLE shadowed_stage_table (a INT)")
    cur.execute("CREATE TEMP TABLE shadowed_stage_table (a INT)")
    path = tmp_path / "data.csv"
    path.write_text("1\n")

    cur.execute(f"PUT 'file://{path}' @%shadowed_stage_table AUTO_COMPRESS=FALSE")
    assert [r["status"] for r in cur.fetchall()] == ["UPLOADED"]
    cur.execute("LIST @db1.schema1.%shadowed_stage_table")
    assert [r["name"] for r in cur.fetchall()] == ["data.csv"]


@pytest.mark.parametrize("comment", ["-- uploaded files", "/* uploaded files */"])
@pytest.mark.parametrize(
    ("stage_name", "reference"),
    [("commented_stage", "@commented_stage"), ('"commented -- stage"', "'@\"commented -- stage\"'")],
)
def test_list_stage_with_trailing_comment(
    dcur: snowflake.connector.cursor.DictCursor, comment: str, stage_name: str, reference: str
) -> None:
    dcur.execute(f"CREATE STAGE {stage_name}")
    dcur.execute(f"LIST {reference} {comment}")
    assert dcur.fetchall() == []


def test_put_unquoted_src(dcur: snowflake.connector.cursor.DictCursor) -> None:
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        temp_file.write("1,2\n")
        temp_file.flush()
        temp_file_basename = os.path.basename(temp_file.name)

        dcur.execute("CREATE STAGE stage6")
        dcur.execute(f"PUT file://{temp_file.name} @stage6")
        assert dcur.fetchall()[0]["source"] == temp_file_basename


def test_put_rejects_collection_auto_compress(dcur: snowflake.connector.cursor.DictCursor) -> None:
    dcur.execute("CREATE STAGE stage7")

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("PUT 'file:///tmp/example.csv' @stage7 AUTO_COMPRESS=(FALSE)")

    assert str(excinfo.value) == "001481 (42601): Invalid value specified for property 'AUTO_COMPRESS'"


def test_put_auto_compress_false(dcur: snowflake.connector.cursor.DictCursor) -> None:
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv") as temp_file:
        data = "1,2\n"
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        dcur.execute("CREATE STAGE stage7")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage7 AUTO_COMPRESS=FALSE")
        put_results = dcur.fetchall()
        assert put_results == [
            {
                "source": temp_file_basename,
                "target": temp_file_basename,
                "source_size": len(data),
                # Snowflake client-side encryption can make internal-stage targets
                # larger; fakesnow stores plain local files.
                "target_size": IsInt(ge=len(data)),
                "source_compression": "NONE",
                "target_compression": "NONE",
                "status": "UPLOADED",
                "message": "",
            }
        ]

        dcur.execute("LIST @stage7")
        results = dcur.fetchall()
        assert len(results) == 1
        assert results[0]["name"] == f"stage7/{temp_file_basename}"
        assert results[0]["size"] == put_results[0]["target_size"]


def test_put_gzipped_src_not_recompressed(dcur: snowflake.connector.cursor.DictCursor) -> None:
    with tempfile.NamedTemporaryFile(suffix=".csv.gz") as temp_file:
        data = gzip.compress(b"1,2\n")
        temp_file.write(data)
        temp_file.flush()
        temp_file_path = temp_file.name
        temp_file_basename = os.path.basename(temp_file_path)

        dcur.execute("CREATE STAGE stage7")
        dcur.execute(f"PUT 'file://{temp_file_path}' @stage7")
        assert dcur.fetchall() == [
            {
                "source": temp_file_basename,
                "target": temp_file_basename,
                "source_size": len(data),
                # Snowflake client-side encryption can make internal-stage targets
                # larger; fakesnow stores plain local files.
                "target_size": IsInt(ge=len(data)),
                "source_compression": "GZIP",
                "target_compression": "GZIP",
                "status": "UPLOADED",
                "message": "",
            }
        ]

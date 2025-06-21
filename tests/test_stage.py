import os
import tempfile
from datetime import timezone

import pytest
import snowflake.connector.cursor
from dirty_equals import IsDatetime, IsNow


def test_create_stage(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE DATABASE db2")
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE SCHEMA schema3")

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
        assert dcur.fetchall() == [
            {
                "source": temp_file_basename,
                "target": f"{temp_file_basename}.gz",
                "source_size": len(data),
                "target_size": 42,  # GZIP compressed size
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
            "size": 42,
            "md5": "29498d110c32a756df8109e70d22fa36",
            "last_modified": IsDatetime(
                # string in RFC 7231 date format (e.g. 'Sat, 31 May 2025 08:50:51 GMT')
                format_string="%a, %d %b %Y %H:%M:%S GMT"
            ),
        }

        # fully qualified stage name quoted
        dcur.execute('CREATE STAGE db1.schema1."stage5"')
        dcur.execute(f"PUT 'file://{temp_file_path}' @db1.schema1.\"stage5\"")

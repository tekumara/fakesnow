from datetime import timezone

import snowflake.connector.cursor
from dirty_equals import IsNow


def test_create_stage(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE DATABASE db2")
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE SCHEMA schema3")

    dcur.execute("CREATE STAGE stage1")
    assert dcur.fetchall() == [{"status": "Stage area STAGE1 successfully created."}]

    dcur.execute("CREATE TEMP STAGE db2.schema2.stage2")
    dcur.execute("CREATE STAGE schema3.stage3 URL='s3://bucket/path/'")
    dcur.execute("CREATE TEMP STAGE stage4 URL='s3://bucket/path/'")

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

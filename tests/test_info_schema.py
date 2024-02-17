from datetime import datetime, timezone

import snowflake.connector.cursor

UTC = timezone.utc


def test_select_views_empty(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("SELECT * FROM information_schema.views")
    assert result
    assert result.fetchall() == []


def test_select_views_with_created_view(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute("CREATE TABLE foo (id INTEGER, name VARCHAR)")
    cur.execute("CREATE VIEW bar AS SELECT * FROM foo WHERE id > 5")

    cur.execute("SELECT * FROM information_schema.views")
    assert cur.fetchall() == [
        {
            "table_catalog": "DB1",
            "table_schema": "SCHEMA1",
            "table_name": "BAR",
            "table_owner": "SYSADMIN",
            "view_definition": "CREATE VIEW SCHEMA1.BAR AS SELECT * FROM FOO WHERE (ID > 5);\n",
            "check_option": "NONE",
            "is_updatable": "NO",
            "insertable_into": "NO",
            "is_secure": "NO",
            "created": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "last_altered": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "last_ddl": datetime(1970, 1, 1, tzinfo=pytz.utc),
            "last_ddl_by": "SYSADMIN",
            "comment": None,
        }
    ]

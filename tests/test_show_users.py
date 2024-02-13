import snowflake.connector.cursor

from fakesnow.connection import USERS_TABLE_FQ_NAME


def test_show_users_base_case(cur: snowflake.connector.cursor.SnowflakeCursor):
    result = cur.execute("SHOW USERS")
    assert result
    assert result.fetchall() == []


def test_show_users_with_users(cur: snowflake.connector.cursor.SnowflakeCursor):
    cur.execute(f"INSERT INTO {USERS_TABLE_FQ_NAME} (name) VALUES ('foo'), ('bar')")

    result = cur.execute("SHOW USERS")
    assert result

    rows = result.fetchall()
    names = [row[0] for row in rows]
    assert names == ["foo", "bar"]

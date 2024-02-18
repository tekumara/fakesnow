from sqlalchemy.engine import Engine
from sqlalchemy.sql.expression import TextClause


def test_create_view(snowflake_engine: Engine):
    """SQLalchemy internally calls `cursor.description`, which will fail if `CREATE VIEW` isn't handled."""
    with snowflake_engine.connect() as conn:
        conn.execute(TextClause("CREATE VIEW foo AS SELECT * FROM information_schema.databases"))

        result = conn.execute("SELECT database_name FROM foo")
        assert result
        assert result.fetchall() == [("DB1",)]

import duckdb
import pytest
from sqlalchemy import Column, MetaData, Table, types
from sqlalchemy.engine import Engine
from sqlalchemy.sql.expression import TextClause


def test_engine(snowflake_engine: Engine):
    # verifies cursor.description, commit, and rollback issued by SQLAlchemy
    with snowflake_engine.connect() as conn:
        conn.execute(TextClause("CREATE VIEW foo AS SELECT * FROM information_schema.databases"))

        result = conn.execute("SELECT database_name FROM foo")
        assert result
        assert result.fetchall() == [("DB1",)]


def test_metadata_create_all(snowflake_engine: Engine):
    metadata = MetaData()

    table = Table("foo", metadata, Column(types.Integer, name="id"), Column(types.String, name="name"))
    metadata.create_all(bind=snowflake_engine)

    with snowflake_engine.connect() as conn:
        result = conn.execute(table.select())
        assert result
        assert result.fetchall() == []


@pytest.mark.xfail(
    reason="sqlglot currently has unsupported SHOW variants",
    strict=True,
    raises=duckdb.ParserException,
)
def test_reflect(snowflake_engine: Engine):
    snowflake_engine.execute(TextClause("CREATE TABLE foo (id INTEGER, name VARCHAR)"))

    metadata = MetaData()
    metadata.reflect(bind=snowflake_engine, only=["foo"])

import datetime
from typing import cast

import snowflake.sqlalchemy
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

    table = cast(Table, Table("foo", metadata, Column(types.Integer, name="id"), Column(types.String, name="name")))
    metadata.create_all(bind=snowflake_engine)

    with snowflake_engine.connect() as conn:
        result = conn.execute(table.select())
        assert result
        assert result.fetchall() == []


def test_reflect(snowflake_engine: Engine):
    with snowflake_engine.connect() as conn:
        conn.execute(TextClause("CREATE TABLE foo (id INTEGER, name VARCHAR)"))

    metadata = MetaData()
    metadata.reflect(bind=snowflake_engine, only=["foo"])

    assert metadata.tables
    foo_table: Table = metadata.tables["foo"]

    with snowflake_engine.connect() as conn:
        result = conn.execute(foo_table.insert().values(id=1, name="one"))

        result = conn.execute(foo_table.select())

        assert result
        assert result.fetchall() == [(1, "one")]


def test_timestamp_ntz(snowflake_engine: Engine):
    metadata = MetaData()
    Table(
        "foo",
        metadata,
        Column("created_at", snowflake.sqlalchemy.TIMESTAMP_NTZ(), nullable=False),
    )

    metadata.create_all(bind=snowflake_engine)
    with snowflake_engine.connect() as conn:
        conn.execute(TextClause("INSERT INTO foo (created_at) VALUES ('2020-01-01')"))
        result = conn.execute(TextClause("SELECT created_at FROM foo"))

        assert result
        assert result.fetch() == [datetime.datetime(2020, 1, 1, 0, 0)]

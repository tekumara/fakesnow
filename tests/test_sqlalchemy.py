from typing import cast

from sqlalchemy import Column, MetaData, Table, types
from sqlalchemy.engine import Engine
from sqlalchemy.sql.expression import TextClause


def test_engine(snowflake_engine: Engine):
    # verifies cursor.description, commit, and rollback issued by SQLAlchemy
    with snowflake_engine.connect() as conn:
        conn.execute(TextClause("CREATE VIEW foo AS SELECT * FROM information_schema.databases"))

        result = conn.execute(TextClause("SELECT database_name FROM foo"))
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
    # runs with autocommit=False
    # when the connection is closed, any open transactions are rolled back
    # and the connection is returned to the pool for reuse
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


def test_create_checkfirst_keeps_transaction(snowflake_engine: Engine):
    # runs with autocommit=False, and checkfirst probes for the missing table with a statement that fails,
    # which leaves the transaction and its earlier insert intact
    metadata = MetaData()
    foo = cast(Table, Table("foo", metadata, Column(types.Integer, name="id")))
    bar = cast(Table, Table("bar", metadata, Column(types.Integer, name="id")))

    with snowflake_engine.connect() as conn:
        foo.create(conn)
        conn.execute(foo.insert().values(id=1))
        bar.create(conn, checkfirst=True)
        conn.commit()

    with snowflake_engine.connect() as conn:
        assert conn.execute(foo.select()).fetchall() == [(1,)]

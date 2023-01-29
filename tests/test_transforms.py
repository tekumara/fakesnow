import sqlglot

from fakesnow.transforms import database_prefix


def test_database_prefix() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM staging.jaffles").transform(database_prefix).sql()
        == "SELECT * FROM staging_jaffles"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM staging.jaffles AS jaffs").transform(database_prefix).sql()
        == "SELECT * FROM staging_jaffles AS jaffs"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA IF NOT EXISTS staging.jaffles").transform(database_prefix).sql()
        == "CREATE SCHEMA IF NOT EXISTS staging_jaffles"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA IF NOT EXISTS jaffles").transform(database_prefix).sql()
        == "CREATE SCHEMA IF NOT EXISTS jaffles"
    )


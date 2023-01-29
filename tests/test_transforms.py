import sqlglot

from fakesnow.transforms import database_as_schema


def test_database_prefix() -> None:
    assert (
        sqlglot.parse_one("SELECT * FROM prod.staging.jaffles").transform(database_as_schema).sql()
        == "SELECT * FROM prod_staging.jaffles"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM staging.jaffles").transform(database_as_schema).sql()
        == "SELECT * FROM staging.jaffles"
    )

    assert (
        sqlglot.parse_one("SELECT * FROM jaffles").transform(database_as_schema).sql()
        == "SELECT * FROM jaffles"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA prod.staging").transform(database_as_schema).sql()
        == "CREATE SCHEMA prod_staging"
    )

    assert (
        sqlglot.parse_one("CREATE SCHEMA staging").transform(database_as_schema).sql()
        == "CREATE SCHEMA staging"
    )

    assert (
        sqlglot.parse_one("DROP SCHEMA prod.staging").transform(database_as_schema).sql()
        == "DROP SCHEMA prod_staging"
    )

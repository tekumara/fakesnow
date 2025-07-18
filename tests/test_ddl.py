import sqlglot

from fakesnow.transforms.ddl import alter_table_add_multiple_columns


def test_alter_table_add_multiple_columns() -> None:
    # Test multiple columns are split into separate statements
    sql = "alter table tab1 add column col1 int, col2 varchar(50), col3 boolean"
    result = alter_table_add_multiple_columns(sqlglot.parse_one(sql, dialect="snowflake"))

    assert len(result) == 3
    assert result[0].sql() == "ALTER TABLE tab1 ADD COLUMN col1 INT"
    assert result[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"
    assert result[2].sql() == "ALTER TABLE tab1 ADD COLUMN col3 BOOLEAN"

    # Test multiple columns with IF EXISTS clause
    sql_if_exists = "alter table if exists tab1 add column col1 int, col2 varchar(50)"
    result_if_exists = alter_table_add_multiple_columns(sqlglot.parse_one(sql_if_exists, dialect="snowflake"))

    assert len(result_if_exists) == 2
    assert result_if_exists[0].sql() == "ALTER TABLE IF EXISTS tab1 ADD COLUMN col1 INT"
    assert result_if_exists[1].sql() == "ALTER TABLE IF EXISTS tab1 ADD COLUMN col2 VARCHAR(50)"

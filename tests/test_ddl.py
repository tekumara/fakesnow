import sqlglot

from fakesnow.transforms.ddl import alter_table_add_multiple_columns, alter_table_strip_cluster_by


def test_alter_table_add_multiple_columns() -> None:
    # Test multiple columns are split into separate statements
    sql = "alter table tab1 add column col1 int, col2 varchar(50), col3 boolean"
    result = alter_table_add_multiple_columns(sqlglot.parse_one(sql, dialect="snowflake"))

    assert len(result) == 3
    assert result[0].sql() == "ALTER TABLE tab1 ADD COLUMN col1 INT"
    assert result[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"
    assert result[2].sql() == "ALTER TABLE tab1 ADD COLUMN col3 BOOLEAN"

    # Test multiple columns with mix with one column having IF NOT EXISTS
    sql_if_not_exists = "alter table tab1 add column if not exists col1 int, col2 varchar(50)"
    result_if_not_exists = alter_table_add_multiple_columns(sqlglot.parse_one(sql_if_not_exists, dialect="snowflake"))

    assert len(result_if_not_exists) == 2
    assert result_if_not_exists[0].sql() == "ALTER TABLE tab1 ADD COLUMN IF NOT EXISTS col1 INT"
    assert result_if_not_exists[1].sql() == "ALTER TABLE tab1 ADD COLUMN col2 VARCHAR(50)"


def test_alter_table_strip_cluster_by() -> None:
    assert (
        sqlglot.parse_one("alter table table1 cluster by (name)").transform(alter_table_strip_cluster_by).sql()
        == "SELECT 'Statement executed successfully.' AS status"
    )

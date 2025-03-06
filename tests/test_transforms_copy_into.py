import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms.copy_into import copy_into

example1 = """
COPY INTO schema_.table_ (a, b)
FROM "s3://bucket/"
STORAGE_INTEGRATION = some_name
FILES=('foo.txt')
FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' FIELD_DELIMITER = '|')
FORCE = TRUE;
"""


def test_transform_example1() -> None:
    result = str(sqlglot.parse_one(example1, read="snowflake").transform(copy_into))

    expected_result = (
        "INSERT INTO schema_.table_ "
        "SELECT a, b "
        "FROM READ_CSV('s3://bucket/foo.txt', header = 1, quote = '\"', sep = ',')"
    )
    assert result == expected_result


def test_execute_example1(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE SCHEMA schema_")
    dcur.execute("CREATE TABLE schema_.table_ (a INT, b INT)")
    dcur.execute(example1)

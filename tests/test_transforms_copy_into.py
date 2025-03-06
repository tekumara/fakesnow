import uuid

import snowflake.connector.cursor
import sqlglot

from fakesnow.transforms.copy_into import copy_into
from tests.moto import Session


def create_table(dcur: snowflake.connector.cursor.DictCursor):
    dcur.execute("CREATE SCHEMA schema_")
    dcur.execute("CREATE TABLE schema_.table_ (a INT, b INT)")


def upload_file(moto: Session, data: str, key: str = "foo.txt"):
    bucket = str(uuid.uuid4())
    client = moto.client()
    client.create_bucket(Bucket=bucket)
    client.put_object(Bucket=bucket, Key=key, Body=data)
    return bucket


class TestCSV:
    sql = """
    COPY INTO schema_.table_ (a, b)
    FROM "s3://{bucket}/"
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV')
    FORCE = TRUE;
    """

    def test_transform(self) -> None:
        result = str(sqlglot.parse_one(self.sql.format(bucket="bucket"), read="snowflake").transform(copy_into))

        expected_result = "INSERT INTO schema_.table_ SELECT a, b FROM READ_CSV('s3://bucket/foo.txt', header = 1)"
        assert result == expected_result

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, moto: Session):
        create_table(dcur)
        bucket = upload_file(moto, "a,b\n1,2\n")
        dcur.execute(self.sql.format(bucket=bucket))


class TestCSVDelimiter:
    sql = """
    COPY INTO schema_.table_ (a, b)
    FROM "s3://{bucket}/"
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV' FIELD_DELIMITER = '|')
    FORCE = TRUE;
    """

    def test_transform(self) -> None:
        result = str(sqlglot.parse_one(self.sql.format(bucket="bucket"), read="snowflake").transform(copy_into))

        expected_result = (
            "INSERT INTO schema_.table_ SELECT a, b FROM READ_CSV('s3://bucket/foo.txt', header = 1, sep = '|')"
        )
        assert result == expected_result

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, moto: Session):
        create_table(dcur)
        bucket = upload_file(moto, "a|b\n1|2\n")
        dcur.execute(self.sql.format(bucket=bucket))


class TestCSVSkipHeader:
    sql = """
    COPY INTO schema_.table_ (a, b)
    FROM "s3://{bucket}/"
    STORAGE_INTEGRATION = some_name
    FILES=('foo.txt')
    FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
    FORCE = TRUE;
    """

    def test_transform(self) -> None:
        result = str(sqlglot.parse_one(self.sql.format(bucket="bucket"), read="snowflake").transform(copy_into))

        expected_result = (
            "INSERT INTO schema_.table_ SELECT a, b FROM READ_CSV('s3://bucket/foo.txt', names = ARRAY('a', 'b'))"
        )
        assert result == expected_result

    def test_execute(self, dcur: snowflake.connector.cursor.DictCursor, moto: Session):
        create_table(dcur)

        bucket = upload_file(moto, "a|b\n1|2\n")
        dcur.execute(self.sql.format(bucket=bucket))

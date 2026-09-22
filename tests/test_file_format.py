import json
from typing import Any, cast

import pytest
import snowflake.connector.cursor
from dirty_equals import IsDatetime, IsStr

from tests.utils import dindent


def test_create_file_format(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV' FIELD_DELIMITER=',' SKIP_HEADER=1")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]

    with pytest.raises(snowflake.connector.errors.ProgrammingError) as excinfo:
        dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

    assert str(excinfo.value) == "002002 (42710): SQL compilation error:\nObject 'MY_FMT' already exists."


def test_create_file_format_or_replace(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

    dcur.execute("CREATE OR REPLACE FILE FORMAT my_fmt TYPE='CSV' SKIP_HEADER=1")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]


def test_create_file_format_if_not_exists(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT IF NOT EXISTS my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]

    dcur.execute("CREATE FILE FORMAT IF NOT EXISTS my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "MY_FMT already exists, statement succeeded."}]


def test_create_file_format_fully_qualified(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT db1.schema1.my_fmt TYPE='CSV'")
    assert dcur.fetchall() == [{"status": "File format MY_FMT successfully created."}]


@pytest.mark.parametrize("statement", ["SHOW FILE FORMATS", "SHOW TERSE FILE FORMATS"])
def test_show_file_formats_metadata(dcur: snowflake.connector.cursor.SnowflakeCursor, statement: str):
    dcur.execute('CREATE FILE FORMAT "My Format" TYPE=CSV')

    dcur.execute(statement)
    expected = {
        "created_on": IsDatetime(),
        "name": "My Format",
        "database_name": "DB1",
        "schema_name": "SCHEMA1",
        "type": "CSV",
        "owner": "SYSADMIN",
        "comment": "",
        "format_options": IsStr(),
        "owner_role_type": "ROLE",
    }
    assert dcur.fetchall() == [expected]
    assert dcur.description
    assert [column.name for column in dcur.description] == list(expected)


@pytest.mark.parametrize(
    ("format_type", "options"),
    [
        (
            "CSV",
            {
                "RECORD_DELIMITER": "\n",
                "FIELD_DELIMITER": ",",
                "FILE_EXTENSION": None,
                "SKIP_HEADER": 0,
                "PARSE_HEADER": False,
                "DATE_FORMAT": "AUTO",
                "TIME_FORMAT": "AUTO",
                "TIMESTAMP_FORMAT": "AUTO",
                "BINARY_FORMAT": "HEX",
                "ESCAPE": "NONE",
                "ESCAPE_UNENCLOSED_FIELD": "\\",
                "TRIM_SPACE": False,
                "FIELD_OPTIONALLY_ENCLOSED_BY": "NONE",
                "NULL_IF": ["\\N"],
                "COMPRESSION": "AUTO",
                "ERROR_ON_COLUMN_COUNT_MISMATCH": True,
                "VALIDATE_UTF8": True,
                "SKIP_BLANK_LINES": False,
                "REPLACE_INVALID_CHARACTERS": False,
                "EMPTY_FIELD_AS_NULL": True,
                "SKIP_BYTE_ORDER_MARK": True,
                "ENCODING": "UTF8",
                "MULTI_LINE": True,
            },
        ),
        (
            "JSON",
            {
                "FILE_EXTENSION": None,
                "DATE_FORMAT": "AUTO",
                "TIME_FORMAT": "AUTO",
                "TIMESTAMP_FORMAT": "AUTO",
                "BINARY_FORMAT": "HEX",
                "TRIM_SPACE": False,
                "NULL_IF": [],
                "COMPRESSION": "AUTO",
                "ENABLE_OCTAL": False,
                "ALLOW_DUPLICATE": False,
                "STRIP_OUTER_ARRAY": False,
                "STRIP_NULL_VALUES": False,
                "IGNORE_UTF8_ERRORS": False,
                "REPLACE_INVALID_CHARACTERS": False,
                "SKIP_BYTE_ORDER_MARK": True,
                "MULTI_LINE": True,
            },
        ),
        (
            "AVRO",
            {"TRIM_SPACE": False, "NULL_IF": [], "COMPRESSION": "AUTO", "REPLACE_INVALID_CHARACTERS": False},
        ),
        ("ORC", {"TRIM_SPACE": False, "NULL_IF": [], "REPLACE_INVALID_CHARACTERS": False}),
        (
            "PARQUET",
            {
                "TRIM_SPACE": False,
                "NULL_IF": [],
                "COMPRESSION": "AUTO",
                "BINARY_AS_TEXT": True,
                "REPLACE_INVALID_CHARACTERS": False,
                "USE_LOGICAL_TYPE": False,
                "USE_VECTORIZED_SCANNER": False,
            },
        ),
        (
            "XML",
            {
                "COMPRESSION": "AUTO",
                "IGNORE_UTF8_ERRORS": False,
                "PRESERVE_SPACE": False,
                "STRIP_OUTER_ELEMENT": False,
                "DISABLE_SNOWFLAKE_DATA": False,
                "DISABLE_AUTO_CONVERT": False,
                "REPLACE_INVALID_CHARACTERS": False,
                "SKIP_BYTE_ORDER_MARK": True,
            },
        ),
    ],
)
def test_show_file_formats_defaults(
    dcur: snowflake.connector.cursor.SnowflakeCursor, format_type: str, options: dict[str, Any]
):
    dcur.execute(f"CREATE FILE FORMAT my_fmt TYPE={format_type}")
    dcur.execute("SHOW FILE FORMATS")
    assert dindent(dcur.fetchall())[0]["format_options"] == json.dumps({"TYPE": format_type, **options}, indent=2)


@pytest.mark.parametrize(
    ("option", "value", "expected"),
    [
        ("SKIP_HEADER", "2", 2),
        ("NULL_IF", "('null', '')", ["null", ""]),
    ],
)
def test_show_file_formats_option_overrides(
    dcur: snowflake.connector.cursor.SnowflakeCursor, option: str, value: str, expected: int | list[str]
):
    dcur.execute(f"CREATE FILE FORMAT my_fmt TYPE=CSV {option}={value}")
    dcur.execute("SHOW FILE FORMATS")
    options = json.loads(cast(list[dict], dcur.fetchall())[0]["format_options"])
    assert options[option] == expected


def test_show_file_formats_comment(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE=CSV COMMENT='Bob''s format'")
    dcur.execute("SHOW FILE FORMATS")
    row = cast(list[dict], dcur.fetchall())[0]
    assert row["comment"] == "Bob's format"
    assert "COMMENT" not in json.loads(row["format_options"])


@pytest.mark.parametrize(
    ("scope", "expected"),
    [
        ("", {"FMT1"}),
        ("IN SCHEMA", {"FMT1"}),
        ("IN SCHEMA schema2", {"FMT2"}),
        ("IN SCHEMA db2.schema2", {"FMT3"}),
        ("IN schema2", {"FMT2"}),
        ("IN db2.schema2", {"FMT3"}),
        ("IN DATABASE", {"FMT1", "FMT2"}),
        ("IN DATABASE db2", {"FMT3"}),
        ("IN ACCOUNT", {"FMT1", "FMT2", "FMT3"}),
    ],
)
def test_show_file_formats_scope(dcur: snowflake.connector.cursor.SnowflakeCursor, scope: str, expected: set[str]):
    dcur.execute("CREATE FILE FORMAT fmt1 TYPE=CSV")
    dcur.execute("CREATE SCHEMA db1.schema2")
    dcur.execute("CREATE FILE FORMAT db1.schema2.fmt2 TYPE=JSON")
    dcur.execute("CREATE DATABASE db2")
    dcur.execute("CREATE SCHEMA db2.schema2")
    dcur.execute("CREATE FILE FORMAT db2.schema2.fmt3 TYPE=PARQUET")
    dcur.execute("USE SCHEMA db1.schema1")

    dcur.execute(f"SHOW FILE FORMATS {scope}")
    assert {row["name"] for row in cast(list[dict], dcur.fetchall())} == expected


def test_show_file_formats_terse_scope(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE FILE FORMAT fmt1 TYPE=CSV")
    dcur.execute("CREATE SCHEMA db1.schema2")
    dcur.execute("CREATE FILE FORMAT db1.schema2.fmt2 TYPE=CSV")
    dcur.execute("show terse file formats in schema db1.schema1")
    assert [row["name"] for row in cast(list[dict], dcur.fetchall())] == ["FMT1"]


def test_show_file_formats_without_current_database(_fakesnow: None):
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as creator,
        snowflake.connector.connect() as conn,
        creator.cursor() as create_cur,
        conn.cursor(snowflake.connector.DictCursor) as cur,
    ):
        create_cur.execute("CREATE FILE FORMAT fmt_a TYPE=CSV")
        create_cur.execute("CREATE DATABASE db2")
        create_cur.execute("CREATE SCHEMA db2.schema2")
        create_cur.execute("CREATE FILE FORMAT db2.schema2.fmt_b TYPE=CSV")
        cur.execute("SHOW FILE FORMATS")
        assert {
            (row["database_name"], row["schema_name"], row["name"]) for row in cast(list[dict], cur.fetchall())
        } == {("DB1", "SCHEMA1", "FMT_A"), ("DB2", "SCHEMA2", "FMT_B")}


def test_show_file_formats_like(dcur: snowflake.connector.cursor.SnowflakeCursor):
    for name in ["csv_one", "csv_two", "json_one"]:
        dcur.execute(f"CREATE FILE FORMAT {name} TYPE=CSV")
    dcur.execute("SHOW FILE FORMATS LIKE 'csv%'")
    assert {row["name"] for row in cast(list[dict], dcur.fetchall())} == {"CSV_ONE", "CSV_TWO"}


def test_show_file_formats_order(dcur: snowflake.connector.cursor.SnowflakeCursor):
    dcur.execute("CREATE SCHEMA db1.schema2")
    for name in ["db1.schema2.a", "db1.schema1.z", "db1.schema1.a"]:
        dcur.execute(f"CREATE FILE FORMAT {name} TYPE=CSV")
    dcur.execute("SHOW FILE FORMATS IN DATABASE db1")
    assert [(row["schema_name"], row["name"]) for row in cast(list[dict], dcur.fetchall())] == [
        ("SCHEMA1", "A"),
        ("SCHEMA1", "Z"),
        ("SCHEMA2", "A"),
    ]


@pytest.mark.parametrize(
    ("statement", "expected"),
    [
        ("CREATE OR REPLACE FILE FORMAT my_fmt TYPE=JSON", "JSON"),
        ("CREATE FILE FORMAT IF NOT EXISTS my_fmt TYPE=JSON", "CSV"),
    ],
)
def test_show_file_formats_after_create_again(
    dcur: snowflake.connector.cursor.SnowflakeCursor, statement: str, expected: str
):
    dcur.execute("CREATE FILE FORMAT my_fmt TYPE=CSV")
    dcur.execute(statement)
    dcur.execute("SHOW FILE FORMATS")
    assert [row["type"] for row in cast(list[dict], dcur.fetchall())] == [expected]


def test_create_file_format_visible_to_other_connections_before_commit(_fakesnow: None):
    with (
        snowflake.connector.connect(database="db1", schema="schema1") as conn1,
        snowflake.connector.connect(database="db1", schema="schema1") as conn2,
        conn1.cursor() as cur1,
        conn2.cursor() as cur2,
    ):
        cur1.execute("BEGIN TRANSACTION")
        cur1.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

        with pytest.raises(snowflake.connector.errors.ProgrammingError, match="already exists"):
            cur2.execute("CREATE FILE FORMAT my_fmt TYPE='CSV'")

        conn1.rollback()

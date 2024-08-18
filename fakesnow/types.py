import re

from snowflake.connector.cursor import ResultMetadata


def describe_as_result_metadata(describe_results: list) -> list[ResultMetadata]:
    # fmt: off
    def as_result_metadata(column_name: str, column_type: str, _: str) -> ResultMetadata:
        # see https://docs.snowflake.com/en/user-guide/python-connector-api.html#type-codes
        # and https://arrow.apache.org/docs/python/api/datatypes.html#type-checking
        if column_type in {"BIGINT", "INTEGER"}:
            return ResultMetadata(
                name=column_name, type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True               # noqa: E501
            )
        elif column_type.startswith("DECIMAL"):
            match = re.search(r'\((\d+),(\d+)\)', column_type)
            if match:
                precision = int(match[1])
                scale = int(match[2])
            else:
                precision = scale = None
            return ResultMetadata(
                name=column_name, type_code=0, display_size=None, internal_size=None, precision=precision, scale=scale, is_nullable=True    # noqa: E501
            )
        elif column_type == "VARCHAR":
            # TODO: fetch internal_size from varchar size
            return ResultMetadata(
                name=column_name, type_code=2, display_size=None, internal_size=16777216, precision=None, scale=None, is_nullable=True      # noqa: E501
            )
        elif column_type == "DOUBLE":
            return ResultMetadata(
                name=column_name, type_code=1, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True          # noqa: E501
            )
        elif column_type == "BOOLEAN":
            return ResultMetadata(
                name=column_name, type_code=13, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True         # noqa: E501
            )
        elif column_type == "DATE":
            return ResultMetadata(
                name=column_name, type_code=3, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True          # noqa: E501
            )
        elif column_type in {"TIMESTAMP", "TIMESTAMP_NS"}:
            return ResultMetadata(
                name=column_name, type_code=8, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True                # noqa: E501
            )
        elif column_type == "TIMESTAMP WITH TIME ZONE":
            return ResultMetadata(
                name=column_name, type_code=7, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True                # noqa: E501
            )
        elif column_type == "BLOB":
            return ResultMetadata(
                name=column_name, type_code=11, display_size=None, internal_size=8388608, precision=None, scale=None, is_nullable=True      # noqa: E501
            )
        elif column_type == "TIME":
            return ResultMetadata(
                name=column_name, type_code=12, display_size=None, internal_size=None, precision=0, scale=9, is_nullable=True               # noqa: E501
            )
        elif column_type == "JSON":
            # TODO: correctly map OBJECT and ARRAY see https://github.com/tekumara/fakesnow/issues/26
            return ResultMetadata(
                name=column_name, type_code=5, display_size=None, internal_size=None, precision=None, scale=None, is_nullable=True               # noqa: E501
            )
        else:
            # TODO handle more types
            raise NotImplementedError(f"for column type {column_type}")

    # fmt: on

    meta = [
        as_result_metadata(column_name, column_type, null)
        for (column_name, column_type, null, _, _, _) in describe_results
    ]
    return meta

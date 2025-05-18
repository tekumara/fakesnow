import pytest
from snowflake.connector.cursor import ResultMetadata

from tests.matchers import IsResultMetadata


def test_is_result_metadata_with_partial_match():
    metadata = ResultMetadata(
        name="COL1", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True
    )

    assert metadata == IsResultMetadata(name="COL1", type_code=0)
    assert metadata == IsResultMetadata(type_code=0, precision=38, scale=0)


def test_is_result_metadata_with_non_match():
    metadata = ResultMetadata(
        name="COL1", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True
    )

    assert metadata != IsResultMetadata(name="DIFFERENT", type_code=0)
    assert metadata != IsResultMetadata(name="COL1", type_code=2)


def test_repr():
    metadata = ResultMetadata(
        name="COL1", type_code=0, display_size=None, internal_size=None, precision=38, scale=0, is_nullable=True
    )

    matcher = IsResultMetadata(name="DIFFERENT", type_code=5)

    with pytest.raises(AssertionError):
        assert metadata == matcher

    assert repr(matcher) == "IsResultMetadata(name='DIFFERENT', type_code=5)"


def test_is_result_metadata_with_non_result_metadata():
    matcher = IsResultMetadata(name="COL1", type_code=2)
    assert ("COL1", 2) != matcher  # noqa: SIM300

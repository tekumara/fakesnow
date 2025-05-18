from typing import Any

from dirty_equals import DirtyEquals
from snowflake.connector.cursor import ResultMetadata


class IsResultMetadata(DirtyEquals[ResultMetadata]):
    """
    A custom dirty_equals matcher that does a partial match against ResultMetadata NamedTuples.

    Example:
        assert result == IsResultMetadata(name='COLUMN_NAME', type_code=2)
    """

    def __init__(self, **kwargs: Any) -> None:
        self.partial_kwargs = kwargs
        super().__init__(**kwargs)

    def equals(self, other: Any) -> bool:  # noqa: ANN401
        if not isinstance(other, ResultMetadata):
            return False

        # Convert the namedtuple to a dict for comparison
        other_dict = other._asdict()

        # Check the provided fields match
        for key, expected in self.partial_kwargs.items():
            if key not in other_dict or other_dict[key] != expected:
                return False

        return True

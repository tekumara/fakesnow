from typing import ClassVar

from sqlglot.dialects.snowflake import Snowflake
from sqlglot.tokenizer_core import TokenType


class SnowflakeWithStageCommands(Snowflake):
    """Let stage transforms parse LIST references that SQLGlot treats as expressions."""

    class Tokenizer(Snowflake.Tokenizer):
        KEYWORDS: ClassVar[dict[str, TokenType]] = {**Snowflake.Tokenizer.KEYWORDS, "LIST": TokenType.COMMAND}

"""Parse Snowflake SQL with Snowflake-compatible syntax errors."""

from typing import ClassVar

import snowflake.connector.errors
import sqlglot
from sqlglot import Expr
from sqlglot.dialects.snowflake import Snowflake
from sqlglot.errors import ParseError
from sqlglot.tokenizer_core import TokenType


class FakeSnowflakeDialect(Snowflake):
    class Tokenizer(Snowflake.Tokenizer):
        # Snowflake uses ? for positional binds, so ?:: is a placeholder followed
        # by a cast, not the generic SQLGlot try-cast operator.
        # https://github.com/tobymao/sqlglot/issues/9313
        KEYWORDS: ClassVar[dict[str, TokenType]] = {
            key: value for key, value in Snowflake.Tokenizer.KEYWORDS.items() if key != "?::"
        }


def parse(command: str) -> list[Expr | None]:
    try:
        return sqlglot.parse(command, read=FakeSnowflakeDialect())
    except ParseError as error:
        raise syntax_error(error) from None


def parse_one(command: str) -> Expr:
    try:
        return sqlglot.parse_one(command, read=FakeSnowflakeDialect())
    except ParseError as error:
        raise syntax_error(error) from None


def syntax_error(error: ParseError) -> snowflake.connector.errors.ProgrammingError:
    # Strip terminal highlighting from SQLGlot's error message.
    message = str(error).replace("\x1b[4m", "").replace("\x1b[0m", "")
    return snowflake.connector.errors.ProgrammingError(msg=message, errno=1003, sqlstate="42000")

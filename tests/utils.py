from __future__ import annotations

import json
from collections.abc import Sequence
from typing import cast


def indent(rows: Sequence[tuple] | Sequence[dict]) -> list[tuple]:
    # indent duckdb json strings tuple values to match snowflake json strings
    assert isinstance(rows[0], tuple), f"{type(rows[0]).__name__} is not tuple"
    return [
        (*[json.dumps(json.loads(c), indent=2) if (isinstance(c, str) and c.startswith(("[", "{"))) else c for c in r],)
        for r in rows
    ]


def dindent(rows: Sequence[tuple] | Sequence[dict]) -> list[dict]:
    # indent duckdb json strings dict values to match snowflake json strings
    assert isinstance(rows[0], dict), f"{type(rows[0]).__name__} is not dict"
    return [
        {
            k: json.dumps(json.loads(v), indent=2) if (isinstance(v, str) and v.startswith(("[", "{"))) else v
            for k, v in cast(dict, r).items()
        }
        for r in rows
    ]


def strip(s: str) -> str:
    '''
    Removes newlines and all leading whitespace from each line in the text. For example:

    text = """
        an example
            text with indentation
                hehe
    """
    dedent(text) == "an example text with indentation hehe"
    '''

    # Split the string into lines
    lines = s.split("\n")

    # Remove empty lines at the beginning and end, and strip whitespace from each line
    lines = [line.strip() for line in lines if line.strip()]

    return " ".join(lines) if lines else ""

from __future__ import annotations

from typing import Any, Union

from sqlglot import exp

MutableParams = Union[list[Any], dict[Any, Any]]


def pop_qmark_param(params: MutableParams | None, expr: exp.Expression, pl: exp.Placeholder) -> Any:  # noqa: ANN401
    assert isinstance(params, list), "params must be provided as a list or tuple to resolve qmarks"
    i = index_of_placeholder(expr, pl)
    return params.pop(i)


def index_of_placeholder(expr: exp.Expression, target: exp.Placeholder) -> int:
    """Count the number of prior placeholders to determine the index.

    Args:
        expression (exp.Expression): The expression to search.
        ph (exp.Placeholder): The placeholder to find.

    Returns:
        int: The index of the placeholder, or -1 if not found.
    """
    for index, ph in enumerate(expr.find_all(exp.Placeholder, bfs=False)):
        if ph is target:
            return index
    return -1

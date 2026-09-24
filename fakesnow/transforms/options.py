from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from sqlglot import Expr, exp


def _option_value(value: Expr | None) -> Any:  # noqa: ANN401
    """Convert an option value expression to a Python value."""
    if isinstance(value, exp.Literal):
        return value.this if value.is_string else int(value.this)
    if isinstance(value, exp.Boolean):
        return value.this
    if isinstance(value, exp.Paren):
        return [_option_value(value.this)]
    if isinstance(value, exp.Tuple):
        return [_option_value(expression) for expression in value.expressions]
    if isinstance(value, exp.Var):
        return value.this
    raise NotImplementedError(f"{value.__class__.__name__} as an option value")


def parse_options(properties: Iterable[Expr], *, statement: str = "SQL") -> dict[str, Any]:
    """Convert SQL option properties to a dict of uppercase names and Python values."""
    options: dict[str, Any] = {}
    for prop in properties:
        if isinstance(prop, exp.TemporaryProperty):
            continue
        assert isinstance(prop, exp.Property), f"{prop.__class__} is not a Property"
        assert isinstance(prop.this, exp.Var), f"{prop.this.__class__} is not a Var"
        name = prop.this.name.upper()
        value = prop.args.get("value")
        try:
            options[name] = _option_value(value)
        except NotImplementedError:
            raise NotImplementedError(f"{statement} option {name} with value {value}") from None
    return options

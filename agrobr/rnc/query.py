from __future__ import annotations

from typing import Any

from agrobr.exceptions import InvalidParameterError


def validate_filters(
    filters: dict[str, str | None],
    extras: dict[str, Any],
    *,
    use_cache: bool,
    as_polars: bool,
    return_meta: bool,
) -> dict[str, str]:
    if extras:
        raise InvalidParameterError(f"Parâmetros desconhecidos: {', '.join(sorted(extras))}")
    for name, flag in (
        ("use_cache", use_cache),
        ("as_polars", as_polars),
        ("return_meta", return_meta),
    ):
        if not isinstance(flag, bool):
            raise InvalidParameterError(f"{name} deve ser booleano")
    selected = {}
    for name, value in filters.items():
        if value is None:
            continue
        if not isinstance(value, str) or not value.strip():
            raise InvalidParameterError(f"{name} deve ser texto não vazio")
        selected[name] = value.strip()
    return selected

from __future__ import annotations

import contextvars
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import date
from functools import wraps
from typing import Any, TypeVar

from agrobr import exceptions

__all__ = [
    "deterministic",
    "deterministic_decorator",
    "get_snapshot",
    "is_deterministic",
]

_snapshot_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "agrobr_snapshot", default=None
)

F = TypeVar("F", bound=Callable[..., Any])


def get_snapshot() -> str | None:
    return _snapshot_var.get()


def is_deterministic() -> bool:
    return _snapshot_var.get() is not None


def _validate_snapshot(snapshot: str) -> None:
    try:
        date.fromisoformat(snapshot)
    except (TypeError, ValueError) as exc:
        raise exceptions.InvalidParameterError(
            f"snapshot inválido: {snapshot!r}. Use uma data ISO no formato AAAA-MM-DD."
        ) from exc


@asynccontextmanager
async def deterministic(snapshot: str) -> AsyncIterator[None]:
    _validate_snapshot(snapshot)
    token = _snapshot_var.set(snapshot)
    try:
        yield
    finally:
        _snapshot_var.reset(token)


def deterministic_decorator(snapshot: str) -> Callable[[F], F]:
    _validate_snapshot(snapshot)

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with deterministic(snapshot):
                return await func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator

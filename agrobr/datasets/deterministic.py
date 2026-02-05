"""Context manager para modo determinístico (reprodutibilidade)."""

from __future__ import annotations

import contextvars
from contextlib import asynccontextmanager
from datetime import date
from functools import wraps
from typing import Any, Callable, TypeVar

_snapshot_var: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "agrobr_snapshot", default=None
)

F = TypeVar("F", bound=Callable[..., Any])


def get_snapshot() -> str | None:
    """Retorna snapshot ativo no contexto atual (ou None)."""
    return _snapshot_var.get()


def is_deterministic() -> bool:
    """Retorna True se estamos em modo determinístico."""
    return _snapshot_var.get() is not None


@asynccontextmanager
async def deterministic(snapshot: str):
    """Context manager para modo determinístico.

    Args:
        snapshot: Data de corte no formato "YYYY-MM-DD".
            Todas as consultas filtram data <= snapshot.
            Rede é bloqueada, apenas cache local é usado.

    Example:
        async with deterministic("2025-12-31"):
            df = await datasets.preco_diario("soja")
    """
    date.fromisoformat(snapshot)
    token = _snapshot_var.set(snapshot)
    try:
        yield
    finally:
        _snapshot_var.reset(token)


def deterministic_decorator(snapshot: str) -> Callable[[F], F]:
    """Decorator para modo determinístico em funções async.

    Args:
        snapshot: Data de corte no formato "YYYY-MM-DD".

    Example:
        @deterministic_decorator("2025-12-31")
        async def meu_pipeline():
            df = await datasets.preco_diario("soja")
            return df
    """
    date.fromisoformat(snapshot)

    def decorator(func: F) -> F:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> Any:
            async with deterministic(snapshot):
                return await func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator

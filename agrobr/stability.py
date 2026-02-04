"""Decorators e utilitarios para estabilidade de API."""

from __future__ import annotations

import functools
import warnings
from collections.abc import Callable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, TypeVar

import structlog

logger = structlog.get_logger()

F = TypeVar("F", bound=Callable[..., Any])


class APIStatus(StrEnum):
    STABLE = "stable"
    EXPERIMENTAL = "experimental"
    DEPRECATED = "deprecated"
    INTERNAL = "internal"


@dataclass
class APIInfo:
    status: APIStatus
    since: str
    deprecated_in: str | None = None
    removed_in: str | None = None
    replacement: str | None = None
    notes: str | None = None


_api_registry: dict[str, APIInfo] = {}


def stable(since: str, notes: str | None = None) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        info = APIInfo(status=APIStatus.STABLE, since=since, notes=notes)
        _api_registry[func.__qualname__] = info

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            return func(*args, **kwargs)

        wrapper._api_info = info  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def experimental(since: str, notes: str | None = None) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        info = APIInfo(status=APIStatus.EXPERIMENTAL, since=since, notes=notes)
        _api_registry[func.__qualname__] = info

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            warnings.warn(
                f"{func.__qualname__} is experimental and may change without notice",
                stacklevel=2,
            )
            return func(*args, **kwargs)

        wrapper._api_info = info  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def deprecated(
    since: str,
    removed_in: str | None = None,
    replacement: str | None = None,
) -> Callable[[F], F]:
    def decorator(func: F) -> F:
        info = APIInfo(
            status=APIStatus.DEPRECATED,
            since=since,
            deprecated_in=since,
            removed_in=removed_in,
            replacement=replacement,
        )
        _api_registry[func.__qualname__] = info

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            msg = f"{func.__qualname__} is deprecated since {since}"
            if removed_in:
                msg += f" and will be removed in {removed_in}"
            if replacement:
                msg += f". Use {replacement} instead"
            warnings.warn(msg, DeprecationWarning, stacklevel=2)
            return func(*args, **kwargs)

        wrapper._api_info = info  # type: ignore[attr-defined]
        return wrapper  # type: ignore[return-value]

    return decorator


def internal(func: F) -> F:
    info = APIInfo(status=APIStatus.INTERNAL, since="0.1.0")
    _api_registry[func.__qualname__] = info

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)

    wrapper._api_info = info  # type: ignore[attr-defined]
    return wrapper  # type: ignore[return-value]


def get_api_info(func: Callable[..., Any]) -> APIInfo | None:
    return getattr(func, "_api_info", None)


def list_stable_apis() -> list[str]:
    return [name for name, info in _api_registry.items() if info.status == APIStatus.STABLE]


def list_experimental_apis() -> list[str]:
    return [name for name, info in _api_registry.items() if info.status == APIStatus.EXPERIMENTAL]


def list_deprecated_apis() -> list[str]:
    return [name for name, info in _api_registry.items() if info.status == APIStatus.DEPRECATED]


def get_api_registry() -> dict[str, APIInfo]:
    return _api_registry.copy()


__all__ = [
    "APIStatus",
    "APIInfo",
    "stable",
    "experimental",
    "deprecated",
    "internal",
    "get_api_info",
    "list_stable_apis",
    "list_experimental_apis",
    "list_deprecated_apis",
    "get_api_registry",
]

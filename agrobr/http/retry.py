"""Retry com exponential backoff."""

from __future__ import annotations

import asyncio
from functools import wraps
from typing import Any, Awaitable, Callable, Sequence, TypeVar

import httpx
import structlog

from agrobr import constants

logger = structlog.get_logger()
T = TypeVar("T")

RETRIABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
)


async def retry_async(
    func: Callable[[], Awaitable[T]],
    max_attempts: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    retriable_exceptions: Sequence[type[Exception]] = RETRIABLE_EXCEPTIONS,
) -> T:
    """Executa função async com retry exponential backoff."""
    settings = constants.HTTPSettings()
    max_attempts = max_attempts or settings.max_retries
    base_delay = base_delay or settings.retry_base_delay
    max_delay = max_delay or settings.retry_max_delay

    last_exception: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return await func()

        except retriable_exceptions as e:
            last_exception = e
            if attempt < max_attempts - 1:
                delay = min(
                    base_delay * (settings.retry_exponential_base**attempt),
                    max_delay,
                )
                logger.warning(
                    "retry_scheduled",
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    delay_seconds=delay,
                    error=str(e),
                )
                await asyncio.sleep(delay)
            else:
                logger.error(
                    "retry_exhausted",
                    attempts=max_attempts,
                    last_error=str(e),
                )

    if last_exception:
        raise last_exception
    raise RuntimeError("Retry logic error: no exception captured")


def with_retry(
    max_attempts: int | None = None,
    base_delay: float | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
    """Decorator para retry automático."""

    def decorator(func: Callable[..., Awaitable[T]]) -> Callable[..., Awaitable[T]]:
        @wraps(func)
        async def wrapper(*args: Any, **kwargs: Any) -> T:
            return await retry_async(
                lambda: func(*args, **kwargs),
                max_attempts=max_attempts,
                base_delay=base_delay,
            )

        return wrapper

    return decorator


def should_retry_status(status_code: int) -> bool:
    """Verifica se o status code permite retry."""
    return status_code in constants.RETRIABLE_STATUS_CODES

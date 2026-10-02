from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from functools import wraps
from typing import Any, TypeVar

import httpx

from agrobr import _log, constants
from agrobr.exceptions import InvalidParameterError

logger = _log.get_logger(__name__)
T = TypeVar("T")


class RetriableStatusError(httpx.HTTPStatusError):
    """Status retriable detectado pelo client; entra no retry sem capturar
    HTTPStatusError genérico (404/403 de raise_for_status propagam imediato)."""


RETRIABLE_EXCEPTIONS: tuple[type[Exception], ...] = (
    httpx.TimeoutException,
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    RetriableStatusError,
)


def _extract_retry_after(response: httpx.Response) -> float | None:
    raw = response.headers.get("Retry-After")
    if raw is None:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def _apos(tentativas: int) -> str:
    return f"after {tentativas} attempt" if tentativas == 1 else f"after {tentativas} attempts"


def _politica(
    settings: constants.HTTPSettings,
    max_attempts: int | None,
    base_delay: float | None,
    max_delay: float | None,
) -> tuple[int, float, float]:
    """Resolve tentativas e esperas: ``None`` vale o ``HTTPSettings``, ``0`` tentativas vale 1 e negativo é inválido."""
    tentativas = settings.max_retries if max_attempts is None else max_attempts
    base = settings.retry_base_delay if base_delay is None else base_delay
    teto = settings.retry_max_delay if max_delay is None else max_delay
    if tentativas < 0 or base < 0 or teto < 0:
        raise InvalidParameterError(
            "max_attempts, base_delay e max_delay não podem ser negativos "
            f"(recebidos: {tentativas}, {base}, {teto})"
        )
    return max(tentativas, 1), base, teto


async def retry_async(
    func: Callable[[], Awaitable[T]],
    max_attempts: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
    retriable_exceptions: Sequence[type[Exception]] = RETRIABLE_EXCEPTIONS,
) -> T:
    settings = constants.HTTPSettings()
    max_attempts, base_delay, max_delay = _politica(settings, max_attempts, base_delay, max_delay)

    last_exception: Exception | None = None

    for attempt in range(max_attempts):
        try:
            return await func()

        except tuple(retriable_exceptions) as e:
            last_exception = e
            if attempt < max_attempts - 1:
                delay = min(
                    base_delay * (settings.retry_exponential_base**attempt),
                    max_delay,
                )
                if isinstance(e, httpx.HTTPStatusError):
                    retry_after = _extract_retry_after(e.response)
                    if retry_after is not None:
                        delay = min(retry_after, max_delay)
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


async def retry_on_status(
    func: Callable[[], Awaitable[httpx.Response]],
    source: str,
    max_attempts: int | None = None,
    base_delay: float | None = None,
    max_delay: float | None = None,
) -> httpx.Response:
    from agrobr.exceptions import SourceUnavailableError
    from agrobr.http.rate_limiter import RateLimiter

    settings = constants.HTTPSettings()
    _max, _base, _cap = _politica(settings, max_attempts, base_delay, max_delay)

    last_response: httpx.Response | None = None

    for attempt in range(_max):
        try:
            async with RateLimiter.acquire(source):
                response = await func()
        except RETRIABLE_EXCEPTIONS as exc:
            if attempt < _max - 1:
                delay = min(_base * (settings.retry_exponential_base**attempt), _cap)
                logger.warning(
                    f"{source}_retry",
                    attempt=attempt + 1,
                    error=str(exc),
                    delay=delay,
                )
                await asyncio.sleep(delay)
                continue
            raise SourceUnavailableError(
                source=source,
                last_error=f"{type(exc).__name__}: {exc} {_apos(_max)}",
            ) from exc

        if not should_retry_status(response.status_code):
            return response

        last_response = response

        if attempt < _max - 1:
            delay = min(_base * (settings.retry_exponential_base**attempt), _cap)
            retry_after = _extract_retry_after(response)
            if retry_after is not None:
                delay = min(retry_after, _cap)
            logger.warning(
                f"{source}_retry",
                attempt=attempt + 1,
                status=response.status_code,
                delay=delay,
            )
            await asyncio.sleep(delay)

    assert last_response is not None
    logger.error(
        f"{source}_retry_exhausted",
        status=last_response.status_code,
    )
    raise SourceUnavailableError(
        source=source,
        url=str(last_response.url),
        last_error=f"HTTP {last_response.status_code} {_apos(_max)}",
    )


def with_retry(
    max_attempts: int | None = None,
    base_delay: float | None = None,
) -> Callable[[Callable[..., Awaitable[T]]], Callable[..., Awaitable[T]]]:
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
    return status_code in constants.RETRIABLE_STATUS_CODES

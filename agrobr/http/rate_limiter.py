"""Rate limiter por fonte usando semáforos."""

from __future__ import annotations

import asyncio
import time
from contextlib import asynccontextmanager
from typing import AsyncIterator

import structlog

from agrobr import constants

logger = structlog.get_logger()


class RateLimiter:
    """Garante intervalo mínimo entre requests para cada fonte."""

    _semaphores: dict[str, asyncio.Semaphore] = {}
    _last_request: dict[str, float] = {}
    _lock = asyncio.Lock()

    @classmethod
    def _get_delay(cls, source: constants.Fonte) -> float:
        settings = constants.HTTPSettings()
        delays = {
            constants.Fonte.CEPEA: settings.rate_limit_cepea,
            constants.Fonte.CONAB: settings.rate_limit_conab,
            constants.Fonte.IBGE: settings.rate_limit_ibge,
            constants.Fonte.NOTICIAS_AGRICOLAS: settings.rate_limit_noticias_agricolas,
        }
        return delays.get(source, 1.0)

    @classmethod
    @asynccontextmanager
    async def acquire(cls, source: constants.Fonte) -> AsyncIterator[None]:
        """Context manager que garante rate limiting."""
        source_key = source.value

        async with cls._lock:
            if source_key not in cls._semaphores:
                cls._semaphores[source_key] = asyncio.Semaphore(1)

        async with cls._semaphores[source_key]:
            now = time.monotonic()
            last = cls._last_request.get(source_key, 0)
            delay = cls._get_delay(source)
            elapsed = now - last

            if elapsed < delay:
                wait_time = delay - elapsed
                logger.debug(
                    "rate_limit_wait",
                    source=source_key,
                    wait_seconds=wait_time,
                )
                await asyncio.sleep(wait_time)

            try:
                yield
            finally:
                cls._last_request[source_key] = time.monotonic()

    @classmethod
    def reset(cls) -> None:
        """Reseta estado do rate limiter."""
        cls._semaphores.clear()
        cls._last_request.clear()

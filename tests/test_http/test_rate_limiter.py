"""Testes de resiliência para agrobr.http.rate_limiter."""

from __future__ import annotations

import asyncio
import time

import pytest

from agrobr import constants
from agrobr.http.rate_limiter import RateLimiter


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reseta estado do rate limiter antes de cada teste."""
    RateLimiter.reset()
    yield
    RateLimiter.reset()


class TestRateLimiter:
    """Testes para RateLimiter."""

    @pytest.mark.asyncio
    async def test_acquire_allows_first_request_immediately(self):
        start = time.monotonic()
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            pass
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_acquire_enforces_delay_between_requests(self):
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            pass

        start = time.monotonic()
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            pass
        elapsed = time.monotonic() - start

        delay = RateLimiter._get_delay(constants.Fonte.IBGE)
        assert elapsed >= delay * 0.8

    @pytest.mark.asyncio
    async def test_different_sources_independent(self):
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            pass

        start = time.monotonic()
        async with RateLimiter.acquire(constants.Fonte.BCB):
            pass
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_reset_clears_state(self):
        async with RateLimiter.acquire(constants.Fonte.CEPEA):
            pass

        RateLimiter.reset()

        start = time.monotonic()
        async with RateLimiter.acquire(constants.Fonte.CEPEA):
            pass
        elapsed = time.monotonic() - start
        assert elapsed < 0.5

    @pytest.mark.asyncio
    async def test_concurrent_requests_serialized(self):
        order: list[int] = []

        async def task(n: int) -> None:
            async with RateLimiter.acquire(constants.Fonte.ABIOVE):
                order.append(n)

        await asyncio.gather(task(1), task(2), task(3))
        assert len(order) == 3

    def test_get_delay_returns_configured_value(self):
        delay = RateLimiter._get_delay(constants.Fonte.CEPEA)
        settings = constants.HTTPSettings()
        assert delay == settings.rate_limit_cepea

    def test_get_delay_unknown_source_returns_default(self):
        delay = RateLimiter._get_delay("unknown_source")
        assert delay == 1.0

    @pytest.mark.asyncio
    async def test_all_sources_have_delay(self):
        for fonte in constants.Fonte:
            delay = RateLimiter._get_delay(fonte)
            assert delay > 0, f"{fonte} has no delay configured"

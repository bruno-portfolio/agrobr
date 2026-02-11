"""Testes de resiliência para agrobr.http.retry."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.http.retry import (
    RETRIABLE_EXCEPTIONS,
    retry_async,
    should_retry_status,
    with_retry,
)


class TestRetryAsync:
    """Testes para retry_async."""

    @pytest.mark.asyncio
    async def test_success_first_attempt(self):
        func = AsyncMock(return_value="ok")
        result = await retry_async(func, max_attempts=3, base_delay=0.01)
        assert result == "ok"
        assert func.call_count == 1

    @pytest.mark.asyncio
    async def test_success_after_failures(self):
        func = AsyncMock(
            side_effect=[httpx.TimeoutException("t1"), httpx.TimeoutException("t2"), "ok"]
        )
        result = await retry_async(func, max_attempts=3, base_delay=0.01, max_delay=0.02)
        assert result == "ok"
        assert func.call_count == 3

    @pytest.mark.asyncio
    async def test_exhausts_max_retries_raises(self):
        func = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        with pytest.raises(httpx.TimeoutException, match="timeout"):
            await retry_async(func, max_attempts=3, base_delay=0.01, max_delay=0.02)
        assert func.call_count == 3

    @pytest.mark.asyncio
    async def test_network_error_retried(self):
        func = AsyncMock(side_effect=[httpx.NetworkError("net"), "ok"])
        result = await retry_async(func, max_attempts=3, base_delay=0.01)
        assert result == "ok"
        assert func.call_count == 2

    @pytest.mark.asyncio
    async def test_remote_protocol_error_retried(self):
        func = AsyncMock(side_effect=[httpx.RemoteProtocolError("proto"), "ok"])
        result = await retry_async(func, max_attempts=3, base_delay=0.01)
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_non_retriable_exception_propagates_immediately(self):
        func = AsyncMock(side_effect=ValueError("bad"))
        with pytest.raises(ValueError, match="bad"):
            await retry_async(func, max_attempts=3, base_delay=0.01)
        assert func.call_count == 1

    @pytest.mark.asyncio
    async def test_custom_retriable_exceptions(self):
        func = AsyncMock(side_effect=[ValueError("v"), "ok"])
        result = await retry_async(
            func,
            max_attempts=3,
            base_delay=0.01,
            retriable_exceptions=[ValueError],
        )
        assert result == "ok"

    @pytest.mark.asyncio
    async def test_backoff_exponential(self):
        func = AsyncMock(
            side_effect=[
                httpx.TimeoutException("t1"),
                httpx.TimeoutException("t2"),
                "ok",
            ]
        )
        sleep_calls: list[float] = []
        original_sleep = asyncio.sleep

        async def mock_sleep(delay: float) -> None:
            sleep_calls.append(delay)
            await original_sleep(0)

        with patch("agrobr.http.retry.asyncio.sleep", side_effect=mock_sleep):
            result = await retry_async(func, max_attempts=3, base_delay=1.0, max_delay=30.0)

        assert result == "ok"
        assert len(sleep_calls) == 2
        assert sleep_calls[1] > sleep_calls[0]

    @pytest.mark.asyncio
    async def test_max_delay_caps_backoff(self):
        func = AsyncMock(side_effect=[httpx.TimeoutException("t")] * 4 + ["ok"])
        sleep_calls: list[float] = []

        async def mock_sleep(delay: float) -> None:
            sleep_calls.append(delay)

        with patch("agrobr.http.retry.asyncio.sleep", side_effect=mock_sleep):
            await retry_async(func, max_attempts=5, base_delay=1.0, max_delay=5.0)

        for d in sleep_calls:
            assert d <= 5.0

    @pytest.mark.asyncio
    async def test_single_attempt_no_retry(self):
        func = AsyncMock(side_effect=httpx.TimeoutException("t"))
        with pytest.raises(httpx.TimeoutException):
            await retry_async(func, max_attempts=1, base_delay=0.01)
        assert func.call_count == 1


class TestWithRetryDecorator:
    """Testes para o decorator with_retry."""

    @pytest.mark.asyncio
    async def test_decorator_success(self):
        @with_retry(max_attempts=3, base_delay=0.01)
        async def my_func(x: int) -> int:
            return x * 2

        result = await my_func(5)
        assert result == 10

    @pytest.mark.asyncio
    async def test_decorator_retries_on_failure(self):
        call_count = 0

        @with_retry(max_attempts=3, base_delay=0.01)
        async def flaky_func() -> str:
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise httpx.TimeoutException("timeout")
            return "success"

        with patch("agrobr.http.retry.asyncio.sleep", new_callable=AsyncMock):
            result = await flaky_func()

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_decorator_exhausts_retries(self):
        @with_retry(max_attempts=2, base_delay=0.01)
        async def always_fails() -> str:
            raise httpx.NetworkError("down")

        with (
            patch("agrobr.http.retry.asyncio.sleep", new_callable=AsyncMock),
            pytest.raises(httpx.NetworkError),
        ):
            await always_fails()


class TestShouldRetryStatus:
    """Testes para should_retry_status."""

    def test_retriable_codes(self):
        for code in [408, 429, 500, 502, 503, 504]:
            assert should_retry_status(code) is True

    def test_non_retriable_codes(self):
        for code in [200, 201, 301, 400, 401, 403, 404, 405]:
            assert should_retry_status(code) is False


class TestRetriableExceptions:
    """Verifica composição do tuple RETRIABLE_EXCEPTIONS."""

    def test_contains_expected_types(self):
        assert httpx.TimeoutException in RETRIABLE_EXCEPTIONS
        assert httpx.NetworkError in RETRIABLE_EXCEPTIONS
        assert httpx.RemoteProtocolError in RETRIABLE_EXCEPTIONS

    def test_http_status_error_not_retriable_by_default(self):
        assert httpx.HTTPStatusError not in RETRIABLE_EXCEPTIONS

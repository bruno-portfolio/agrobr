"""Testes de resiliência para agrobr.http.retry."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from agrobr.exceptions import SourceUnavailableError
from agrobr.http.retry import (
    RETRIABLE_EXCEPTIONS,
    RetriableStatusError,
    retry_async,
    retry_on_status,
    should_retry_status,
    with_retry,
)
from tests.helpers import RETRY_SLEEP, levanta_exatamente, make_mock_response, sem_excecao


def _status_error(cls, status: int, headers: dict[str, str] | None = None):
    request = httpx.Request("GET", "https://test.local")
    response = httpx.Response(status, request=request, headers=headers)
    return cls(f"status {status}", request=request, response=response)


class TestRetriableStatusError:
    def test_na_tupla_global(self):
        assert RetriableStatusError in RETRIABLE_EXCEPTIONS

    @pytest.mark.asyncio
    async def test_retriable_status_error_is_retried(self):
        func = AsyncMock(side_effect=[_status_error(RetriableStatusError, 500), "ok"])
        result = await retry_async(func, max_attempts=3, base_delay=0.01, max_delay=0.02)
        assert result == "ok"
        assert func.call_count == 2


class TestRetryAsync:
    """Testes para retry_async."""

    @pytest.mark.asyncio
    async def test_exhausts_max_retries_raises(self):
        func = AsyncMock(side_effect=httpx.TimeoutException("timeout"))
        with pytest.raises(httpx.TimeoutException, match="timeout"):
            await retry_async(func, max_attempts=3, base_delay=0.01, max_delay=0.02)
        assert func.call_count == 3

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


class TestWithRetryDecorator:
    """Testes para o decorator with_retry."""

    @pytest.mark.asyncio
    async def test_decorator_success(self):
        @with_retry(max_attempts=3, base_delay=0.01)
        async def my_func(x: int) -> int:
            return x * 2

        result = await my_func(5)
        assert result == 10


class TestShouldRetryStatus:
    """Testes para should_retry_status."""

    def test_retriable_codes(self):
        for code in [408, 429, 500, 502, 503, 504]:
            assert should_retry_status(code) is True


class TestRetriableExceptions:
    """Verifica composição do tuple RETRIABLE_EXCEPTIONS."""

    def test_contains_expected_types(self):
        assert httpx.TimeoutException in RETRIABLE_EXCEPTIONS
        assert httpx.NetworkError in RETRIABLE_EXCEPTIONS
        assert httpx.RemoteProtocolError in RETRIABLE_EXCEPTIONS

    def test_http_status_error_not_retriable_by_default(self):
        assert httpx.HTTPStatusError not in RETRIABLE_EXCEPTIONS


class TestRetryOnStatusTransport:
    @pytest.mark.asyncio
    async def test_transport_exhausted_raises_source_unavailable(self):
        call_count = 0

        async def func() -> httpx.Response:
            nonlocal call_count
            call_count += 1
            raise httpx.TimeoutException("timeout")

        with (
            patch(RETRY_SLEEP, new_callable=AsyncMock),
            pytest.raises(SourceUnavailableError, match="after 3 retries"),
        ):
            await retry_on_status(func, source="test", max_attempts=3)

        assert call_count == 3

    @pytest.mark.asyncio
    async def test_mixed_transport_and_status(self):
        resp_500 = make_mock_response(500)
        resp_ok = make_mock_response(200)
        call_count = 0

        async def func() -> httpx.Response:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise httpx.TimeoutException("timeout")
            if call_count == 2:
                return resp_500
            return resp_ok

        with patch(RETRY_SLEEP, new_callable=AsyncMock):
            result = await retry_on_status(func, source="test", max_attempts=4)

        assert result.status_code == 200
        assert call_count == 3


async def test_retry_async_espera_retry_after_e_depois_backoff():
    erro_429 = _status_error(RetriableStatusError, 429, headers={"Retry-After": "0.5"})
    func = AsyncMock(side_effect=[erro_429, httpx.TimeoutException("t"), "ok"])
    with patch(RETRY_SLEEP, new_callable=AsyncMock) as espera, sem_excecao():
        resultado = await retry_async(func, max_attempts=3, base_delay=2.0, max_delay=10.0)
    assert resultado == "ok"
    assert [chamada.args[0] for chamada in espera.await_args_list] == [0.5, 4.0]


async def test_retry_async_sem_nova_tentativa_nao_espera():
    func = AsyncMock(side_effect=httpx.TimeoutException("t"))
    with (
        patch(RETRY_SLEEP, new_callable=AsyncMock) as espera,
        levanta_exatamente(httpx.TimeoutException),
    ):
        await retry_async(func, max_attempts=1, base_delay=0.01)
    espera.assert_not_awaited()


async def test_retry_on_status_espera_retry_after_e_depois_backoff():
    respostas = [
        make_mock_response(503, headers={"Retry-After": "0.5"}),
        make_mock_response(500),
        make_mock_response(200),
    ]
    func = AsyncMock(side_effect=respostas)
    with (
        patch(RETRY_SLEEP, new_callable=AsyncMock) as espera,
        patch("agrobr.http.rate_limiter._async_sleep", new_callable=AsyncMock),
    ):
        resposta = await retry_on_status(
            func, source="teste", max_attempts=3, base_delay=2.0, max_delay=10.0
        )
    assert resposta.status_code == 200
    assert [chamada.args[0] for chamada in espera.await_args_list] == [0.5, 4.0]

"""Testes de resiliência para agrobr.http.rate_limiter."""

from __future__ import annotations

import asyncio
import time
import warnings
from types import SimpleNamespace

import pytest

from agrobr import constants
from agrobr.http import rate_limiter
from agrobr.http.rate_limiter import RateLimiter


@pytest.fixture(autouse=True)
def _reset_rate_limiter():
    """Reseta estado do rate limiter antes de cada teste."""
    RateLimiter.reset()
    yield
    RateLimiter.reset()


class TestRateLimiter:
    @pytest.mark.asyncio
    @pytest.mark.parametrize("elapsed", [0.0, 0.25, 1.0, 1.5])
    async def test_acquire_enforces_delay_between_requests(self, monkeypatch, elapsed):
        clock = SimpleNamespace(now=100.0)
        waits: list[float] = []

        async def controlled_sleep(delay: float) -> None:
            waits.append(delay)
            clock.now += delay

        monkeypatch.setenv("AGROBR_HTTP_RATE_LIMIT_IBGE", "1.0")
        monkeypatch.setattr(rate_limiter, "time", SimpleNamespace(monotonic=lambda: clock.now))
        monkeypatch.setattr(rate_limiter, "_async_sleep", controlled_sleep)

        async with RateLimiter.acquire(constants.Fonte.IBGE):
            clock.now += 0.5
        assert waits == []
        assert RateLimiter._last_request["ibge"] == 100.5

        clock.now += elapsed
        async with RateLimiter.acquire(constants.Fonte.IBGE):
            pass

        assert waits == ([1.0 - elapsed] if elapsed < 1.0 else [])
        assert RateLimiter._last_request["ibge"] == 100.5 + max(elapsed, 1.0)

    def test_get_delay_unknown_source_returns_default(self):
        delay = RateLimiter._get_delay("unknown_source")
        settings = constants.HTTPSettings()
        assert delay == settings.rate_limit_default


async def test_reset_esquece_intervalo_e_semaforo_por_fonte():
    async with RateLimiter.acquire("fonte_teste"):
        pass
    assert "fonte_teste" in RateLimiter._last_request
    assert "fonte_teste" in RateLimiter._next_slot
    assert "fonte_teste" in RateLimiter._vagas
    RateLimiter.reset()
    assert RateLimiter._last_request == {}
    assert RateLimiter._next_slot == {}
    assert RateLimiter._vagas == {}
    assert dict(RateLimiter._semaphores) == {}


async def test_concorrencia_por_fonte_respeita_o_maximo(monkeypatch):
    monkeypatch.setattr(
        constants,
        "HTTPSettings",
        lambda: SimpleNamespace(max_concurrent_default=1, rate_limit_default=0.0),
    )
    ativos = 0
    pico = 0

    async def usar():
        nonlocal ativos, pico
        async with RateLimiter.acquire("fonte_concorrente"):
            ativos += 1
            pico = max(pico, ativos)
            await asyncio.sleep(0.01)
            ativos -= 1

    await asyncio.gather(usar(), usar(), usar())
    assert pico == 1


async def test_vaga_do_processo_tem_o_maximo_e_espera_com_teto(monkeypatch):
    monkeypatch.setenv("AGROBR_HTTP_TIMEOUT_READ", "0.2")
    vaga = RateLimiter._vaga("fonte_presa")
    assert vaga.acquire(blocking=False)

    async def pedir():
        async with RateLimiter.acquire("fonte_presa"):
            return time.monotonic()

    try:
        assert not vaga.acquire(blocking=False)
        with warnings.catch_warnings(record=True) as avisos:
            warnings.simplefilter("always")
            inicio = time.monotonic()
            try:
                fim = await asyncio.wait_for(pedir(), timeout=5)
            except TimeoutError:
                fim = None
    finally:
        vaga.release()

    assert fim is not None, "a espera pela vaga presa não tem teto"
    assert fim - inicio >= 0.15
    assert [aviso for aviso in avisos if "vaga de fonte_presa" in str(aviso.message)]


async def test_reserva_espaca_os_inicios_com_concorrencia(monkeypatch):
    monkeypatch.setattr(
        constants,
        "HTTPSettings",
        lambda: SimpleNamespace(max_concurrent_default=2, rate_limit_default=0.3),
    )
    inicios: list[float] = []

    async def usar():
        async with RateLimiter.acquire("fonte_dupla"):
            inicios.append(time.monotonic())
            await asyncio.sleep(0.4)

    await asyncio.gather(usar(), usar())
    primeiro, segundo = sorted(inicios)
    assert segundo - primeiro >= 0.25

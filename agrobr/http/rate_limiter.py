from __future__ import annotations

import asyncio
import threading
import time
import weakref
from asyncio import sleep as _async_sleep
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import structlog

from agrobr import constants
from agrobr.utils.warnings import warn_once

logger = structlog.get_logger()

_VAGA_POLL_SECONDS = 0.05


class RateLimiter:
    """Intervalo e concorrência por fonte valem no processo inteiro, entre loops e threads.

    O horário do último pedido, a reserva do próximo e as vagas por fonte ficam num estado do
    processo protegido por ``threading.Lock``; só os semáforos do asyncio são criados por loop.
    A vaga entre threads é disputada sem bloquear o loop e tem teto (``timeout_read``): uma
    tarefa do loop travado pelo ``agrobr.sync`` pode segurar a vaga, e sem teto seria um impasse.
    """

    _estado = threading.Lock()
    _semaphores: weakref.WeakKeyDictionary[
        asyncio.AbstractEventLoop, dict[str, asyncio.Semaphore]
    ] = weakref.WeakKeyDictionary()
    _vagas: dict[str, threading.BoundedSemaphore] = {}
    _last_request: dict[str, float] = {}
    _next_slot: dict[str, float] = {}

    @classmethod
    def _get_delay(cls, source_key: str) -> float:
        settings = constants.HTTPSettings()
        return getattr(settings, f"rate_limit_{source_key}", settings.rate_limit_default)

    @classmethod
    def _max_concurrent(cls, source_key: str) -> int:
        settings = constants.HTTPSettings()
        return getattr(settings, f"max_concurrent_{source_key}", settings.max_concurrent_default)

    @classmethod
    def _loop_semaphore(cls, source_key: str) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        with cls._estado:
            if loop not in cls._semaphores:
                cls._semaphores[loop] = {}
            por_fonte = cls._semaphores[loop]
            if source_key not in por_fonte:
                por_fonte[source_key] = asyncio.Semaphore(cls._max_concurrent(source_key))
            return por_fonte[source_key]

    @classmethod
    def _vaga(cls, source_key: str) -> threading.BoundedSemaphore:
        with cls._estado:
            if source_key not in cls._vagas:
                cls._vagas[source_key] = threading.BoundedSemaphore(cls._max_concurrent(source_key))
            return cls._vagas[source_key]

    @classmethod
    async def _ocupar(cls, vaga: threading.BoundedSemaphore, source_key: str) -> bool:
        if vaga.acquire(blocking=False):
            return True
        teto = constants.HTTPSettings().timeout_read
        prazo = time.monotonic() + teto
        while time.monotonic() < prazo:
            await _async_sleep(_VAGA_POLL_SECONDS)
            if vaga.acquire(blocking=False):
                return True
        warn_once(
            f"rate_limit_sem_vaga:{source_key}",
            f"agrobr: a vaga de {source_key} ficou ocupada por mais de {teto:g} s, em geral por uma "
            "tarefa do loop que chamou o agrobr.sync; o pedido segue sem vaga, e o intervalo entre "
            "pedidos continua valendo. Dentro de um loop, prefira o await na API async.",
        )
        return False

    @classmethod
    async def _aguardar_horario(cls, source_key: str) -> None:
        delay = cls._get_delay(source_key)
        with cls._estado:
            now = time.monotonic()
            start = max(
                now,
                cls._last_request.get(source_key, float("-inf")) + delay,
                cls._next_slot.get(source_key, float("-inf")) + delay,
            )
            cls._next_slot[source_key] = start
        if start > now:
            logger.debug("rate_limit_wait", source=source_key, wait_seconds=start - now)
            await _async_sleep(start - now)

    @classmethod
    @asynccontextmanager
    async def acquire(cls, source: constants.Fonte | str) -> AsyncIterator[None]:
        source_key = source.value if isinstance(source, constants.Fonte) else source

        async with cls._loop_semaphore(source_key):
            vaga = cls._vaga(source_key)
            ocupada = await cls._ocupar(vaga, source_key)
            try:
                await cls._aguardar_horario(source_key)
                try:
                    yield
                finally:
                    with cls._estado:
                        cls._last_request[source_key] = time.monotonic()
            finally:
                if ocupada:
                    vaga.release()

    @classmethod
    def reset(cls) -> None:
        with cls._estado:
            cls._semaphores.clear()
            cls._vagas.clear()
            cls._last_request.clear()
            cls._next_slot.clear()

"""Wrappers síncronos para APIs async do agrobr."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


def _get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """
    Obtém event loop existente ou cria novo.

    Trata casos especiais:
    - Jupyter notebooks (loop já rodando)
    - Threads secundárias (sem loop default)
    """
    try:
        loop = asyncio.get_running_loop()
        try:
            import nest_asyncio

            nest_asyncio.apply()
            return loop
        except ImportError:
            raise RuntimeError(
                "Event loop already running. Install nest_asyncio for Jupyter support: "
                "pip install nest_asyncio"
            ) from None
    except RuntimeError:
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop


def run_sync(coro: Awaitable[T]) -> T:
    """
    Executa coroutine de forma síncrona.

    Args:
        coro: Coroutine a executar

    Returns:
        Resultado da coroutine
    """
    loop = _get_or_create_event_loop()

    if loop.is_running():
        import nest_asyncio

        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)


def sync_wrapper(async_func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    """
    Decorator que cria versão síncrona de função async.

    Usage:
        @sync_wrapper
        async def fetch_data():
            ...

        # Agora pode chamar:
        fetch_data()  # Síncrono
    """

    @functools.wraps(async_func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return run_sync(async_func(*args, **kwargs))

    if wrapper.__doc__:
        wrapper.__doc__ = f"[SYNC] {wrapper.__doc__}"

    return wrapper


class _SyncModule:
    """Módulo que expõe versões síncronas da API."""

    def __init__(self, async_module: Any) -> None:
        self._async_module = async_module

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._async_module, name)

        if asyncio.iscoroutinefunction(attr):
            return sync_wrapper(attr)

        return attr


class _SyncCepea(_SyncModule):
    """API síncrona do CEPEA."""

    pass


class _SyncConab(_SyncModule):
    """API síncrona da CONAB."""

    pass


class _SyncIbge(_SyncModule):
    """API síncrona do IBGE."""

    pass


_cepea: _SyncCepea | None = None
_conab: _SyncConab | None = None
_ibge: _SyncIbge | None = None


def __getattr__(name: str) -> Any:
    """Lazy loading para evitar imports circulares."""
    global _cepea, _conab, _ibge

    if name == "cepea":
        if _cepea is None:
            from agrobr import cepea as async_cepea

            _cepea = _SyncCepea(async_cepea)
        return _cepea
    elif name == "conab":
        if _conab is None:
            from agrobr import conab as async_conab

            _conab = _SyncConab(async_conab)
        return _conab
    elif name == "ibge":
        if _ibge is None:
            from agrobr import ibge as async_ibge

            _ibge = _SyncIbge(async_ibge)
        return _ibge

    raise AttributeError(f"module 'agrobr.sync' has no attribute '{name}'")

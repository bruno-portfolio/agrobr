"""Wrappers síncronos para APIs async do agrobr."""

from __future__ import annotations

import asyncio
import functools
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


def _get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    """
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
        return asyncio.run(coro)  # type: ignore[arg-type]


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


class _SyncAnpDiesel(_SyncModule):
    """API sincrona da ANP Diesel."""

    pass


class _SyncMapaPsr(_SyncModule):
    """API sincrona do MAPA PSR."""

    pass


class _SyncAlt:
    """Container sincrono para fontes alt data."""

    def __init__(self) -> None:
        self._modules: dict[str, _SyncModule | None] = {
            "anp_diesel": None,
            "mapa_psr": None,
        }
        self._classes: dict[str, type[_SyncModule]] = {
            "anp_diesel": _SyncAnpDiesel,
            "mapa_psr": _SyncMapaPsr,
        }

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._modules:
            raise AttributeError(f"'_SyncAlt' has no attribute '{name}'")

        if self._modules[name] is None:
            import importlib

            async_module = importlib.import_module(f"agrobr.alt.{name}")
            cls = self._classes[name]
            self._modules[name] = cls(async_module)

        return self._modules[name]


class _SyncAbiove(_SyncModule):
    """API síncrona da ABIOVE."""

    pass


class _SyncCepea(_SyncModule):
    """API síncrona do CEPEA."""

    pass


class _SyncConab(_SyncModule):
    """API síncrona da CONAB."""

    pass


class _SyncDeral(_SyncModule):
    """API síncrona do DERAL."""

    pass


class _SyncIbge(_SyncModule):
    """API síncrona do IBGE."""

    pass


class _SyncDatasets(_SyncModule):
    """API síncrona dos datasets."""

    pass


class _SyncImea(_SyncModule):
    """API síncrona do IMEA."""

    pass


class _SyncInmet(_SyncModule):
    """API síncrona do INMET."""

    pass


class _SyncBcb(_SyncModule):
    """API síncrona do BCB/SICOR."""

    pass


class _SyncComexstat(_SyncModule):
    """API síncrona do ComexStat."""

    pass


class _SyncAnda(_SyncModule):
    """API síncrona da ANDA."""

    pass


class _SyncAntaq(_SyncModule):
    """API síncrona da ANTAQ."""

    pass


class _SyncNasaPower(_SyncModule):
    """API síncrona do NASA POWER."""

    pass


class _SyncDesmatamento(_SyncModule):
    """API síncrona do Desmatamento PRODES/DETER."""

    pass


class _SyncQueimadas(_SyncModule):
    """API síncrona do Queimadas/INPE."""

    pass


class _SyncUsda(_SyncModule):
    """API síncrona do USDA."""

    pass


class _SyncB3(_SyncModule):
    """API síncrona da B3."""

    pass


class _SyncComtrade(_SyncModule):
    """API síncrona do UN Comtrade."""

    pass


class _SyncMapBiomas(_SyncModule):
    """API síncrona do MapBiomas."""

    pass


_modules: dict[str, _SyncModule | None] = {
    "abiove": None,
    "anda": None,
    "antaq": None,
    "b3": None,
    "bcb": None,
    "cepea": None,
    "comexstat": None,
    "comtrade": None,
    "conab": None,
    "datasets": None,
    "deral": None,
    "desmatamento": None,
    "ibge": None,
    "imea": None,
    "inmet": None,
    "mapbiomas": None,
    "nasa_power": None,
    "queimadas": None,
    "usda": None,
}

_MODULE_CLASSES: dict[str, type[_SyncModule]] = {
    "abiove": _SyncAbiove,
    "anda": _SyncAnda,
    "antaq": _SyncAntaq,
    "b3": _SyncB3,
    "bcb": _SyncBcb,
    "cepea": _SyncCepea,
    "comexstat": _SyncComexstat,
    "comtrade": _SyncComtrade,
    "conab": _SyncConab,
    "datasets": _SyncDatasets,
    "deral": _SyncDeral,
    "desmatamento": _SyncDesmatamento,
    "ibge": _SyncIbge,
    "imea": _SyncImea,
    "inmet": _SyncInmet,
    "mapbiomas": _SyncMapBiomas,
    "nasa_power": _SyncNasaPower,
    "queimadas": _SyncQueimadas,
    "usda": _SyncUsda,
}


_alt_instance: _SyncAlt | None = None


def __getattr__(name: str) -> Any:
    """Lazy loading para evitar imports circulares."""
    global _alt_instance

    if name == "alt":
        if _alt_instance is None:
            _alt_instance = _SyncAlt()
        return _alt_instance

    if name not in _modules:
        raise AttributeError(f"module 'agrobr.sync' has no attribute '{name}'")

    if _modules[name] is None:
        import importlib

        async_module = importlib.import_module(f"agrobr.{name}")
        cls = _MODULE_CLASSES[name]
        _modules[name] = cls(async_module)

    return _modules[name]

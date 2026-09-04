from __future__ import annotations

import asyncio
import functools
import inspect
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

T = TypeVar("T")


def _get_or_create_event_loop() -> asyncio.AbstractEventLoop:
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        try:
            return asyncio.get_event_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            return loop

    try:
        import nest_asyncio

        nest_asyncio.apply()
        return loop
    except ImportError:
        raise RuntimeError(
            "Event loop already running. Install nest_asyncio for Jupyter support: "
            "pip install nest_asyncio"
        ) from None


def run_sync(coro: Awaitable[T]) -> T:
    loop = _get_or_create_event_loop()

    if loop.is_running():
        import nest_asyncio

        nest_asyncio.apply()
        return loop.run_until_complete(coro)
    else:
        return asyncio.run(coro)  # type: ignore[arg-type]


def sync_wrapper(async_func: Callable[..., Awaitable[T]]) -> Callable[..., T]:
    @functools.wraps(async_func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        return run_sync(async_func(*args, **kwargs))

    if wrapper.__doc__:
        wrapper.__doc__ = f"[SYNC] {wrapper.__doc__}"

    return wrapper


class _SyncModule:
    def __init__(self, async_module: Any) -> None:
        self._async_module = async_module

    def __getattr__(self, name: str) -> Any:
        attr = getattr(self._async_module, name)

        if inspect.iscoroutinefunction(attr):
            return sync_wrapper(attr)

        return attr


class _SyncAlt:
    def __init__(self) -> None:
        self._modules: dict[str, _SyncModule | None] = {
            "anp_diesel": None,
            "antt_pedagio": None,
            "mapa_psr": None,
            "sicar": None,
        }

    def __getattr__(self, name: str) -> Any:
        if name.startswith("_"):
            raise AttributeError(name)
        if name not in self._modules:
            raise AttributeError(f"'_SyncAlt' has no attribute '{name}'")

        if self._modules[name] is None:
            import importlib

            async_module = importlib.import_module(f"agrobr.alt.{name}")
            self._modules[name] = _SyncModule(async_module)

        return self._modules[name]


_modules: dict[str, _SyncModule | None] = {
    "abiove": None,
    "acervo_fundiario": None,
    "ana": None,
    "anda": None,
    "anec": None,
    "antaq": None,
    "b3": None,
    "bcb": None,
    "cepea": None,
    "cftc": None,
    "comexstat": None,
    "comtrade": None,
    "conab": None,
    "datasets": None,
    "defensivos": None,
    "deral": None,
    "desmatamento": None,
    "embrapa_solos": None,
    "funai": None,
    "ibama": None,
    "ibge": None,
    "icmbio": None,
    "incra": None,
    "imea": None,
    "inmet": None,
    "lista_suja": None,
    "mapbiomas": None,
    "mapbiomas_alerta": None,
    "nasa_power": None,
    "noticias_agricolas": None,
    "queimadas": None,
    "rio_verde": None,
    "rnc": None,
    "sfb": None,
    "sicar": None,
    "unica": None,
    "usda": None,
    "zarc": None,
}

_alt_instance: _SyncAlt | None = None


def __getattr__(name: str) -> Any:
    global _alt_instance

    if name == "alt":
        if _alt_instance is None:
            _alt_instance = _SyncAlt()
        return _alt_instance

    if name not in _modules:
        raise AttributeError(f"module 'agrobr.sync' has no attribute '{name}'")

    if _modules[name] is None:
        import importlib

        path = f"agrobr.alt.{name}" if name == "sicar" else f"agrobr.{name}"
        async_module = importlib.import_module(path)
        _modules[name] = _SyncModule(async_module)

    return _modules[name]

from __future__ import annotations

import asyncio
import threading
import weakref
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from datetime import UTC, datetime, timedelta

from agrobr import constants

from . import catalog

_state_lock = threading.RLock()
_loop_locks: weakref.WeakKeyDictionary[asyncio.AbstractEventLoop, asyncio.Lock] = (
    weakref.WeakKeyDictionary()
)
_catalog: catalog.Catalog | None = None


def now() -> datetime:
    return datetime.now(UTC)


def expires_at(received_at: datetime, *, is_catalog: bool = False) -> datetime:
    ttl = constants.ZARC_CATALOG_TTL_SECONDS if is_catalog else constants.ZARC_CACHE_TTL_SECONDS
    return received_at + timedelta(seconds=ttl)


def clear() -> None:
    global _catalog
    with _state_lock:
        _catalog = None


def get_catalog() -> catalog.Catalog | None:
    with _state_lock:
        if _catalog is None or now() >= expires_at(
            _catalog.acquisition.received_at, is_catalog=True
        ):
            return None
        return _catalog.model_copy(deep=True)


def put_catalog(value: catalog.Catalog) -> None:
    global _catalog
    with _state_lock:
        if _catalog is None or _catalog.acquisition.received_at <= value.acquisition.received_at:
            _catalog = value.model_copy(deep=True)


@asynccontextmanager
async def acquisition_lock() -> AsyncIterator[None]:
    loop = asyncio.get_running_loop()
    with _state_lock:
        for previous_loop in list(_loop_locks):
            if previous_loop.is_closed():
                del _loop_locks[previous_loop]
        lock = _loop_locks.get(loop)
        if lock is None:
            lock = asyncio.Lock()
            _loop_locks[loop] = lock
    async with lock:
        yield

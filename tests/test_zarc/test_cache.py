from __future__ import annotations

import asyncio
from datetime import timedelta

import httpx
import pytest

from agrobr.zarc import acquisition, cache, catalog


@pytest.fixture(autouse=True)
def clean_cache():
    cache.clear()
    yield
    cache.clear()


def captured(value: bytes = b"valid") -> acquisition.HTTPAcquisition:
    request = httpx.Request("GET", "https://example.org/data.csv")
    return acquisition.from_response(
        httpx.Response(200, content=value, request=request), str(request.url)
    )


def test_catalog_has_independent_absolute_one_hour_ttl(monkeypatch):
    resource = captured().resource
    listing = catalog.parse_catalog({"success": True, "result": {"resources": []}}, resource)
    cache.put_catalog(listing)
    expiry = resource.received_at + timedelta(hours=1)
    monkeypatch.setattr(cache, "now", lambda: expiry - timedelta(microseconds=1))
    assert cache.get_catalog() is not None
    monkeypatch.setattr(cache, "now", lambda: expiry)
    assert cache.get_catalog() is None


@pytest.mark.asyncio
async def test_lock_waiter_cancellation_leaves_next_call_available():
    async def enter():
        async with cache.acquisition_lock():
            return True

    async with cache.acquisition_lock():
        waiter = asyncio.create_task(enter())
        await asyncio.sleep(0)
        waiter.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiter
    assert await asyncio.wait_for(enter(), 1)


def test_contended_locks_do_not_bind_later_sync_loops():
    async def exercise():
        async def enter():
            async with cache.acquisition_lock():
                await asyncio.sleep(0)

        await asyncio.gather(enter(), enter())

    asyncio.run(exercise())
    asyncio.run(exercise())

from __future__ import annotations

import asyncio

import httpx
import pytest

from agrobr.alt.antt_pedagio import client


@pytest.mark.asyncio
async def test_closed_session_is_not_reused():
    async with client.session() as outer:
        await outer.aclose()
        async with client.session() as replacement:
            assert replacement is not outer
            assert not replacement.is_closed
    assert replacement.is_closed


@pytest.mark.asyncio
async def test_session_from_other_loop_is_not_reused():
    foreign_loop = asyncio.new_event_loop()
    async with httpx.AsyncClient() as foreign:
        state = client._SESSION.set((foreign_loop, foreign))
        try:
            async with client.session() as owned:
                assert owned is not foreign
            assert not foreign.is_closed
            assert client._SESSION.get() == (foreign_loop, foreign)
        finally:
            client._SESSION.reset(state)
            foreign_loop.close()

from __future__ import annotations

import re

import pytest

from agrobr import conab
from agrobr.conab import client


@pytest.mark.integration
@pytest.mark.asyncio
async def test_list_levantamentos_live():
    levantamentos = await client.list_levantamentos()

    assert levantamentos
    assert all(re.fullmatch(r"\d{4}/\d{2}", item["safra"]) for item in levantamentos)


@pytest.mark.integration
@pytest.mark.asyncio
async def test_safras_soja_live():
    df = await conab.safras("soja")

    assert not df.empty
    assert "uf" in df.columns

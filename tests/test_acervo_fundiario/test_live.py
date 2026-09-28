from __future__ import annotations

import pytest

from agrobr import acervo_fundiario
from agrobr.acervo_fundiario import client
from agrobr.acervo_fundiario.models import BASE_URL, FILENAME_PATTERNS
from agrobr.normalize.regions import UFS_VALIDAS

pytestmark = [pytest.mark.integration, pytest.mark.integration_br]


@pytest.mark.asyncio
async def test_live_sigef_smallest_uf():
    df = await acervo_fundiario.sigef("AC", use_cache=False)
    assert len(df) > 0
    assert "uf" in df.columns
    assert df["uf"].dropna().unique().tolist() == ["AC"]


@pytest.mark.asyncio
async def test_live_snci_smallest_uf():
    df = await acervo_fundiario.snci("PI", use_cache=False)
    assert len(df) >= 0
    assert "uf" in df.columns


@pytest.mark.asyncio
async def test_live_assentamentos_brasil():
    df = await acervo_fundiario.assentamentos(use_cache=False)
    assert len(df) >= 8000
    assert {"MG", "GO", "BA"}.issubset(set(df["uf"].dropna().unique()))


@pytest.mark.asyncio
async def test_live_uf_availability_unchanged():
    from urllib.parse import quote

    import httpx

    async with httpx.AsyncClient(timeout=30.0) as c:
        for tema in ("sigef", "snci"):
            for uf in sorted(UFS_VALIDAS):
                url = BASE_URL + quote(FILENAME_PATTERNS[tema].format(uf=uf))
                r = await c.head(url)
                assert r.status_code == 200, f"{tema.upper()} {uf} regrediu: {r.status_code}"


@pytest.mark.asyncio
async def test_live_head_revalidation_avoids_redownload(tmp_path, monkeypatch):
    monkeypatch.setenv("AGROBR_CACHE_CACHE_DIR", str(tmp_path))
    client._FETCH_LOCKS.clear()

    df1 = await acervo_fundiario.sigef("AC")
    assert len(df1) > 0

    import time

    t0 = time.monotonic()
    df2 = await acervo_fundiario.sigef("AC")
    elapsed = time.monotonic() - t0
    assert len(df1) == len(df2)
    assert elapsed < 5.0

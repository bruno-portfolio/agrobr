from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.rnc import api
from agrobr.rnc import cache as rnc_cache

GOLDEN_DIR = Path(__file__).resolve().parent.parent / "golden_data" / "rnc"


@pytest.fixture(autouse=True)
def _isolated_cache(tmp_path, monkeypatch):
    monkeypatch.setattr(rnc_cache, "_cache_dir", lambda: tmp_path)


def _registradas_bytes():
    return (GOLDEN_DIR / "registradas_sample.csv").read_bytes()


def _protegidas_bytes():
    return (GOLDEN_DIR / "protegidas_sample.csv").read_bytes()


@pytest.mark.asyncio
async def test_registradas_returns_dataframe():
    with patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock:
        mock.return_value = (_registradas_bytes(), "https://example.com")

        from agrobr.rnc import registradas

        df = await registradas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 25
        assert "cultivar" in df.columns


@pytest.mark.asyncio
async def test_registradas_filter_cultivar():
    with patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock:
        mock.return_value = (_registradas_bytes(), "https://example.com")

        from agrobr.rnc import registradas

        df = await registradas(cultivar="Bonella")
        assert len(df) > 0
        assert all("Bonella" in v for v in df["cultivar"].values)


@pytest.mark.asyncio
async def test_registradas_return_meta():
    with patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock:
        mock.return_value = (_registradas_bytes(), "https://example.com")

        from agrobr.rnc import registradas

        result = await registradas(return_meta=True)
        assert isinstance(result, tuple)
        assert len(result) == 2
        df, meta = result
        assert isinstance(df, pd.DataFrame)
        assert meta.source == "rnc"


@pytest.mark.asyncio
async def test_registradas_as_polars():
    polars = pytest.importorskip("polars")
    with patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock:
        mock.return_value = (_registradas_bytes(), "https://example.com")

        from agrobr.rnc import registradas

        df = await registradas(as_polars=True)
        assert isinstance(df, polars.DataFrame)


@pytest.mark.asyncio
async def test_protegidas_returns_dataframe():
    with patch("agrobr.rnc.client.fetch_protegidas", new_callable=AsyncMock) as mock:
        mock.return_value = (_protegidas_bytes(), "https://example.com")

        from agrobr.rnc import protegidas

        df = await protegidas()
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 25
        assert "titular" in df.columns


@pytest.mark.asyncio
async def test_protegidas_filter_situacao():
    with patch("agrobr.rnc.client.fetch_protegidas", new_callable=AsyncMock) as mock:
        mock.return_value = (_protegidas_bytes(), "https://example.com")

        from agrobr.rnc import protegidas

        df = await protegidas(situacao="DEFINITIVA")
        assert isinstance(df, pd.DataFrame)
        for v in df["situacao"].values:
            assert "DEFINITIVA" in v


@pytest.mark.asyncio
async def test_registradas_filter_especie():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_registradas_bytes(), "https://example.com")
        from agrobr.rnc import registradas

        df = await registradas(especie="Abacaxi")
        assert len(df) > 0
        assert all("Abacaxi" in v for v in df["nome_comum"].values)


@pytest.mark.asyncio
async def test_registradas_filter_grupo():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_registradas_bytes(), "https://example.com")
        from agrobr.rnc import registradas

        df = await registradas(grupo="FLORESTAIS")
        assert len(df) > 0
        assert all("FLORESTAIS" in v for v in df["grupo"].values)


@pytest.mark.asyncio
async def test_registradas_filter_mantenedor():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_registradas_bytes(), "https://example.com")
        from agrobr.rnc import registradas

        df = await registradas(mantenedor="IAC")
        assert len(df) > 0
        assert all("IAC" in v for v in df["mantenedor"].values)


@pytest.mark.asyncio
async def test_registradas_filter_mantenedor_literal():
    source = pd.DataFrame({"mantenedor": ["BASF S/A", "BASF S.A"]})
    cached = AsyncMock(return_value=(0, "https://example.com/registradas.csv", source))

    with patch.object(api, "_ensure_registradas", cached):
        df = await api.registradas(mantenedor="BASF S.A")

    assert df["mantenedor"].tolist() == ["BASF S.A"]


@pytest.mark.asyncio
async def test_protegidas_filter_cultivar():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_protegidas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_protegidas_bytes(), "https://example.com")
        from agrobr.rnc import protegidas

        df = await protegidas(cultivar="BRS")
        assert len(df) > 0
        assert all("BRS" in v for v in df["cultivar"].values)


@pytest.mark.asyncio
async def test_protegidas_filter_especie():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_protegidas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_protegidas_bytes(), "https://example.com")
        from agrobr.rnc import protegidas

        df = await protegidas(especie="ALFACE")
        assert len(df) > 0
        assert all("ALFACE" in v for v in df["nome_comum"].values)


@pytest.mark.asyncio
async def test_protegidas_filter_titular():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_protegidas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_protegidas_bytes(), "https://example.com")
        from agrobr.rnc import protegidas

        df = await protegidas(titular="SAKATA")
        assert len(df) > 0
        assert all("SAKATA" in v for v in df["titular"].values)


@pytest.mark.asyncio
async def test_registradas_combined_filters():
    with (
        patch("agrobr.rnc.cache.read_cached", return_value=None),
        patch("agrobr.rnc.client.fetch_registradas", new_callable=AsyncMock) as mock,
    ):
        mock.return_value = (_registradas_bytes(), "https://example.com")
        from agrobr.rnc import registradas

        df = await registradas(especie="Abacate", cultivar="Bonella")
        assert len(df) > 0
        assert all("Abacate" in v for v in df["nome_comum"].values)
        assert all("Bonella" in v for v in df["cultivar"].values)

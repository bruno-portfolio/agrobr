from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.desmatamento import api

PRODES_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "prodes_sample"
DETER_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "deter_sample"


def _prodes_csv_bytes() -> bytes:
    return PRODES_DIR.joinpath("response.csv").read_bytes()


def _deter_csv_bytes() -> bytes:
    return DETER_DIR.joinpath("response.csv").read_bytes()


class TestProdes:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df = await api.prodes(bioma="Cerrado", ano=2022)

        assert len(df) >= 5
        assert "ano" in df.columns
        assert "area_km2" in df.columns
        assert "uf" in df.columns
        assert "bioma" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df, meta = await api.prodes(bioma="Cerrado", ano=2022, return_meta=True)

        assert meta.source == "desmatamento"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None
        assert "terrabrasilis_prodes" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df = await api.prodes(bioma="Cerrado", uf="PA")

        assert len(df) >= 1
        assert (df["uf"] == "PA").all()

    @pytest.mark.asyncio
    async def test_filter_uf_case_insensitive(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df = await api.prodes(bioma="Cerrado", uf="pa")

        assert len(df) >= 1
        assert (df["uf"] == "PA").all()

    @pytest.mark.asyncio
    async def test_empty_filter(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df = await api.prodes(bioma="Cerrado", uf="XX")

        assert len(df) == 0


class TestDeter:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ):
            df = await api.deter(bioma="Amazônia")

        assert len(df) >= 5
        assert "data" in df.columns
        assert "area_km2" in df.columns
        assert "uf" in df.columns
        assert "classe" in df.columns
        assert "bioma" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ):
            df, meta = await api.deter(bioma="Amazônia", return_meta=True)

        assert meta.source == "desmatamento"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None
        assert "terrabrasilis_deter" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_classe(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ):
            df = await api.deter(bioma="Amazônia", classe="DESMATAMENTO_CR")

        assert len(df) >= 1
        assert (df["classe"] == "DESMATAMENTO_CR").all()

    @pytest.mark.asyncio
    async def test_filter_classe_degradacao(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ):
            df = await api.deter(bioma="Amazônia", classe="DEGRADACAO")

        assert len(df) >= 1
        assert (df["classe"] == "DEGRADACAO").all()

    @pytest.mark.asyncio
    async def test_empty_filter(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ):
            df = await api.deter(bioma="Amazônia", classe="CLASSE_INEXISTENTE")

        assert len(df) == 0

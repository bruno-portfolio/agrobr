from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.mapbiomas import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "mapbiomas"


def _golden_xlsx() -> bytes:
    return GOLDEN_DIR.joinpath("biome_state_sample.xlsx").read_bytes()


class TestCobertura:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura()

        assert len(df) >= 20
        assert "bioma" in df.columns
        assert "estado" in df.columns
        assert "classe_id" in df.columns
        assert "classe" in df.columns
        assert "ano" in df.columns
        assert "area_ha" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df, meta = await api.cobertura(return_meta=True)

        assert meta.source == "mapbiomas"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None
        assert "mapbiomas_dataverse" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_bioma(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(bioma="Cerrado")

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_filter_bioma_normalized(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(bioma="cerrado")

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_filter_estado(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(estado="AC")

        assert len(df) >= 1
        assert (df["estado"].str.upper() == "AC").all()

    @pytest.mark.asyncio
    async def test_filter_ano(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(ano=2020)

        assert len(df) >= 1
        assert (df["ano"] == 2020).all()

    @pytest.mark.asyncio
    async def test_filter_classe_id(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(classe_id=3)

        assert len(df) >= 1
        assert (df["classe_id"] == 3).all()

    @pytest.mark.asyncio
    async def test_empty_filter(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(estado="XX")

        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_combined_filters(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(bioma="Cerrado", estado="GO", ano=2020)

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()
        assert (df["estado"].str.upper() == "GO").all()
        assert (df["ano"] == 2020).all()


class TestTransicao:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao()

        assert len(df) >= 20
        assert "bioma" in df.columns
        assert "estado" in df.columns
        assert "classe_de_id" in df.columns
        assert "classe_de" in df.columns
        assert "classe_para_id" in df.columns
        assert "classe_para" in df.columns
        assert "periodo" in df.columns
        assert "area_ha" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df, meta = await api.transicao(return_meta=True)

        assert meta.source == "mapbiomas"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert "mapbiomas_dataverse" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_bioma(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao(bioma="Cerrado")

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_filter_periodo(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao(periodo="1985-2024")

        assert len(df) >= 1
        assert (df["periodo"] == "1985-2024").all()

    @pytest.mark.asyncio
    async def test_filter_classe_de_id(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao(classe_de_id=0)

        assert len(df) >= 1
        assert (df["classe_de_id"] == 0).all()

    @pytest.mark.asyncio
    async def test_filter_classe_para_id(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao(classe_para_id=3)

        assert len(df) >= 1
        assert (df["classe_para_id"] == 3).all()

    @pytest.mark.asyncio
    async def test_empty_filter(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.transicao(estado="XX")

        assert len(df) == 0

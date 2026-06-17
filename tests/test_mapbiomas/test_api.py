from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.mapbiomas import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "mapbiomas"


def _golden_xlsx() -> bytes:
    return GOLDEN_DIR.joinpath("biome_state_sample", "response.xlsx").read_bytes()


def _make_municipal_xlsx() -> bytes:
    data = {
        "biome": ["Amazônia", "Amazônia", "Cerrado", "Cerrado"],
        "state": ["Pará", "Pará", "Goiás", "Goiás"],
        "municipality": ["Belém", "Marabá", "Goiânia", "Anápolis"],
        "class": [3, 15, 3, 15],
        "class_level_0": ["Natural", "Antropic", "Natural", "Antropic"],
        2020: [100.0, 200.0, 150.0, 250.0],
        2021: [110.0, 190.0, 160.0, 240.0],
    }
    df = pd.DataFrame(data)
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="COVERAGE_10", index=False)
    return buf.getvalue()


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


class TestCoberturaMunicipal:
    @pytest.mark.asyncio
    async def test_nivel_municipio_calls_municipal_fetch(self):
        xlsx_bytes = _make_municipal_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_municipality",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test_mun.xlsx"),
        ) as mock_fetch:
            df = await api.cobertura(nivel="municipio")

        mock_fetch.assert_called_once()
        assert "municipio" in df.columns
        assert len(df) == 8

    @pytest.mark.asyncio
    async def test_nivel_estado_calls_state_fetch(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test.xlsx"),
        ) as mock_fetch:
            df = await api.cobertura(nivel="estado")

        mock_fetch.assert_called_once()
        assert "municipio" not in df.columns

    @pytest.mark.asyncio
    async def test_filter_municipio(self):
        xlsx_bytes = _make_municipal_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_municipality",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test_mun.xlsx"),
        ):
            df = await api.cobertura(nivel="municipio", municipio="Belém")

        assert len(df) >= 1
        assert (df["municipio"].str.contains("Belém")).all()

    @pytest.mark.asyncio
    async def test_filter_municipio_combined(self):
        xlsx_bytes = _make_municipal_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_municipality",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test_mun.xlsx"),
        ):
            df = await api.cobertura(nivel="municipio", estado="PA", municipio="Belém", ano=2020)

        assert len(df) >= 1
        assert (df["estado"].str.upper() == "PA").all()
        assert (df["municipio"].str.contains("Belém")).all()
        assert (df["ano"] == 2020).all()

    @pytest.mark.asyncio
    async def test_return_meta_municipal(self):
        xlsx_bytes = _make_municipal_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_municipality",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test_mun.xlsx"),
        ):
            df, meta = await api.cobertura(nivel="municipio", return_meta=True)

        assert meta.source == "mapbiomas"
        assert meta.records_count == len(df)
        assert "municipio" in meta.columns

    @pytest.mark.asyncio
    async def test_municipio_filter_no_match(self):
        xlsx_bytes = _make_municipal_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state_municipality",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://data.mapbiomas.org/test_mun.xlsx"),
        ):
            df = await api.cobertura(nivel="municipio", municipio="CidadeInexistente")

        assert len(df) == 0


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


class TestValidacaoColecao:
    @pytest.mark.asyncio
    async def test_cobertura_colecao_invalida(self):
        with pytest.raises(ValueError, match="nao suportada"):
            await api.cobertura(colecao=9)

    @pytest.mark.asyncio
    async def test_transicao_colecao_invalida(self):
        with pytest.raises(ValueError, match="nao suportada"):
            await api.transicao(colecao=9)

    @pytest.mark.asyncio
    async def test_cobertura_colecao_atual_passa_validacao(self):
        xlsx_bytes = _golden_xlsx()
        with patch.object(
            api.client,
            "fetch_biome_state",
            new_callable=AsyncMock,
            return_value=(xlsx_bytes, "https://storage.googleapis.com/mapbiomas-public/test.xlsx"),
        ):
            df = await api.cobertura(colecao=10)
        assert len(df) >= 1

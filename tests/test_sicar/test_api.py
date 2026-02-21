"""Testes para agrobr.alt.sicar.api."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.alt.sicar import api
from agrobr.alt.sicar.api import _build_cql_filter, imoveis, resumo
from agrobr.alt.sicar.models import COLUNAS_IMOVEIS

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "sicar"


def _load_golden_pages(name: str) -> list[bytes]:
    csv_path = GOLDEN_DIR / name / "response.csv"
    return [csv_path.read_bytes()]


class TestBuildCqlFilter:
    def test_no_filters(self):
        assert _build_cql_filter() is None

    def test_municipio_ilike(self):
        result = _build_cql_filter(municipio="Sorriso")
        assert "municipio ILIKE '%Sorriso%'" in result

    def test_status_filter(self):
        result = _build_cql_filter(status="AT")
        assert "status_imovel='AT'" in result

    def test_tipo_filter(self):
        result = _build_cql_filter(tipo="IRU")
        assert "tipo_imovel='IRU'" in result

    def test_area_min(self):
        result = _build_cql_filter(area_min=100.0)
        assert "area>=100.0" in result

    def test_area_max(self):
        result = _build_cql_filter(area_max=500.0)
        assert "area<=500.0" in result

    def test_criado_apos(self):
        result = _build_cql_filter(criado_apos="2020-01-01")
        assert "dat_criacao>='2020-01-01'" in result

    def test_compound_filter(self):
        result = _build_cql_filter(municipio="Sorriso", status="AT", area_min=100.0)
        assert " AND " in result
        assert "municipio ILIKE" in result
        assert "status_imovel" in result
        assert "area>=" in result

    def test_municipio_escaping(self):
        result = _build_cql_filter(municipio="It's a test")
        assert "It''s a test" in result


class TestImoveis:
    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF"):
            await imoveis("XX")

    @pytest.mark.asyncio
    async def test_invalid_status_raises(self):
        with pytest.raises(ValueError, match="Status"):
            await imoveis("DF", status="INVALID")

    @pytest.mark.asyncio
    async def test_invalid_tipo_raises(self):
        with pytest.raises(ValueError, match="Tipo"):
            await imoveis("DF", tipo="XYZ")

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_returns_dataframe(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=5,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
        ):
            df = await imoveis("DF")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 10
        for col in COLUNAS_IMOVEIS:
            assert col in df.columns

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_return_meta(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=5,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
        ):
            df, meta = await imoveis("DF", return_meta=True)

        assert meta.source == "sicar"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.source_method == "httpx+wfs+csv"

    @pytest.mark.asyncio
    async def test_empty_result(self):
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=([], "https://test.url"),
            ),
        ):
            df = await imoveis("DF")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    @pytest.mark.asyncio
    async def test_uf_case_insensitive(self):
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=0,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=([], "https://test.url"),
            ),
        ):
            df = await imoveis("df")  # lowercase

        assert isinstance(df, pd.DataFrame)

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_sorted_by_cod_imovel(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=5,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
        ):
            df = await imoveis("DF")

        cods = df["cod_imovel"].tolist()
        assert cods == sorted(cods)


class TestResumo:
    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF"):
            await resumo("XX")

    @pytest.mark.asyncio
    async def test_uf_level_mode(self):
        with patch.object(
            api.client,
            "fetch_hits",
            new_callable=AsyncMock,
            side_effect=[1000, 600, 200, 150, 50],
        ):
            df = await resumo("DF")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["total"].iloc[0] == 1000
        assert df["ativos"].iloc[0] == 600
        assert df["pendentes"].iloc[0] == 200
        assert df["suspensos"].iloc[0] == 150
        assert df["cancelados"].iloc[0] == 50

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_mt_municipio" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_municipio_mode(self):
        pages = _load_golden_pages("imoveis_mt_municipio")
        with patch.object(
            api.client,
            "fetch_imoveis",
            new_callable=AsyncMock,
            return_value=(pages, "https://test.url"),
        ):
            df = await resumo("MT", municipio="SORRISO")

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["total"].iloc[0] == 10
        assert df["ativos"].iloc[0] == 10
        assert df["pendentes"].iloc[0] == 0

    @pytest.mark.asyncio
    async def test_return_meta(self):
        with patch.object(
            api.client,
            "fetch_hits",
            new_callable=AsyncMock,
            side_effect=[100, 60, 20, 15, 5],
        ):
            df, meta = await resumo("DF", return_meta=True)

        assert meta.source == "sicar"
        assert meta.records_count == 1

    @pytest.mark.asyncio
    async def test_uf_case_insensitive(self):
        with patch.object(
            api.client,
            "fetch_hits",
            new_callable=AsyncMock,
            side_effect=[0, 0, 0, 0, 0],
        ):
            df = await resumo("df")  # lowercase
        assert isinstance(df, pd.DataFrame)

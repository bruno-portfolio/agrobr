"""Testes para agrobr.alt.sicar.api."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.alt.sicar import api
from agrobr.alt.sicar.api import _build_cql_filter, imoveis, imoveis_geo, resumo
from agrobr.alt.sicar.models import COLUNAS_IMOVEIS, COLUNAS_IMOVEIS_GEO

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "sicar"


def call_args_cql(mock: AsyncMock) -> str:
    args = mock.call_args
    return args[0][1] if len(args[0]) > 1 else args[1].get("cql_filter", "")


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

    def test_cod_municipio_filter(self):
        result = _build_cql_filter(cod_municipio=1508159)
        assert "cod_municipio_ibge=1508159" in result

    def test_cod_municipio_ignores_municipio(self):
        result = _build_cql_filter(cod_municipio=1508159)
        assert "ILIKE" not in result

    def test_municipio_and_cod_municipio_prefers_cod(self):
        result = _build_cql_filter(cod_municipio=1508159, municipio=None)
        assert "cod_municipio_ibge=1508159" in result


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
    async def test_municipio_and_cod_municipio_raises(self):
        with pytest.raises(ValueError, match="municipio.*cod_municipio"):
            await imoveis("DF", municipio="Brasilia", cod_municipio=5300108)

    @pytest.mark.asyncio
    async def test_cod_municipio_filter(self):
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
            ) as mock_fetch,
        ):
            await imoveis("PA", cod_municipio=1508159)

        cql = mock_fetch.call_args[0][1]
        assert "cod_municipio_ibge=1508159" in cql

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


class TestImoveisLargeQueryWarning:
    def _warning_events(self, mock_logger) -> list[str]:
        return [c.args[0] for c in mock_logger.warning.call_args_list]

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_warns_when_total_exceeds_threshold(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=200_000),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis("MT")

        assert "sicar_large_query" in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_no_warning_when_total_below_threshold(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=10),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis("DF")

        assert "sicar_large_query" not in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_skips_preflight_check_when_municipio_filter(self):
        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock) as mock_hits,
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
        ):
            await imoveis("DF", municipio="Brasilia")

        mock_hits.assert_not_called()

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_df_sample" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_hit_count_check_source_unavailable_is_logged_and_swallowed(self):
        from agrobr.exceptions import SourceUnavailableError

        pages = _load_golden_pages("imoveis_df_sample")
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="sicar", last_error="TLS handshake"),
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=(pages, "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            df = await imoveis("DF")

        assert len(df) == 10
        assert "sicar_hit_count_check_failed" in self._warning_events(mock_logger)


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

    @pytest.mark.asyncio
    async def test_municipio_and_cod_municipio_raises(self):
        with pytest.raises(ValueError, match="municipio.*cod_municipio"):
            await resumo("MT", municipio="Sorriso", cod_municipio=5107925)

    @pytest.mark.asyncio
    @pytest.mark.skipif(
        not (GOLDEN_DIR / "imoveis_mt_municipio" / "response.csv").exists(),
        reason="No golden data",
    )
    async def test_cod_municipio_mode(self):
        pages = _load_golden_pages("imoveis_mt_municipio")
        with patch.object(
            api.client,
            "fetch_imoveis",
            new_callable=AsyncMock,
            return_value=(pages, "https://test.url"),
        ) as mock_fetch:
            df = await resumo("MT", cod_municipio=5107925)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        cql = mock_fetch.call_args[0][1]
        assert "cod_municipio_ibge=5107925" in cql


DUPLICATE_CSV = (
    b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
    b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
    b"s.1,PA-001,AT,2014-06-15T10:30:00Z,2023-01-10T14:20:00Z,"
    b"120.5,,PA,BELEM,1501402,5.0,IRU\n"
    b"s.2,PA-001,AT,2014-06-15T10:30:00Z,2022-05-01T10:00:00Z,"
    b"120.5,,PA,BELEM,1501402,5.0,IRU\n"
    b"s.3,PA-002,PE,2018-03-22T08:15:00Z,,"
    b"45.2,,PA,BELEM,1501402,1.8,IRU\n"
)

NULL_DATA_CRIACAO_CSV = (
    b"FID,cod_imovel,status_imovel,dat_criacao,data_atualizacao,"
    b"area,condicao,uf,municipio,cod_municipio_ibge,m_fiscal,tipo_imovel\n"
    b"s.1,PA-001,AT,,2023-01-10T14:20:00Z,"
    b"120.5,,PA,BELEM,1501402,5.0,IRU\n"
    b"s.2,PA-002,PE,2018-03-22T08:15:00Z,,"
    b"45.2,,PA,BELEM,1501402,1.8,IRU\n"
)


class TestImoveisDedup:
    @pytest.mark.asyncio
    async def test_dedup_removes_duplicates(self):
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=3,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=([DUPLICATE_CSV], "https://test.url"),
            ),
        ):
            df = await imoveis("PA")

        assert len(df) == 2
        assert df["cod_imovel"].is_unique

    @pytest.mark.asyncio
    async def test_dedup_keeps_most_recent(self):
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=3,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=([DUPLICATE_CSV], "https://test.url"),
            ),
        ):
            df = await imoveis("PA")

        pa001 = df[df["cod_imovel"] == "PA-001"]
        assert len(pa001) == 1
        assert pa001["data_atualizacao"].iloc[0].year == 2023


def _load_golden_geojson() -> bytes:
    geo_path = GOLDEN_DIR / "imoveis_geo_sample" / "response.geojson"
    return geo_path.read_bytes()


class TestImoveisGeo:
    gpd = pytest.importorskip("geopandas")

    @pytest.mark.asyncio
    async def test_returns_geodataframe(self):
        import geopandas

        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=10),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
        ):
            gdf = await imoveis_geo("DF")

        assert isinstance(gdf, geopandas.GeoDataFrame)
        assert len(gdf) == 10
        for col in COLUNAS_IMOVEIS_GEO:
            assert col in gdf.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=10),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
        ):
            gdf, meta = await imoveis_geo("DF", return_meta=True)

        assert meta.source == "sicar"
        assert meta.records_count == len(gdf)
        assert meta.source_method == "httpx+wfs+geojson"
        assert meta.selected_source == "sicar_wfs_geo"

    @pytest.mark.asyncio
    async def test_filter_municipio(self):
        geojson = _load_golden_geojson()
        mock_fetch = AsyncMock(return_value=([geojson], "https://test.url"))
        with patch.object(api.client, "fetch_imoveis_geo", mock_fetch):
            await imoveis_geo("DF", municipio="Brasilia")

        call_args = mock_fetch.call_args
        cql = call_args[0][1] if len(call_args[0]) > 1 else call_args[1].get("cql_filter")
        assert cql is not None
        assert "municipio ILIKE" in cql

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF"):
            await imoveis_geo("XX")

    @pytest.mark.asyncio
    async def test_municipio_and_cod_municipio_raises(self):
        with pytest.raises(ValueError, match="municipio.*cod_municipio"):
            await imoveis_geo("DF", municipio="Brasilia", cod_municipio=5300108)

    @pytest.mark.asyncio
    async def test_cod_municipio_filter(self):
        geojson = _load_golden_geojson()
        mock_fetch = AsyncMock(return_value=([geojson], "https://test.url"))
        with patch.object(api.client, "fetch_imoveis_geo", mock_fetch):
            await imoveis_geo("PA", cod_municipio=1508159)

        cql = call_args_cql(mock_fetch)
        assert "cod_municipio_ibge=1508159" in cql

    @pytest.mark.asyncio
    async def test_dedup_by_cod_imovel(self):
        import json

        geojson_data = json.loads(_load_golden_geojson())
        geojson_data["features"].append(geojson_data["features"][0])
        data = json.dumps(geojson_data).encode()

        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=11),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([data], "https://test.url"),
            ),
        ):
            gdf = await imoveis_geo("DF")

        assert gdf["cod_imovel"].is_unique

    @pytest.mark.asyncio
    async def test_empty_result(self):
        import geopandas

        empty = b'{"type":"FeatureCollection","features":[]}'
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=0),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([empty], "https://test.url"),
            ),
        ):
            gdf = await imoveis_geo("DF")

        assert isinstance(gdf, geopandas.GeoDataFrame)
        assert len(gdf) == 0


class TestImoveisGeoLargeQueryWarning:
    gpd = pytest.importorskip("geopandas")

    def _warning_events(self, mock_logger) -> list[str]:
        return [c.args[0] for c in mock_logger.warning.call_args_list]

    @pytest.mark.asyncio
    async def test_warns_when_unbounded_total_exceeds_threshold(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=200_000),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis_geo("MT", max_features=None)

        assert "sicar_geo_large_query" in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    async def test_no_warning_when_max_features_caps_below_threshold(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=200_000),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis_geo("MT", max_features=5_000)

        assert "sicar_geo_large_query" not in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    async def test_warns_when_max_features_itself_exceeds_threshold(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=500_000),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis_geo("MT", max_features=150_000)

        assert "sicar_geo_large_query" in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    async def test_no_warning_when_total_below_threshold(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock, return_value=10),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            await imoveis_geo("DF", max_features=None)

        assert "sicar_geo_large_query" not in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    async def test_skips_preflight_check_when_municipio_filter(self):
        geojson = _load_golden_geojson()
        with (
            patch.object(api.client, "fetch_hits", new_callable=AsyncMock) as mock_hits,
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
        ):
            await imoveis_geo("MT", municipio="Sorriso", max_features=None)

        mock_hits.assert_not_called()

    @pytest.mark.asyncio
    async def test_hit_count_check_httpx_error_is_logged_and_swallowed(self):
        import httpx

        geojson = _load_golden_geojson()
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                side_effect=httpx.ConnectError("boom"),
            ),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            gdf = await imoveis_geo("MT", max_features=None)

        assert len(gdf) == 10
        assert "sicar_geo_hit_count_check_failed" in self._warning_events(mock_logger)

    @pytest.mark.asyncio
    async def test_hit_count_check_source_unavailable_is_logged_and_swallowed(self):
        from agrobr.exceptions import SourceUnavailableError

        geojson = _load_golden_geojson()
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                side_effect=SourceUnavailableError(source="sicar", last_error="TLS handshake"),
            ),
            patch.object(
                api.client,
                "fetch_imoveis_geo",
                new_callable=AsyncMock,
                return_value=([geojson], "https://test.url"),
            ),
            patch.object(api, "logger") as mock_logger,
        ):
            gdf = await imoveis_geo("MT", max_features=None)

        assert len(gdf) == 10
        assert "sicar_geo_hit_count_check_failed" in self._warning_events(mock_logger)


class TestImoveisNullDataCriacao:
    @pytest.mark.asyncio
    async def test_null_data_criacao_accepted(self):
        with (
            patch.object(
                api.client,
                "fetch_hits",
                new_callable=AsyncMock,
                return_value=2,
            ),
            patch.object(
                api.client,
                "fetch_imoveis",
                new_callable=AsyncMock,
                return_value=([NULL_DATA_CRIACAO_CSV], "https://test.url"),
            ),
        ):
            df = await imoveis("PA")

        assert len(df) == 2
        assert pd.isna(df[df["cod_imovel"] == "PA-001"]["data_criacao"].iloc[0])

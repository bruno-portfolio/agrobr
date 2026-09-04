from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.icmbio import api

UCS_DIR = Path(__file__).parent.parent / "golden_data" / "icmbio" / "ucs_sample"
UCS_GEO_DIR = Path(__file__).parent.parent / "golden_data" / "icmbio" / "ucs_geo_sample"


def _ucs_csv_bytes() -> bytes:
    return UCS_DIR.joinpath("response.csv").read_bytes()


def _ucs_geojson_bytes() -> bytes:
    return UCS_GEO_DIR.joinpath("response.geojson").read_bytes()


class TestUcs:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        csv_bytes = _ucs_csv_bytes()
        with patch.object(
            api.client,
            "fetch_ucs",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows"),
        ):
            df = await api.ucs()

        assert len(df) == 10
        assert "codigo" in df.columns
        assert "nome" in df.columns
        assert "area_ha" in df.columns
        assert "grupo" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _ucs_csv_bytes()
        with patch.object(
            api.client,
            "fetch_ucs",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows"),
        ):
            df, meta = await api.ucs(return_meta=True)

        assert meta.source == "icmbio"
        assert meta.source_method == "httpx+wfs+csv"
        assert meta.records_count == len(df)
        assert meta.parser_version == 2
        assert meta.fetch_timestamp is not None
        assert "icmbio_wfs" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.ucs(uf="INVALID")

    @pytest.mark.asyncio
    async def test_invalid_grupo_raises(self):
        with pytest.raises(ValueError, match="Grupo invalido"):
            await api.ucs(grupo="XX")

    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_fetch(self):
        with (
            patch.object(api.client, "fetch_ucs", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await api.ucs(bioma="Cerrado'")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_bbox_raises(self):
        with pytest.raises(ValueError, match="BBOX"):
            await api.ucs(bbox=(10.0, 20.0, 5.0, 15.0))

    @pytest.mark.asyncio
    async def test_valid_uf_passes(self):
        csv_bytes = _ucs_csv_bytes()
        with patch.object(
            api.client,
            "fetch_ucs",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows"),
        ):
            df = await api.ucs(uf="MT")

        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_valid_grupo_passes(self):
        csv_bytes = _ucs_csv_bytes()
        with patch.object(
            api.client,
            "fetch_ucs",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows"),
        ):
            df = await api.ucs(grupo="PI")

        assert len(df) >= 1

    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        csv_bytes = _ucs_csv_bytes()
        with patch.object(
            api.client,
            "fetch_ucs",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows"),
        ):
            result = await api.ucs(as_polars=True)

        assert isinstance(result, pl.DataFrame)


gpd = pytest.importorskip("geopandas")


class TestUcsGeo:
    @pytest.mark.asyncio
    async def test_returns_geodataframe(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo()

        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) >= 10
        assert "codigo" in gdf.columns
        assert "geometry" in gdf.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf, meta = await api.ucs_geo(return_meta=True)

        assert meta.source == "icmbio"
        assert meta.source_method == "httpx+wfs+geojson"
        assert meta.records_count == len(gdf)
        assert "icmbio_wfs_geo" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.ucs_geo(uf="INVALID")

    @pytest.mark.asyncio
    async def test_invalid_grupo_raises(self):
        with pytest.raises(ValueError, match="Grupo invalido"):
            await api.ucs_geo(grupo="XX")

    @pytest.mark.asyncio
    async def test_client_receives_only_bbox(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ) as mock_fetch:
            await api.ucs_geo(
                uf="MT", grupo="PI", bioma="Cerrado", bbox=(-60.0, -15.0, -50.0, -10.0)
            )

        call_kwargs = mock_fetch.call_args[1]
        assert "uf" not in call_kwargs
        assert "grupo" not in call_kwargs
        assert "bioma" not in call_kwargs
        assert call_kwargs["bbox"] == (-60.0, -15.0, -50.0, -10.0)

    @pytest.mark.asyncio
    async def test_post_filter_by_uf_contains(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(uf="MT")

        assert len(gdf) == 7
        assert gdf["uf"].str.contains("MT").all()

    @pytest.mark.asyncio
    async def test_post_filter_by_uf_exact(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(uf="DF")

        assert len(gdf) == 2

    @pytest.mark.asyncio
    async def test_post_filter_by_grupo(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(grupo="US")

        assert len(gdf) == 4
        assert (gdf["grupo"] == "US").all()

    @pytest.mark.asyncio
    async def test_post_filter_by_bioma(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(bioma="Pantanal")

        assert len(gdf) == 1

    @pytest.mark.asyncio
    async def test_post_filter_combined(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(uf="MT", grupo="PI", bioma="Cerrado")

        assert len(gdf) >= 1
        assert gdf["uf"].str.contains("MT").all()
        assert (gdf["grupo"] == "PI").all()
        assert (gdf["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_post_filter_no_match_returns_empty(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo(uf="AC")

        assert len(gdf) == 0
        assert isinstance(gdf, gpd.GeoDataFrame)

    @pytest.mark.asyncio
    async def test_post_filter_meta_reflects_filtered_count(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf, meta = await api.ucs_geo(grupo="US", return_meta=True)

        assert len(gdf) == 4
        assert meta.records_count == 4

    @pytest.mark.asyncio
    async def test_geometry_preserved(self):
        geojson_bytes = _ucs_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_ucs_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://geoservicos.inde.gov.br/geoserver/ICMBio/ows",
            ),
        ):
            gdf = await api.ucs_geo()

        assert "geometry" in gdf.columns
        assert gdf.geometry.is_valid.all()

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.desmatamento import api
from agrobr.exceptions import InvalidParameterError

PRODES_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "prodes_sample"
PRODES_GEO_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "prodes_geo_sample"
DETER_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "deter_sample"
DETER_GEO_DIR = Path(__file__).parent.parent / "golden_data" / "desmatamento" / "deter_geo_sample"


def _prodes_csv_bytes() -> bytes:
    return PRODES_DIR.joinpath("response.csv").read_bytes()


def _deter_csv_bytes() -> bytes:
    return DETER_DIR.joinpath("response.csv").read_bytes()


def _prodes_geojson_bytes() -> bytes:
    return PRODES_GEO_DIR.joinpath("response.geojson").read_bytes()


def _deter_geojson_bytes() -> bytes:
    return DETER_GEO_DIR.joinpath("response.geojson").read_bytes()


class TestProdes:
    @pytest.mark.parametrize(
        ("fn_name", "fetch_name"),
        [
            ("prodes", "fetch_prodes"),
            ("deter", "fetch_deter"),
            ("prodes_geo", "fetch_prodes_geo"),
            ("deter_geo", "fetch_deter_geo"),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_request(self, fn_name, fetch_name):
        with (
            patch.object(api.client, fetch_name, new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await getattr(api, fn_name)(bioma="Atlantida")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_result_at_limit_emits_filter_warning(self):
        with (
            patch.object(
                api.client,
                "fetch_prodes",
                new_callable=AsyncMock,
                return_value=(b"csv", "https://example.com/prodes.csv"),
            ),
            patch.object(
                api.parser,
                "parse_prodes_csv",
                return_value=pd.DataFrame(index=range(api.client.MAX_FEATURES_PER_REQUEST)),
            ),
            pytest.warns(UserWarning, match="filtre por ano e/ou UF"),
        ):
            await api.prodes()

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
            df = await api.prodes(bioma="Cerrado", uf="MT")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_filter_uf_case_insensitive(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ):
            df = await api.prodes(bioma="Cerrado", uf="mt")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.prodes(bioma="Cerrado", uf="XX")

    @pytest.mark.asyncio
    async def test_bioma_normalization(self):
        csv_bytes = _prodes_csv_bytes()
        with patch.object(
            api.client,
            "fetch_prodes",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.csv"),
        ) as mock_fetch:
            await api.prodes(bioma="cerrado")

        assert mock_fetch.call_args[0][0] == "Cerrado"


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
    async def test_bioma_normalization(self):
        csv_bytes = _deter_csv_bytes()
        with patch.object(
            api.client,
            "fetch_deter",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://terrabrasilis.dpi.inpe.br/geoserver/deter.csv"),
        ) as mock_fetch:
            await api.deter(bioma="amazonia")

        assert mock_fetch.call_args[0][0] == "Amazônia"

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


gpd = pytest.importorskip("geopandas")


class TestProdesGeo:
    @pytest.mark.asyncio
    async def test_bioma_normalization(self):
        geojson_bytes = _prodes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_prodes_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.geojson",
            ),
        ) as mock_fetch:
            await api.prodes_geo(bioma="cerrado")

        assert mock_fetch.call_args[0][0] == "Cerrado"

    @pytest.mark.asyncio
    async def test_returns_geodataframe(self):
        geojson_bytes = _prodes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_prodes_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.geojson",
            ),
        ):
            gdf = await api.prodes_geo(bioma="Cerrado")

        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) >= 5
        assert "ano" in gdf.columns
        assert "area_km2" in gdf.columns
        assert "geometry" in gdf.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        geojson_bytes = _prodes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_prodes_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.geojson",
            ),
        ):
            gdf, meta = await api.prodes_geo(bioma="Cerrado", return_meta=True)

        assert meta.source == "desmatamento"
        assert meta.source_method == "httpx+wfs+geojson"
        assert meta.records_count == len(gdf)
        assert "terrabrasilis_prodes_geo" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        geojson_bytes = _prodes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_prodes_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.geojson",
            ),
        ):
            gdf = await api.prodes_geo(bioma="Cerrado", uf="MA")

        assert len(gdf) >= 1
        assert (gdf["uf"] == "MA").all()

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.prodes_geo(bioma="Cerrado", uf="XX")

    @pytest.mark.asyncio
    async def test_geometry_preserved_after_filter(self):
        geojson_bytes = _prodes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_prodes_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/prodes.geojson",
            ),
        ):
            gdf = await api.prodes_geo(bioma="Cerrado", uf="PA")

        assert "geometry" in gdf.columns
        assert gdf.geometry.is_valid.all()


class TestDeterGeo:
    @pytest.mark.asyncio
    async def test_bioma_normalization(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ) as mock_fetch:
            await api.deter_geo(bioma="amazonia")

        assert mock_fetch.call_args[0][0] == "Amazônia"

    @pytest.mark.asyncio
    async def test_returns_geodataframe(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ):
            gdf = await api.deter_geo(bioma="Amazônia")

        assert isinstance(gdf, gpd.GeoDataFrame)
        assert len(gdf) >= 5
        assert "data" in gdf.columns
        assert "area_km2" in gdf.columns
        assert "geometry" in gdf.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ):
            gdf, meta = await api.deter_geo(bioma="Amazônia", return_meta=True)

        assert meta.source == "desmatamento"
        assert meta.source_method == "httpx+wfs+geojson"
        assert meta.records_count == len(gdf)
        assert "terrabrasilis_deter_geo" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_classe(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ):
            gdf = await api.deter_geo(bioma="Amazônia", classe="DESMATAMENTO_CR")

        assert len(gdf) >= 1
        assert (gdf["classe"] == "DESMATAMENTO_CR").all()

    @pytest.mark.asyncio
    async def test_filter_classe_empty(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ):
            gdf = await api.deter_geo(bioma="Amazônia", classe="CLASSE_INEXISTENTE")

        assert len(gdf) == 0
        assert isinstance(gdf, gpd.GeoDataFrame)

    @pytest.mark.asyncio
    async def test_geometry_preserved_after_filter(self):
        geojson_bytes = _deter_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_deter_geo",
            new_callable=AsyncMock,
            return_value=(
                geojson_bytes,
                "https://terrabrasilis.dpi.inpe.br/geoserver/deter.geojson",
            ),
        ):
            gdf = await api.deter_geo(bioma="Amazônia", classe="DESMATAMENTO_CR")

        assert "geometry" in gdf.columns
        assert gdf.geometry.is_valid.all()

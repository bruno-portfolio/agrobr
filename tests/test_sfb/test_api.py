from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError, SourceUnavailableError
from agrobr.sfb import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "sfb" / "cnfp_sample"


def _geojson_bytes() -> bytes:
    return GOLDEN_DIR.joinpath("response.geojson").read_bytes()


def _arcgis_json_bytes() -> bytes:
    data = json.loads(_geojson_bytes())
    arcgis: dict[str, list[dict[str, object]]] = {"features": []}
    for feat in data["features"]:
        arcgis["features"].append({"attributes": feat["properties"]})
    return json.dumps(arcgis).encode()


def _concessoes_geojson_bytes() -> bytes:
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "fid": 1,
                    "nome_uc": "Flona Jamari",
                    "uf": "RO",
                    "bioma": "Amazonia",
                    "hectares": 220000,
                    "criacao": 1984,
                    "grupo": "Uso Sustentavel",
                    "cat_nome": "FLONA",
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [
                        [[-63.0, -9.0], [-62.8, -9.0], [-62.8, -8.8], [-63.0, -8.8], [-63.0, -9.0]]
                    ],
                },
            },
        ],
    }
    return json.dumps(data).encode()


def _concessoes_arcgis_bytes() -> bytes:
    data = json.loads(_concessoes_geojson_bytes())
    arcgis: dict[str, list[dict[str, object]]] = {"features": []}
    for feat in data["features"]:
        arcgis["features"].append({"attributes": feat["properties"]})
    return json.dumps(arcgis).encode()


def _ifn_geojson_bytes() -> bytes:
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "co_pontos_lote": 1,
                    "co_lote": 10,
                    "no_lote": "Lote A",
                    "no_conglomerado": "CG-01",
                    "no_uf": "PA",
                    "no_municipio": "Belterra",
                    "no_bioma": "Amazonia",
                },
                "geometry": {"type": "Point", "coordinates": [-55.0, -3.0]},
            },
        ],
    }
    return json.dumps(data).encode()


def _ifn_arcgis_bytes() -> bytes:
    data = json.loads(_ifn_geojson_bytes())
    arcgis: dict[str, list[dict[str, object]]] = {"features": []}
    for feat in data["features"]:
        arcgis["features"].append({"attributes": feat["properties"]})
    return json.dumps(arcgis).encode()


class TestCnfp:
    def test_where_escapes_filter_literals(self):
        where = api._build_where(
            "cnfp",
            uf="M'T",
            bioma="Cerrado'",
            categoria="FLONA'",
        )

        assert "uf='M''T'" in where
        assert "bioma='CERRADO'''" in where
        assert "categoria='FLONA'''" in where

    @pytest.mark.asyncio
    async def test_cnfp_returns_dataframe(self):
        arcgis = _arcgis_json_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            df = await api.cnfp()

        assert len(df) == 5
        assert "nome" in df.columns
        assert "uf" in df.columns

    @pytest.mark.asyncio
    async def test_cnfp_return_meta(self):
        arcgis = _arcgis_json_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            df, meta = await api.cnfp(return_meta=True)

        assert meta.source == "sfb"
        assert meta.source_method == "httpx+arcgis+json"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert "sfb_cnfp" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_cnfp_uf_filter(self):
        arcgis = _arcgis_json_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ) as mock_fetch:
            await api.cnfp(uf="PA")

        call_kwargs = mock_fetch.call_args[1]
        assert "uf='PA'" in call_kwargs["where"]

    @pytest.mark.asyncio
    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.cnfp(uf="INVALID")

    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_fetch(self):
        with (
            patch.object(api.client, "fetch_layer", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await api.cnfp(bioma="Cerrado'")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_invalid_categoria_raises_before_fetch(self):
        with (
            patch.object(api.client, "fetch_layer", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Categoria inválida"),
        ):
            await api.cnfp(categoria="x' OR 1=1 OR categoria='")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_cnfp_as_polars(self):
        pl = pytest.importorskip("polars")
        arcgis = _arcgis_json_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            result = await api.cnfp(as_polars=True)

        assert isinstance(result, pl.DataFrame)


class TestCnfpGeo:
    @pytest.fixture(autouse=True)
    def _skip_no_geopandas(self):
        pytest.importorskip("geopandas")

    @pytest.mark.asyncio
    async def test_cnfp_geo_returns_geodataframe(self):
        import geopandas as local_gpd

        geojson = _geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf = await api.cnfp_geo()

        assert isinstance(gdf, local_gpd.GeoDataFrame)
        assert len(gdf) == 5
        assert "geometry" in gdf.columns

    @pytest.mark.asyncio
    async def test_cnfp_geo_return_meta(self):
        geojson = _geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf, meta = await api.cnfp_geo(return_meta=True)

        assert meta.source == "sfb"
        assert meta.source_method == "httpx+arcgis+geojson"
        assert "sfb_cnfp_geo" in meta.attempted_sources


class TestConcessoes:
    @pytest.mark.asyncio
    async def test_concessoes_dataframe(self):
        arcgis = _concessoes_arcgis_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            df = await api.concessoes()

        assert len(df) == 1
        assert {"nome", "uf", "bioma"}.issubset(df.columns)

    @pytest.mark.asyncio
    async def test_concessoes_return_meta(self):
        arcgis = _concessoes_arcgis_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            result = await api.concessoes(return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert meta.source == "sfb"
        assert "sfb_concessoes" in meta.attempted_sources


class TestConcessoesGeo:
    @pytest.mark.asyncio
    async def test_concessoes_geo_returns_geodataframe(self):
        geopandas = pytest.importorskip("geopandas")
        geojson = _concessoes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf = await api.concessoes_geo()

        assert isinstance(gdf, geopandas.GeoDataFrame)

    @pytest.mark.asyncio
    async def test_concessoes_geo_return_meta(self):
        geopandas = pytest.importorskip("geopandas")
        geojson = _concessoes_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf, meta = await api.concessoes_geo(return_meta=True)

        assert isinstance(gdf, geopandas.GeoDataFrame)
        assert meta.source == "sfb"
        assert "sfb_concessoes_geo" in meta.attempted_sources


class TestIfnConglomerados:
    @pytest.mark.asyncio
    async def test_ifn_conglomerados_returns_dataframe(self):
        arcgis = _ifn_arcgis_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            df = await api.ifn_conglomerados()

        assert len(df) == 1
        assert {"uf", "bioma", "conglomerado"}.issubset(df.columns)

    @pytest.mark.asyncio
    async def test_ifn_conglomerados_return_meta(self):
        arcgis = _ifn_arcgis_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([arcgis], "https://mapas.florestal.gov.br/arcgis"),
        ):
            result = await api.ifn_conglomerados(return_meta=True)

        assert isinstance(result, tuple)
        df, meta = result
        assert meta.source == "sfb"
        assert "sfb_ifn_conglomerados" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_ifn_conglomerados_propagates_source_unavailable(self):
        error = SourceUnavailableError(
            source="sfb",
            url="https://mapas.florestal.gov.br/arcgis",
            last_error="ArcGIS error 500: MapServer not started",
        )
        with (
            patch.object(
                api.client,
                "fetch_layer",
                new_callable=AsyncMock,
                side_effect=error,
            ),
            pytest.raises(SourceUnavailableError, match="not started"),
        ):
            await api.ifn_conglomerados()


class TestIfnConglomeradosGeo:
    @pytest.mark.asyncio
    async def test_ifn_conglomerados_geo_returns_geodataframe(self):
        geopandas = pytest.importorskip("geopandas")
        geojson = _ifn_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf = await api.ifn_conglomerados_geo()

        assert isinstance(gdf, geopandas.GeoDataFrame)

    @pytest.mark.asyncio
    async def test_ifn_conglomerados_geo_return_meta(self):
        geopandas = pytest.importorskip("geopandas")
        geojson = _ifn_geojson_bytes()
        with patch.object(
            api.client,
            "fetch_layer",
            new_callable=AsyncMock,
            return_value=([geojson], "https://mapas.florestal.gov.br/arcgis"),
        ):
            gdf, meta = await api.ifn_conglomerados_geo(return_meta=True)

        assert isinstance(gdf, geopandas.GeoDataFrame)
        assert meta.source == "sfb"
        assert "sfb_ifn_conglomerados_geo" in meta.attempted_sources

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
    @pytest.fixture(autouse=True)
    def source_count(self, monkeypatch):
        body = (
            b'<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" numberOfFeatures="10"/>'
        )
        monkeypatch.setattr(
            api.client, "fetch_ucs_count", AsyncMock(return_value=(body, "https://test/hits"))
        )

    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_fetch(self):
        with (
            patch.object(api.client, "fetch_ucs", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="Bioma inválido"),
        ):
            await api.ucs(bioma="Cerrado'")

        fetch.assert_not_awaited()

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
    @pytest.fixture(autouse=True)
    def source_count(self, monkeypatch):
        import json

        count = len(json.loads(_ucs_geojson_bytes())["features"])
        body = (
            '<wfs:FeatureCollection xmlns:wfs="http://www.opengis.net/wfs" '
            f'numberOfFeatures="{count}"/>'
        ).encode()
        monkeypatch.setattr(
            api.client, "fetch_ucs_count", AsyncMock(return_value=(body, "https://test/hits"))
        )

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

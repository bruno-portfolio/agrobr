from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.exceptions import InvalidParameterError
from agrobr.queimadas import api

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "queimadas" / "focos_sample"


def _golden_csv_bytes() -> bytes:
    return GOLDEN_DIR.joinpath("response.csv").read_bytes()


class TestFocos:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9)

        assert len(df) >= 8
        assert "data" in df.columns
        assert "lat" in df.columns
        assert "satelite" in df.columns
        assert "uf" in df.columns

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df, meta = await api.focos(ano=2024, mes=9, return_meta=True)

        assert meta.source == "queimadas"
        assert meta.records_count == len(df)
        assert meta.parser_version == 1
        assert meta.fetch_timestamp is not None
        assert "queimadas" in meta.attempted_sources

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, uf="MT")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_filter_bioma(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, bioma="Cerrado")

        assert len(df) >= 1
        assert (df["bioma"] == "Cerrado").all()

    @pytest.mark.asyncio
    async def test_filter_bioma_without_accent(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2025, mes=1, bioma="Amazonia")

        assert len(df) == 8
        assert (df["bioma"] == "Amazônia").all()

    @pytest.mark.asyncio
    async def test_invalid_bioma_raises_before_fetch(self):
        with (
            patch.object(
                api.client,
                "fetch_focos_mensal",
                new_callable=AsyncMock,
            ) as mock_fetch,
            pytest.raises(ValueError, match="Bioma inválido.*Atlantida"),
        ):
            await api.focos(ano=2025, mes=1, bioma="Atlantida")

        mock_fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_filter_satelite(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, satelite="GOES-16")

        assert len(df) >= 1
        assert (df["satelite"] == "GOES-16").all()

    @pytest.mark.asyncio
    async def test_dia_uses_diario_fetch(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_diario",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ) as mock_diario:
            await api.focos(ano=2024, mes=9, dia=15)

        mock_diario.assert_called_once_with("20240915")

    @pytest.mark.asyncio
    async def test_no_dia_uses_mensal_fetch(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ) as mock_mensal:
            await api.focos(ano=2024, mes=9)

        mock_mensal.assert_called_once_with(2024, 9)

    @pytest.mark.asyncio
    async def test_filter_uf_case_insensitive(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            df = await api.focos(ano=2024, mes=9, uf="mt")

        assert len(df) >= 1
        assert (df["uf"] == "MT").all()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwargs", "message"),
        [
            ({"ano": 2025, "mes": 13}, "mes"),
            ({"ano": 2025, "mes": "1"}, "inteiro"),
            ({"ano": 2025, "mes": 2, "dia": 30}, "data inválida"),
            ({"ano": 9999, "mes": 1}, "ano"),
            ({"ano": 2025, "mes": 1, "uf": "XX"}, "UF invalida"),
        ],
    )
    @pytest.mark.asyncio
    async def test_invalid_parameters_raise_before_fetch(self, kwargs, message):
        with (
            patch.object(
                api.client,
                "fetch_focos_mensal",
                new_callable=AsyncMock,
            ) as monthly,
            patch.object(
                api.client,
                "fetch_focos_diario",
                new_callable=AsyncMock,
            ) as daily,
            pytest.raises(InvalidParameterError, match=message),
        ):
            await api.focos(**kwargs)

        monthly.assert_not_awaited()
        daily.assert_not_awaited()


class TestFocosAsPolars:
    @pytest.mark.asyncio
    async def test_as_polars(self):
        pl = pytest.importorskip("polars")
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            result = await api.focos(ano=2024, mes=9, as_polars=True)
        assert isinstance(result, pl.DataFrame)


class TestFocosGeo:
    @pytest.fixture(autouse=True)
    def _skip_no_geopandas(self):
        pytest.importorskip("geopandas")

    @pytest.mark.asyncio
    async def test_returns_geodataframe(self):
        import geopandas as local_gpd

        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf = await api.focos_geo(ano=2024, mes=9)

        assert isinstance(gdf, local_gpd.GeoDataFrame)
        assert len(gdf) >= 8
        assert "geometry" in gdf.columns

    @pytest.mark.asyncio
    async def test_geometry_is_point(self):
        from shapely.geometry import Point

        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf = await api.focos_geo(ano=2024, mes=9)

        for geom in gdf.geometry:
            assert isinstance(geom, Point)

    @pytest.mark.asyncio
    async def test_crs_4326(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf = await api.focos_geo(ano=2024, mes=9)

        assert gdf.crs.to_epsg() == 4326

    @pytest.mark.asyncio
    async def test_return_meta(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf, meta = await api.focos_geo(ano=2024, mes=9, return_meta=True)

        assert meta.source == "queimadas"
        assert meta.records_count == len(gdf)

    @pytest.mark.asyncio
    async def test_filters_passthrough(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf = await api.focos_geo(ano=2024, mes=9, uf="MT")

        assert len(gdf) >= 1
        assert (gdf["uf"] == "MT").all()

    @pytest.mark.asyncio
    async def test_lat_lon_match_geometry(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv"),
        ):
            gdf = await api.focos_geo(ano=2024, mes=9)

        for _, row in gdf.iterrows():
            assert abs(row.geometry.x - row["lon"]) < 1e-6
            assert abs(row.geometry.y - row["lat"]) < 1e-6

    @pytest.mark.asyncio
    async def test_geopandas_not_installed(self):
        with (
            patch(
                "agrobr.queimadas.api.check_geopandas",
                side_effect=ImportError(
                    "geopandas is required for geo functions. Install with: pip install agrobr[geo]"
                ),
            ),
            pytest.raises(ImportError, match="agrobr\\[geo\\]"),
        ):
            await api.focos_geo(ano=2024, mes=9)

    @pytest.mark.asyncio
    async def test_empty_result(self):
        import geopandas as local_gpd
        import pandas as pd

        empty_df = pd.DataFrame(columns=["data", "lat", "lon", "satelite", "uf", "bioma"])
        with patch.object(
            api,
            "focos",
            new_callable=AsyncMock,
            return_value=empty_df,
        ):
            gdf = await api.focos_geo(ano=2024, mes=9)

        assert len(gdf) == 0
        assert isinstance(gdf, local_gpd.GeoDataFrame)


@pytest.mark.integration
class TestFocosIntegration:
    @pytest.mark.asyncio
    @pytest.mark.slow
    async def test_filter_bioma_without_accent_real(self):
        df = await api.focos(ano=2025, mes=1, uf="MT", bioma="Amazonia")

        assert not df.empty
        assert (df["bioma"] == "Amazônia").all()

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
    async def test_filter_uf_case_insensitive(self):
        csv_bytes = _golden_csv_bytes()
        with patch.object(
            api.client,
            "fetch_focos_mensal",
            new_callable=AsyncMock,
            return_value=(csv_bytes, "https://example.com/focos.csv", csv_bytes, None),
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
            ({"ano": 2025, "mes": 1, "uf": "XX"}, "UF inválida"),
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
            return_value=(csv_bytes, "https://example.com/focos.csv", csv_bytes, None),
        ):
            result = await api.focos(ano=2024, mes=9, as_polars=True)
        assert isinstance(result, pl.DataFrame)


class TestFocosGeo:
    @pytest.fixture(autouse=True)
    def _skip_no_geopandas(self):
        pytest.importorskip("geopandas")

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

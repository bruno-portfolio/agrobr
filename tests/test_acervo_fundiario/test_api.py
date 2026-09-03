from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.acervo_fundiario import api
from agrobr.exceptions import SourceUnavailableError


@pytest.mark.asyncio
class TestSigef:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_pyogrio_ausente_falha_antes_do_download(self, mock_download):
        with (
            patch.object(
                api,
                "check_pyogrio",
                side_effect=ImportError(
                    "pyogrio is required. Install with: pip install agrobr[geo]"
                ),
            ),
            pytest.raises(ImportError, match=r"pip install agrobr\[geo\]"),
        ):
            await api.sigef("ES")

        mock_download.assert_not_awaited()

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_returns_dataframe(self, mock_dl, synthetic_sigef_zip: Path):
        mock_dl.return_value = synthetic_sigef_zip
        df = await api.sigef("ES")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 5
        assert "uf" in df.columns

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_return_meta(self, mock_dl, synthetic_sigef_zip: Path):
        mock_dl.return_value = synthetic_sigef_zip
        df, meta = await api.sigef("ES", return_meta=True)
        assert meta.source == "acervo_fundiario"
        assert meta.source_method == "httpx+pyogrio+shapefile_zip"
        assert meta.records_count == 5
        assert "Sigef Brasil_ES.zip" in meta.source_url

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_as_polars(self, mock_dl, synthetic_sigef_zip: Path):
        pl = pytest.importorskip("polars")
        mock_dl.return_value = synthetic_sigef_zip
        result = await api.sigef("ES", as_polars=True)
        assert isinstance(result, pl.DataFrame)

    async def test_invalid_uf(self):
        with pytest.raises(ValueError, match="UF invalida"):
            await api.sigef("XX")

    async def test_uf_not_in_sigef(self):
        with pytest.raises(SourceUnavailableError, match="nao disponivel em SIGEF"):
            await api.sigef("AP")

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_kwargs_legacy_tipo_ignored(self, mock_dl, synthetic_sigef_zip: Path):
        mock_dl.return_value = synthetic_sigef_zip
        df = await api.sigef("ES", tipo="particular")
        assert isinstance(df, pd.DataFrame)


@pytest.mark.asyncio
class TestSigefGeo:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_returns_geodataframe(self, mock_dl, synthetic_sigef_zip: Path):
        gpd = pytest.importorskip("geopandas")
        mock_dl.return_value = synthetic_sigef_zip
        gdf = await api.sigef_geo("ES")
        assert isinstance(gdf, gpd.GeoDataFrame)

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_return_meta_with_geo(self, mock_dl, synthetic_sigef_zip: Path):
        pytest.importorskip("geopandas")
        mock_dl.return_value = synthetic_sigef_zip
        gdf, meta = await api.sigef_geo("ES", return_meta=True)
        assert meta.source == "acervo_fundiario"
        assert meta.records_count == 5

    async def test_uf_not_in_sigef(self):
        with pytest.raises(SourceUnavailableError):
            await api.sigef_geo("AP")


@pytest.mark.asyncio
class TestSnci:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_returns_dataframe(self, mock_dl, synthetic_snci_zip: Path):
        mock_dl.return_value = synthetic_snci_zip
        df = await api.snci("GO")
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 4

    async def test_uf_not_in_snci(self):
        with pytest.raises(SourceUnavailableError, match="nao disponivel em SNCI"):
            await api.snci("ES")

    async def test_invalid_uf(self):
        with pytest.raises(ValueError):
            await api.snci("XX")


@pytest.mark.asyncio
class TestSnciGeo:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_returns_geodataframe(self, mock_dl, synthetic_snci_zip: Path):
        gpd = pytest.importorskip("geopandas")
        mock_dl.return_value = synthetic_snci_zip
        gdf = await api.snci_geo("GO")
        assert isinstance(gdf, gpd.GeoDataFrame)


@pytest.mark.asyncio
class TestAssentamentos:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_no_uf_returns_all(self, mock_dl, synthetic_assentamentos_zip: Path):
        mock_dl.return_value = synthetic_assentamentos_zip
        df = await api.assentamentos()
        assert len(df) == 4

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_uf_filter(self, mock_dl, synthetic_assentamentos_zip: Path):
        mock_dl.return_value = synthetic_assentamentos_zip
        df = await api.assentamentos(uf="MG")
        assert len(df) == 2
        assert df["uf"].unique().tolist() == ["MG"]

    async def test_invalid_uf_raises(self):
        with pytest.raises(ValueError):
            await api.assentamentos(uf="XX")

    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_return_meta(self, mock_dl, synthetic_assentamentos_zip: Path):
        mock_dl.return_value = synthetic_assentamentos_zip
        df, meta = await api.assentamentos(return_meta=True)
        assert meta.source == "acervo_fundiario"
        assert "Assentamento Brasil.zip" in meta.source_url


@pytest.mark.asyncio
class TestAssentamentosGeo:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_returns_geodataframe(self, mock_dl, synthetic_assentamentos_zip: Path):
        gpd = pytest.importorskip("geopandas")
        mock_dl.return_value = synthetic_assentamentos_zip
        gdf = await api.assentamentos_geo()
        assert isinstance(gdf, gpd.GeoDataFrame)


@pytest.mark.asyncio
class TestLicenseWarning:
    @patch.object(api.client, "download_and_cache", new_callable=AsyncMock)
    async def test_first_call_warns(self, mock_dl, synthetic_sigef_zip: Path):
        mock_dl.return_value = synthetic_sigef_zip
        with pytest.warns(UserWarning, match="vedado o uso comercial"):
            await api.sigef("ES")

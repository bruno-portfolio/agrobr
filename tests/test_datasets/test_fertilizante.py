"""Testes específicos para o dataset fertilizante (fetch com mock)."""

from unittest.mock import AsyncMock, patch

import httpx
import pandas as pd
import pytest

from agrobr.datasets.deterministic import deterministic
from agrobr.datasets.fertilizante import (
    FERTILIZANTE_INFO,
    FertilizanteDataset,
    fertilizante,
)
from agrobr.exceptions import SourceUnavailableError

from .conftest import make_source, mock_source_meta


def _mock_df():
    return pd.DataFrame(
        [
            {
                "ano": 2024,
                "mes": 1,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 150000.0,
            },
            {
                "ano": 2024,
                "mes": 1,
                "uf": "SP",
                "produto_fertilizante": "total",
                "volume_ton": 100000.0,
            },
        ]
    )


class TestFertilizanteInfo:
    def test_license_zona_cinza(self):
        assert FERTILIZANTE_INFO.license == "zona_cinza"

    def test_single_source_anda(self):
        assert len(FERTILIZANTE_INFO.sources) == 1
        assert FERTILIZANTE_INFO.sources[0].name == "anda"

    def test_only_total_product(self):
        assert FERTILIZANTE_INFO.products == ["total"]


class TestFertilizanteFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())
        df = await dataset.fetch("total", ano=2024)

        assert len(df) == 2
        assert "volume_ton" in df.columns
        assert df.iloc[0]["volume_ton"] == 150000.0

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())
        df, meta = await dataset.fetch("total", ano=2024, return_meta=True)

        assert meta.dataset == "fertilizante"
        assert meta.contract_version == "2.0"
        assert "anda" in meta.attempted_sources
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_fetch_invalid_produto(self):
        dataset = FertilizanteDataset()
        with pytest.raises(ValueError, match="não suportado"):
            await dataset.fetch("ureia")

    @pytest.mark.asyncio
    async def test_source_failure(self):
        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(side_effect=httpx.ConnectError("down"))

        with pytest.raises(SourceUnavailableError):
            await dataset.fetch("total")

    @pytest.mark.asyncio
    async def test_snapshot_sets_ano(self):
        dataset = FertilizanteDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2024-06-15"):
            await dataset.fetch("total")

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2024

    @pytest.mark.asyncio
    async def test_snapshot_does_not_override_explicit_ano(self):
        dataset = FertilizanteDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        async with deterministic("2024-06-15"):
            await dataset.fetch("total", ano=2023)

        _, kwargs = mock_fn.call_args
        assert kwargs["ano"] == 2023

    @pytest.mark.asyncio
    async def test_forwards_uf(self):
        dataset = FertilizanteDataset()
        mock_fn = make_source(_mock_df())
        dataset.info.sources[0].fetch_fn = mock_fn

        await dataset.fetch("total", ano=2024, uf="MT")

        _, kwargs = mock_fn.call_args
        assert kwargs["uf"] == "MT"


class TestFertilizanteNormalize:
    @pytest.mark.asyncio
    async def test_normalize_adds_produto_fertilizante(self):
        df = _mock_df().drop(columns=["produto_fertilizante"])
        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = make_source(df)

        result = await dataset.fetch("total", ano=2024)

        assert result["produto_fertilizante"].iloc[0] == "total"

    @pytest.mark.asyncio
    async def test_normalize_keeps_existing_produto_fertilizante(self):
        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = make_source(_mock_df())

        result = await dataset.fetch("total", ano=2024)

        assert result["produto_fertilizante"].iloc[0] == "total"


class TestFertilizantePublicAPI:
    @pytest.mark.asyncio
    async def test_public_function_delegates(self):
        with patch.object(FertilizanteDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await fertilizante("total", ano=2024, uf="MT")

            mock_fetch.assert_called_once_with("total", ano=2024, uf="MT", return_meta=False)

    @pytest.mark.asyncio
    async def test_public_function_default_produto(self):
        with patch.object(FertilizanteDataset, "fetch", new_callable=AsyncMock) as mock_fetch:
            mock_fetch.return_value = _mock_df()
            await fertilizante()

            mock_fetch.assert_called_once_with("total", ano=None, uf=None, return_meta=False)


class TestFertilizanteFetchFunctions:
    @pytest.mark.asyncio
    async def test_fetch_anda_forwards_params(self):
        df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.anda.entregas", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.fertilizante import _fetch_anda

            await _fetch_anda("total", ano=2023, uf="MT")
        mock_fn.assert_called_once_with(2023, produto="total", uf="MT", return_meta=True)

    @pytest.mark.asyncio
    async def test_fetch_anda_defaults_ano_to_current_year(self):
        df = _mock_df()
        meta = mock_source_meta()
        with patch(
            "agrobr.anda.entregas", new_callable=AsyncMock, return_value=(df, meta)
        ) as mock_fn:
            from agrobr.datasets.fertilizante import _fetch_anda

            await _fetch_anda("total")
        _, kwargs = mock_fn.call_args
        assert kwargs["uf"] is None
        assert isinstance(mock_fn.call_args[0][0], int)

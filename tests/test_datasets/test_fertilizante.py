"""Testes específicos para o dataset fertilizante (fetch com mock)."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.fertilizante import FertilizanteDataset


class TestFertilizanteFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        mock_df = pd.DataFrame(
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

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df = await dataset.fetch("total", ano=2024)

        assert len(df) == 2
        assert "volume_ton" in df.columns
        assert df.iloc[0]["volume_ton"] == 150000.0

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        mock_df = pd.DataFrame(
            [
                {
                    "ano": 2024,
                    "mes": 1,
                    "uf": "MT",
                    "produto_fertilizante": "total",
                    "volume_ton": 150000.0,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = FertilizanteDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df, meta = await dataset.fetch("total", ano=2024, return_meta=True)

        assert meta.dataset == "fertilizante"
        assert meta.contract_version == "1.0"
        assert "anda" in meta.attempted_sources
        assert meta.records_count == len(df)

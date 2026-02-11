"""Testes específicos para o dataset custo_producao (fetch com mock)."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.custo_producao import CustoProducaoDataset


class TestCustoProducaoFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        mock_df = pd.DataFrame(
            [
                {
                    "cultura": "soja",
                    "uf": "MT",
                    "safra": "2024/25",
                    "tecnologia": "alta",
                    "categoria": "Sementes",
                    "item": "Semente",
                    "unidade": "kg/ha",
                    "quantidade_ha": 50.0,
                    "preco_unitario": 5.50,
                    "valor_ha": 275.0,
                    "participacao_pct": 8.2,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = CustoProducaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df = await dataset.fetch("soja", safra="2024/25")

        assert len(df) == 1
        assert "valor_ha" in df.columns
        assert df.iloc[0]["valor_ha"] == 275.0

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        mock_df = pd.DataFrame(
            [
                {
                    "cultura": "soja",
                    "uf": "MT",
                    "safra": "2024/25",
                    "tecnologia": "alta",
                    "categoria": "Sementes",
                    "item": "Semente",
                    "unidade": "kg/ha",
                    "quantidade_ha": 50.0,
                    "preco_unitario": 5.50,
                    "valor_ha": 275.0,
                    "participacao_pct": 8.2,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = CustoProducaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df, meta = await dataset.fetch("soja", safra="2024/25", return_meta=True)

        assert meta.dataset == "custo_producao"
        assert meta.contract_version == "1.0"
        assert "conab" in meta.attempted_sources
        assert meta.records_count == len(df)

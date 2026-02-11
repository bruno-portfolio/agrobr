"""Testes específicos para o dataset credito_rural (fetch com mock)."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.credito_rural import CreditoRuralDataset


class TestCreditoRuralFetch:
    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        mock_df = pd.DataFrame(
            [
                {
                    "safra": "2023/2024",
                    "uf": "MT",
                    "produto": "soja",
                    "finalidade": "custeio",
                    "valor": 285431200.0,
                    "area_financiada": 98500.0,
                    "qtd_contratos": 1240,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df = await dataset.fetch("soja", safra="2023/24")

        assert len(df) == 1
        assert "valor" in df.columns
        assert df.iloc[0]["valor"] == 285431200.0

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        mock_df = pd.DataFrame(
            [
                {
                    "safra": "2023/2024",
                    "uf": "MT",
                    "produto": "soja",
                    "finalidade": "custeio",
                    "valor": 285431200.0,
                    "area_financiada": 98500.0,
                    "qtd_contratos": 1240,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = CreditoRuralDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df, meta = await dataset.fetch("soja", safra="2023/24", return_meta=True)

        assert meta.dataset == "credito_rural"
        assert meta.contract_version == "1.0"
        assert "bcb" in meta.attempted_sources
        assert meta.records_count == len(df)

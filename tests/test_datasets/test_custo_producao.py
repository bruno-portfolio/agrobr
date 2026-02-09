"""Testes para o dataset custo_producao."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.custo_producao import CUSTO_PRODUCAO_INFO, CustoProducaoDataset


class TestCustoProducaoInfo:
    def test_info_name(self):
        assert CUSTO_PRODUCAO_INFO.name == "custo_producao"

    def test_info_products(self):
        assert "soja" in CUSTO_PRODUCAO_INFO.products
        assert "milho" in CUSTO_PRODUCAO_INFO.products
        assert "cafe" in CUSTO_PRODUCAO_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in CUSTO_PRODUCAO_INFO.sources]
        assert "conab" in source_names

    def test_info_contract_version(self):
        assert CUSTO_PRODUCAO_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert CUSTO_PRODUCAO_INFO.update_frequency == "yearly"

    def test_info_to_dict(self):
        info_dict = CUSTO_PRODUCAO_INFO.to_dict()
        assert info_dict["name"] == "custo_producao"
        assert "conab" in info_dict["sources"]


class TestCustoProducaoDataset:
    def test_validate_produto_valid(self):
        dataset = CustoProducaoDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = CustoProducaoDataset()
        with pytest.raises(ValueError, match="banana"):
            dataset._validate_produto("banana")

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


class TestCustoProducaoRegistry:
    def test_registered_in_registry(self):
        from agrobr.datasets.registry import list_datasets

        assert "custo_producao" in list_datasets()

    def test_accessible_via_get_dataset(self):
        from agrobr.datasets.registry import get_dataset

        ds = get_dataset("custo_producao")
        assert ds.info.name == "custo_producao"

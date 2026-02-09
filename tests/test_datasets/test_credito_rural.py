"""Testes para o dataset credito_rural."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.credito_rural import CREDITO_RURAL_INFO, CreditoRuralDataset


class TestCreditoRuralInfo:
    def test_info_name(self):
        assert CREDITO_RURAL_INFO.name == "credito_rural"

    def test_info_products(self):
        assert "soja" in CREDITO_RURAL_INFO.products
        assert "milho" in CREDITO_RURAL_INFO.products
        assert "cafe" in CREDITO_RURAL_INFO.products
        assert "cana" in CREDITO_RURAL_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in CREDITO_RURAL_INFO.sources]
        assert "bcb" in source_names

    def test_info_contract_version(self):
        assert CREDITO_RURAL_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert CREDITO_RURAL_INFO.update_frequency == "monthly"

    def test_info_to_dict(self):
        info_dict = CREDITO_RURAL_INFO.to_dict()
        assert info_dict["name"] == "credito_rural"
        assert "bcb" in info_dict["sources"]


class TestCreditoRuralDataset:
    def test_validate_produto_valid(self):
        dataset = CreditoRuralDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = CreditoRuralDataset()
        with pytest.raises(ValueError, match="banana"):
            dataset._validate_produto("banana")

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
        # Patch fetch_fn on the source object directly
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df = await dataset.fetch("soja", safra="2023/24")

        assert len(df) == 1
        assert "valor" in df.columns

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


class TestCreditoRuralRegistry:
    def test_registered_in_registry(self):
        from agrobr.datasets.registry import list_datasets

        assert "credito_rural" in list_datasets()

    def test_accessible_via_get_dataset(self):
        from agrobr.datasets.registry import get_dataset

        ds = get_dataset("credito_rural")
        assert ds.info.name == "credito_rural"

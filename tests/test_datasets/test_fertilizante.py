"""Testes para o dataset fertilizante."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.fertilizante import FERTILIZANTE_INFO, FertilizanteDataset


class TestFertilizanteInfo:
    def test_info_name(self):
        assert FERTILIZANTE_INFO.name == "fertilizante"

    def test_info_products(self):
        assert "total" in FERTILIZANTE_INFO.products
        assert "npk" in FERTILIZANTE_INFO.products
        assert "ureia" in FERTILIZANTE_INFO.products
        assert "kcl" in FERTILIZANTE_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in FERTILIZANTE_INFO.sources]
        assert "anda" in source_names

    def test_info_contract_version(self):
        assert FERTILIZANTE_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert FERTILIZANTE_INFO.update_frequency == "yearly"

    def test_info_to_dict(self):
        info_dict = FERTILIZANTE_INFO.to_dict()
        assert info_dict["name"] == "fertilizante"
        assert "anda" in info_dict["sources"]


class TestFertilizanteDataset:
    def test_validate_produto_valid(self):
        dataset = FertilizanteDataset()
        dataset._validate_produto("total")
        dataset._validate_produto("npk")
        dataset._validate_produto("ureia")

    def test_validate_produto_invalid(self):
        dataset = FertilizanteDataset()
        with pytest.raises(ValueError, match="banana"):
            dataset._validate_produto("banana")

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


class TestFertilizanteRegistry:
    def test_registered_in_registry(self):
        from agrobr.datasets.registry import list_datasets

        assert "fertilizante" in list_datasets()

    def test_accessible_via_get_dataset(self):
        from agrobr.datasets.registry import get_dataset

        ds = get_dataset("fertilizante")
        assert ds.info.name == "fertilizante"

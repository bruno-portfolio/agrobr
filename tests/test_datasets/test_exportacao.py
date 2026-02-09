"""Testes para o dataset exportacao."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.exportacao import EXPORTACAO_INFO, ExportacaoDataset


class TestExportacaoInfo:
    def test_info_name(self):
        assert EXPORTACAO_INFO.name == "exportacao"

    def test_info_products(self):
        assert "soja" in EXPORTACAO_INFO.products
        assert "milho" in EXPORTACAO_INFO.products
        assert "cafe" in EXPORTACAO_INFO.products
        assert "farelo_soja" in EXPORTACAO_INFO.products
        assert "oleo_soja" in EXPORTACAO_INFO.products

    def test_info_sources(self):
        source_names = [s.name for s in EXPORTACAO_INFO.sources]
        assert "comexstat" in source_names
        assert "abiove" in source_names

    def test_info_comexstat_priority(self):
        comexstat = next(s for s in EXPORTACAO_INFO.sources if s.name == "comexstat")
        abiove = next(s for s in EXPORTACAO_INFO.sources if s.name == "abiove")
        assert comexstat.priority < abiove.priority

    def test_info_contract_version(self):
        assert EXPORTACAO_INFO.contract_version == "1.0"

    def test_info_update_frequency(self):
        assert EXPORTACAO_INFO.update_frequency == "monthly"

    def test_info_to_dict(self):
        info_dict = EXPORTACAO_INFO.to_dict()
        assert info_dict["name"] == "exportacao"
        assert "comexstat" in info_dict["sources"]


class TestExportacaoDataset:
    def test_validate_produto_valid(self):
        dataset = ExportacaoDataset()
        dataset._validate_produto("soja")

    def test_validate_produto_invalid(self):
        dataset = ExportacaoDataset()
        with pytest.raises(ValueError, match="banana"):
            dataset._validate_produto("banana")

    @pytest.mark.asyncio
    async def test_fetch_returns_dataframe(self):
        mock_df = pd.DataFrame(
            [
                {
                    "ano": 2024,
                    "mes": 1,
                    "produto": "soja",
                    "uf": "MT",
                    "kg_liquido": 5000000000,
                    "valor_fob_usd": 2500000000,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df = await dataset.fetch("soja", ano=2024)

        assert len(df) == 1
        assert "kg_liquido" in df.columns

    @pytest.mark.asyncio
    async def test_fetch_return_meta(self):
        mock_df = pd.DataFrame(
            [
                {
                    "ano": 2024,
                    "mes": 1,
                    "produto": "soja",
                    "uf": "MT",
                    "kg_liquido": 5000000000,
                    "valor_fob_usd": 2500000000,
                },
            ]
        )

        mock_meta = AsyncMock()
        mock_meta.source_url = "http://test"
        mock_meta.fetched_at = None
        mock_meta.parser_version = 1

        dataset = ExportacaoDataset()
        dataset.info.sources[0].fetch_fn = AsyncMock(return_value=(mock_df, mock_meta))
        df, meta = await dataset.fetch("soja", ano=2024, return_meta=True)

        assert meta.dataset == "exportacao"
        assert meta.contract_version == "1.0"
        assert "comexstat" in meta.attempted_sources
        assert meta.records_count == len(df)


class TestExportacaoRegistry:
    def test_registered_in_registry(self):
        from agrobr.datasets.registry import list_datasets

        assert "exportacao" in list_datasets()

    def test_accessible_via_get_dataset(self):
        from agrobr.datasets.registry import get_dataset

        ds = get_dataset("exportacao")
        assert ds.info.name == "exportacao"

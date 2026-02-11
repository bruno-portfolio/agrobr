"""Testes específicos para o dataset exportacao (fetch com mock + prioridade)."""

from unittest.mock import AsyncMock

import pandas as pd
import pytest

from agrobr.datasets.exportacao import EXPORTACAO_INFO, ExportacaoDataset


class TestExportacaoSpecific:
    def test_info_comexstat_priority(self):
        comexstat = next(s for s in EXPORTACAO_INFO.sources if s.name == "comexstat")
        abiove = next(s for s in EXPORTACAO_INFO.sources if s.name == "abiove")
        assert comexstat.priority < abiove.priority


class TestExportacaoFetch:
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
        assert df.iloc[0]["valor_fob_usd"] == 2500000000

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

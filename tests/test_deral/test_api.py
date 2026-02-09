"""Testes para a API pública DERAL."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.deral import api


def _mock_parsed_df():
    """DataFrame de condição das lavouras mockado."""
    return pd.DataFrame(
        [
            {
                "produto": "soja",
                "data": "15/01/2025",
                "condicao": "boa",
                "pct": 70.0,
                "plantio_pct": None,
                "colheita_pct": None,
            },
            {
                "produto": "soja",
                "data": "15/01/2025",
                "condicao": "media",
                "pct": 25.0,
                "plantio_pct": None,
                "colheita_pct": None,
            },
            {
                "produto": "soja",
                "data": "15/01/2025",
                "condicao": "ruim",
                "pct": 5.0,
                "plantio_pct": None,
                "colheita_pct": None,
            },
            {
                "produto": "milho",
                "data": "15/01/2025",
                "condicao": "boa",
                "pct": 60.0,
                "plantio_pct": None,
                "colheita_pct": None,
            },
            {
                "produto": "milho",
                "data": "15/01/2025",
                "condicao": "media",
                "pct": 30.0,
                "plantio_pct": None,
                "colheita_pct": None,
            },
            {
                "produto": "soja",
                "data": "15/01/2025",
                "condicao": "",
                "pct": None,
                "plantio_pct": 98.0,
                "colheita_pct": None,
            },
        ]
    )


class TestCondicaoLavouras:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        mock_fn = AsyncMock(return_value=b"fake_xls")
        with (
            patch.object(api.client, "fetch_pc_xls", mock_fn),
            patch.object(api.parser, "parse_pc_xls", return_value=_mock_parsed_df()),
        ):
            df = await api.condicao_lavouras()

        assert len(df) == 6
        assert "produto" in df.columns
        assert "condicao" in df.columns
        assert "pct" in df.columns

    @pytest.mark.asyncio
    async def test_filter_produto(self):
        mock_fn = AsyncMock(return_value=b"fake_xls")
        with (
            patch.object(api.client, "fetch_pc_xls", mock_fn),
            patch.object(api.parser, "parse_pc_xls", return_value=_mock_parsed_df()),
        ):
            df = await api.condicao_lavouras("soja")

        assert len(df) == 4
        assert all(df["produto"] == "soja")

    @pytest.mark.asyncio
    async def test_return_meta(self):
        mock_fn = AsyncMock(return_value=b"fake_xls")
        with (
            patch.object(api.client, "fetch_pc_xls", mock_fn),
            patch.object(api.parser, "parse_pc_xls", return_value=_mock_parsed_df()),
        ):
            df, meta = await api.condicao_lavouras(return_meta=True)

        assert meta.source == "deral"
        assert meta.attempted_sources == ["deral"]
        assert meta.selected_source == "deral"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)
        assert "PC.xls" in meta.source_url

    @pytest.mark.asyncio
    async def test_empty_response(self):
        mock_fn = AsyncMock(return_value=b"fake_xls")
        empty_df = pd.DataFrame(
            columns=["produto", "data", "condicao", "pct", "plantio_pct", "colheita_pct"]
        )
        with (
            patch.object(api.client, "fetch_pc_xls", mock_fn),
            patch.object(api.parser, "parse_pc_xls", return_value=empty_df),
        ):
            df = await api.condicao_lavouras()

        assert df.empty
        assert "produto" in df.columns

    @pytest.mark.asyncio
    async def test_no_filter_returns_all(self):
        mock_fn = AsyncMock(return_value=b"fake_xls")
        with (
            patch.object(api.client, "fetch_pc_xls", mock_fn),
            patch.object(api.parser, "parse_pc_xls", return_value=_mock_parsed_df()),
        ):
            df = await api.condicao_lavouras()

        produtos = df["produto"].unique().tolist()
        assert "soja" in produtos
        assert "milho" in produtos

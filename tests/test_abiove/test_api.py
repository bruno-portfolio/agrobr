"""Testes para a API pública ABIOVE."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.abiove import api


def _mock_parsed_df():
    """DataFrame de exportação mockado."""
    return pd.DataFrame(
        [
            {
                "ano": 2024,
                "mes": 1,
                "produto": "grao",
                "volume_ton": 5000000.0,
                "receita_usd_mil": 2500000.0,
            },
            {
                "ano": 2024,
                "mes": 1,
                "produto": "farelo",
                "volume_ton": 2000000.0,
                "receita_usd_mil": 800000.0,
            },
            {
                "ano": 2024,
                "mes": 1,
                "produto": "oleo",
                "volume_ton": 200000.0,
                "receita_usd_mil": 180000.0,
            },
            {
                "ano": 2024,
                "mes": 2,
                "produto": "grao",
                "volume_ton": 6000000.0,
                "receita_usd_mil": 3000000.0,
            },
            {
                "ano": 2024,
                "mes": 2,
                "produto": "farelo",
                "volume_ton": 2200000.0,
                "receita_usd_mil": 880000.0,
            },
            {
                "ano": 2024,
                "mes": 2,
                "produto": "oleo",
                "volume_ton": 220000.0,
                "receita_usd_mil": 198000.0,
            },
        ]
    )


class TestExportacao:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024)

        assert len(df) == 6
        assert "volume_ton" in df.columns
        assert "produto" in df.columns
        assert "receita_usd_mil" in df.columns

    @pytest.mark.asyncio
    async def test_filter_produto(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, produto="grao")

        assert len(df) == 2
        assert all(df["produto"] == "grao")

    @pytest.mark.asyncio
    async def test_filter_mes(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, mes=1)

        assert len(df) == 3
        assert all(df["mes"] == 1)

    @pytest.mark.asyncio
    async def test_agregacao_mensal(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, agregacao="mensal")

        # 2 meses
        assert len(df) == 2
        jan = df[df["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == pytest.approx(5000000 + 2000000 + 200000)

    @pytest.mark.asyncio
    async def test_return_meta(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df, meta = await api.exportacao(ano=2024, return_meta=True)

        assert meta.source == "abiove"
        assert meta.attempted_sources == ["abiove"]
        assert meta.selected_source == "abiove"
        assert meta.source_method == "httpx+openpyxl"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_filter_produto_case_insensitive(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, produto="Grão")

        assert len(df) == 2
        assert all(df["produto"] == "grao")

    @pytest.mark.asyncio
    async def test_combined_filters(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, mes=1, produto="grao")

        assert len(df) == 1
        assert df.iloc[0]["mes"] == 1
        assert df.iloc[0]["produto"] == "grao"
        assert df.iloc[0]["volume_ton"] == 5000000.0

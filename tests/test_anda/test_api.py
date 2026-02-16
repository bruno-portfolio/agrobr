"""Testes para a API pública ANDA."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.anda import api


def _mock_parsed_df():
    """DataFrame de entregas mockado."""
    return pd.DataFrame(
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
            {
                "ano": 2024,
                "mes": 2,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 120000.0,
            },
            {
                "ano": 2024,
                "mes": 2,
                "uf": "SP",
                "produto_fertilizante": "total",
                "volume_ton": 90000.0,
            },
            {
                "ano": 2024,
                "mes": 3,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 80000.0,
            },
            {
                "ano": 2024,
                "mes": 3,
                "uf": "PR",
                "produto_fertilizante": "total",
                "volume_ton": 70000.0,
            },
        ]
    )


class TestEntregas:
    @pytest.mark.asyncio
    async def test_returns_dataframe(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(b"fake_pdf", 2024),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
        ):
            df = await api.entregas(ano=2024)

        assert len(df) == 6
        assert "volume_ton" in df.columns
        assert "uf" in df.columns
        assert "produto_fertilizante" in df.columns

    @pytest.mark.asyncio
    async def test_filter_uf(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(b"fake_pdf", 2024),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
        ):
            df = await api.entregas(ano=2024, uf="MT")

        assert len(df) == 3
        assert all(df["uf"] == "MT")

    @pytest.mark.asyncio
    async def test_agregacao_mensal(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(b"fake_pdf", 2024),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
        ):
            df = await api.entregas(ano=2024, agregacao="mensal")

        # 3 meses distintos
        assert len(df) == 3

    @pytest.mark.asyncio
    async def test_return_meta(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(b"fake_pdf", 2024),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
        ):
            df, meta = await api.entregas(ano=2024, return_meta=True)

        assert meta.source == "anda"
        assert meta.attempted_sources == ["anda"]
        assert meta.selected_source == "anda"
        assert meta.fetch_timestamp is not None
        assert meta.records_count == len(df)

    @pytest.mark.asyncio
    async def test_filter_uf_case_insensitive(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(b"fake_pdf", 2024),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
        ):
            df = await api.entregas(ano=2024, uf="mt")

        assert len(df) == 3
        assert all(df["uf"] == "MT")

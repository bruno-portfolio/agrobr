"""Testes para a API pública ANDA."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.anda import api
from agrobr.exceptions import InvalidParameterError


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
    async def test_future_year_raises_before_download(self):
        with (
            patch.object(api.client, "fetch_entregas_pdf", new_callable=AsyncMock) as fetch,
            pytest.raises(InvalidParameterError, match="ano"),
        ):
            await api.entregas(ano=9999)

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejects_specific_product_before_download(self):
        with (
            patch.object(api.client, "fetch_entregas_pdf", new_callable=AsyncMock) as mock_fetch,
            pytest.raises(ValueError, match="apenas entregas totais"),
        ):
            await api.entregas(ano=2024, produto="ureia")

        mock_fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_rejects_unknown_agregacao_before_download(self):
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(
                    b"fake_pdf",
                    2024,
                    {"url": "https://anda.org.br/x.pdf", "text": "Dados 2024"},
                ),
            ) as fetch,
            patch.object(api.parser, "parse_entregas_pdf", return_value=_mock_parsed_df()),
            patch.object(api.parser, "edicao_impressa", return_value=None),
            pytest.raises(InvalidParameterError, match="agregacao"),
        ):
            await api.entregas(ano=2024, agregacao="semanal")

        fetch.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_agregacao_mensal(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_entregas_pdf",
                new_callable=AsyncMock,
                return_value=(
                    b"fake_pdf",
                    2024,
                    {"url": "https://anda.org.br/x.pdf", "text": "Dados 2024"},
                ),
            ),
            patch.object(api.parser, "parse_entregas_pdf", return_value=mock_df),
            patch.object(api.parser, "edicao_impressa", return_value=None),
        ):
            df = await api.entregas(ano=2024, agregacao="mensal")

        assert len(df) == 3

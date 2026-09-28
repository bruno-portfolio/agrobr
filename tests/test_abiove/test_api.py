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
    async def test_agregacao_mensal(self):
        mock_df = _mock_parsed_df()
        with (
            patch.object(
                api.client,
                "fetch_exportacao_excel",
                new_callable=AsyncMock,
                return_value=(b"fake_excel", "http://test/exp_202412.xlsx", "2024-12"),
            ),
            patch.object(api.parser, "parse_exportacao_excel", return_value=mock_df),
        ):
            df = await api.exportacao(ano=2024, agregacao="mensal")

        # 2 meses
        assert len(df) == 2
        jan = df[df["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == pytest.approx(5000000 + 2000000 + 200000)

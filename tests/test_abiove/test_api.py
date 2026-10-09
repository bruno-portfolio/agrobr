"""Testes para a API pública ABIOVE."""

from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr.abiove import api

from .test_parser import _make_excel_bytes


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

        assert len(df) == 2
        jan = df[df["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == pytest.approx(5000000 + 2000000 + 200000)


@pytest.mark.asyncio
async def test_ausencia_nao_vira_zero_no_detalhe_nem_no_total_mensal():
    planilha = _make_excel_bytes(
        {
            "Soja em Grão": [
                ["Exportação de Soja em Grão"],
                ["Mês", "Volume (t)", "US$ mil"],
                ["Janeiro", 10, 100],
                ["Fevereiro", 20, None],
                ["Março", None, 300],
            ],
            "Farelo": [
                ["Exportação de Farelo"],
                ["Mês", "Volume (t)", "US$ mil"],
                ["Janeiro", 5, None],
                ["Fevereiro", 7, None],
                ["Março", 8, 80],
            ],
        }
    )
    with patch.object(
        api.client,
        "fetch_exportacao_excel",
        new_callable=AsyncMock,
        return_value=(planilha, "http://test/exp_202412.xlsx", "2024-12"),
    ):
        detalhe = await api.exportacao(ano=2024)
        total, meta = await api.exportacao(ano=2024, agregacao="mensal", return_meta=True)

    grao_marco = detalhe[(detalhe["mes"] == 3) & (detalhe["produto"] == "grao")]
    assert grao_marco["volume_ton"].isna().all()
    por_mes = total.set_index("mes")
    assert por_mes.loc[1, "volume_ton"] == 15
    assert pd.isna(por_mes.loc[1, "receita_usd_mil"])
    assert pd.isna(por_mes.loc[2, "receita_usd_mil"])
    assert pd.isna(por_mes.loc[3, "volume_ton"])
    assert por_mes.loc[3, "receita_usd_mil"] == 380
    assert [aviso for aviso in meta.validation_warnings if aviso.startswith("abiove:")] == [
        f"abiove: {medida} sai nulo em 1 mês(es) com produto sem o valor; "
        "a soma das partes conhecidas não é o total"
        for medida in ("volume_ton", "receita_usd_mil")
    ]

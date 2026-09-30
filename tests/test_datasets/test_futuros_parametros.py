from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from agrobr import b3, datasets
from agrobr.b3.models import B3_CONTRATOS_AGRO
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente

PRODUTO = next(iter(B3_CONTRATOS_AGRO))
JANELA = {"inicio": "2026-09-01", "fim": "2026-09-25"}


@pytest.mark.parametrize(
    ("tipo", "argumentos", "mensagem"),
    [
        (
            "ajustes",
            {"data": "2026-09-25", **JANELA},
            "tipo='ajustes' usa data; inicio e fim valem só para",
        ),
        (
            "ajustes",
            {"inicio": "2026-09-01"},
            "tipo='ajustes' usa data; inicio e fim valem só para",
        ),
        ("posicoes", {"fim": "2026-09-25"}, "tipo='posicoes' usa data; inicio e fim valem só para"),
        (
            "historico",
            {"data": "2026-09-25", **JANELA},
            "tipo='historico' usa inicio e fim; omita data",
        ),
        (
            "oi_historico",
            {"data": "2026-09-25", **JANELA},
            "tipo='oi_historico' usa inicio e fim; omita data",
        ),
    ],
    ids=[
        "ajustes_com_janela",
        "ajustes_com_inicio",
        "posicoes_com_fim",
        "historico_com_data",
        "oi_historico_com_data",
    ],
)
async def test_data_e_janela_juntas_sao_recusadas_antes_da_rede(
    monkeypatch, tipo, argumentos, mensagem
):
    chamadas = {
        nome: AsyncMock() for nome in ("ajustes", "posicoes_abertas", "historico", "oi_historico")
    }
    for nome, espiao in chamadas.items():
        monkeypatch.setattr(b3, nome, espiao)
    with levanta_exatamente(InvalidParameterError, match=mensagem):
        await datasets.futuros_agricolas(PRODUTO, tipo=tipo, **argumentos)
    assert all(espiao.await_count == 0 for espiao in chamadas.values())


@pytest.mark.parametrize(
    ("tipo", "argumentos", "mensagem"),
    [
        ("ajustes", {"data": "21-09-2026"}, "data deve ser date, datetime ou texto"),
        ("posicoes", {"data": "2026/09/21"}, "data deve ser date, datetime ou texto"),
        ("historico", {"inicio": "21/09/2026", "fim": "2026.09.25"}, "fim deve ser date"),
        ("historico", {"inicio": "2026-09-25", "fim": "2026-09-01"}, "inicio deve ser anterior"),
    ],
    ids=["ajustes_formato", "posicoes_formato", "historico_formato", "historico_invertido"],
)
async def test_datas_de_todo_tipo_sao_validadas_antes_da_rede(
    monkeypatch, tipo, argumentos, mensagem
):
    chamadas = {
        nome: AsyncMock() for nome in ("ajustes", "posicoes_abertas", "historico", "oi_historico")
    }
    for nome, espiao in chamadas.items():
        monkeypatch.setattr(b3, nome, espiao)
    with levanta_exatamente(InvalidParameterError, match=mensagem):
        await datasets.futuros_agricolas(PRODUTO, tipo=tipo, **argumentos)
    assert all(espiao.await_count == 0 for espiao in chamadas.values())

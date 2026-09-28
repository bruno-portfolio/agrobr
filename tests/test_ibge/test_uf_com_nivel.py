from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from typer.testing import CliRunner

from agrobr import conab, datasets, ibge
from agrobr.cli import app
from agrobr.exceptions import InvalidParameterError
from agrobr.ibge import client
from tests.helpers import levanta_exatamente

MENSAGEM = "só filtra com nivel='uf' ou 'municipio'"


@pytest.fixture
def sidra(monkeypatch: pytest.MonkeyPatch) -> AsyncMock:
    espiao = AsyncMock(side_effect=AssertionError("a consulta chegou ao SIDRA"))
    monkeypatch.setattr(client, "fetch_sidra", espiao)
    return espiao


@pytest.mark.parametrize(
    ("funcao", "argumentos"),
    [
        (ibge.pam, {"produto": "soja", "nivel": "brasil"}),
        (ibge.ppm, {"especie": "bovino", "nivel": "brasil"}),
        (ibge.censo_agro, {"tema": next(iter(client.TABELAS_CENSO_AGRO)), "nivel": "brasil"}),
        (
            ibge.censo_agro_historico,
            {"tema": next(iter(client.TABELAS_CENSO_HISTORICO)), "nivel": "regiao"},
        ),
        (
            ibge.silvicultura,
            {"produto": next(iter(client.PRODUTOS_SILVICULTURA)), "nivel": "brasil"},
        ),
        (
            ibge.extracao_vegetal,
            {"produto": next(iter(client.PRODUTOS_EXTRACAO_VEGETAL)), "nivel": "brasil"},
        ),
    ],
    ids=["pam", "ppm", "censo_agro", "censo_agro_historico", "silvicultura", "extracao_vegetal"],
)
async def test_uf_com_nivel_sem_filtro_recusa_antes_da_rede(sidra, funcao, argumentos):
    with levanta_exatamente(InvalidParameterError, MENSAGEM):
        await funcao(uf="MT", **argumentos)
    sidra.assert_not_awaited()


async def test_producao_anual_recusa_uf_com_nivel_brasil_antes_das_fontes(sidra, monkeypatch):
    safras = AsyncMock(side_effect=AssertionError("o fallback CONAB foi chamado"))
    monkeypatch.setattr(conab, "safras", safras)
    with levanta_exatamente(InvalidParameterError, MENSAGEM):
        await datasets.producao_anual("soja", nivel="brasil", uf="MT")
    sidra.assert_not_awaited()
    safras.assert_not_awaited()


def test_cli_pam_recusa_uf_com_nivel_brasil(sidra):
    resultado = CliRunner().invoke(app, ["ibge", "pam", "soja", "--nivel", "brasil", "--uf", "MT"])
    assert resultado.exit_code == 1
    assert MENSAGEM in resultado.output
    sidra.assert_not_awaited()

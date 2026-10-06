from __future__ import annotations

import copy
import json
import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr.conab import ceasa_precos
from agrobr.conab.ceasa import client, models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import install_replay_http, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_20260923"
AMOSTRA = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_sample"


def _servir(monkeypatch: pytest.MonkeyPatch) -> dict:
    caso = {
        "requests": [
            {
                "match": {
                    "path": models.PENTAHO_BASE,
                    "params": {"path": models.CDA_PROHORT, "dataAccessId": consulta},
                    "skip": 0,
                },
                "file": arquivo,
                "content_type": "application/json",
            }
            for consulta, arquivo in ((models.QUERY_PRECOS, "precos_response.json"),)
        ]
    }
    return install_replay_http(monkeypatch, caso, GOLDEN)


@pytest.mark.parametrize(
    ("argumentos", "mensagem", "exemplo"),
    [
        (
            {"produto": "kiwi"},
            "Produto 'kiwi' fora do que a CONAB/PROHORT publica. Válidos: ",
            "'ABACATE'",
        ),
        (
            {"ceasa": "Xanadu"},
            "CEASA 'Xanadu' fora do que a CONAB/PROHORT publica. Válidas: ",
            "'AMA/BA - JUAZEIRO'",
        ),
    ],
    ids=["produto", "ceasa"],
)
async def test_nome_fora_do_publicado_e_recusado_com_os_validos(
    monkeypatch, argumentos, mensagem, exemplo
):
    _servir(monkeypatch)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        with levanta_exatamente(InvalidParameterError, match=mensagem) as erro:
            await ceasa_precos(**argumentos)
    assert exemplo in str(erro.value)


async def test_nomes_publicados_seguem_filtrando(monkeypatch):
    _servir(monkeypatch)
    with sem_excecao(), warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame = await ceasa_precos(produto="abacate", ceasa="juazeiro")
    assert set(frame["produto"]) <= {"ABACATE"}
    assert set(frame["ceasa"]) <= {"AMA/BA - JUAZEIRO"}


@pytest.mark.parametrize(
    ("original", "publicado", "pedido", "saida"),
    [
        ("TOMATE (KG)", "PITAYA (KG)", "pitaya", "PITAYA"),
        ("ABOBORA (KG)", "ABÓBORA (KG)", "abobora", "ABÓBORA"),
    ],
    ids=["fora_da_tabela", "publicado_com_acento"],
)
async def test_produto_confere_com_o_publicado_depois_da_rede(original, publicado, pedido, saida):
    precos = json.loads((AMOSTRA / "precos_response.json").read_text(encoding="utf-8"))
    alterado = copy.deepcopy(precos)
    linha = next(row for row in alterado["resultset"] if row[0] == original)
    linha[0] = publicado
    url = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery"
    with (
        patch.object(client, "fetch_precos", new_callable=AsyncMock, return_value=(alterado, url)),
        warnings.catch_warnings(),
    ):
        warnings.simplefilter("ignore")
        frame = await ceasa_precos(produto=pedido)
    assert set(frame["produto"]) == {saida}
    assert len(frame) == sum(preco is not None for preco in linha[1:])

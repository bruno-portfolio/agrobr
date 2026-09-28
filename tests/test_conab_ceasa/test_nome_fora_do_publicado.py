from __future__ import annotations

import warnings
from pathlib import Path

import pytest

from agrobr.conab import ceasa_precos
from agrobr.conab.ceasa import models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import install_replay_http, levanta_exatamente, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_20260923"


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
            for consulta, arquivo in (
                (models.QUERY_PRECOS, "precos_response.json"),
                (models.QUERY_CEASAS, "ceasas_response.json"),
            )
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

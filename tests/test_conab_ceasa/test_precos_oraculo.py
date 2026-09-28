from __future__ import annotations

import json
import re
import warnings
from datetime import datetime
from pathlib import Path

from agrobr.conab import ceasa_precos, lista_ceasas
from agrobr.conab.ceasa import models
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_20260923"


def _caso() -> dict:
    return {
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


def _uf(nome: str) -> str:
    if nome.startswith("CEAGESP - "):
        return "SP"
    if nome.startswith("CEASAMINAS - "):
        return "MG"
    return re.fullmatch(r"[A-Z]+/([A-Z]{2}) - .+", nome).group(1)


def _oficial() -> list[tuple]:
    precos = json.loads((GOLDEN / "precos_response.json").read_text(encoding="utf-8"))
    catalogo = [
        nome
        for _, nome in json.loads((GOLDEN / "ceasas_response.json").read_text(encoding="utf-8"))[
            "resultset"
        ]
    ]
    colunas = []
    for indice, coluna in enumerate(precos["metadata"][1:]):
        instituicao, cidade, resto = coluna["colName"].split("\r")
        assert f"{instituicao.strip()} - {cidade.strip()}" == catalogo[indice]
        colunas.append(
            (datetime.strptime(resto[1:11], "%d/%m/%Y").date().isoformat(), catalogo[indice])
        )
    celulas = []
    for linha in precos["resultset"]:
        produto, unidade = re.fullmatch(r"(.+) \((KG|UN|DZ)\)", linha[0]).groups()
        for (data, ceasa), preco in zip(colunas, linha[1:], strict=True):
            if preco is not None:
                celulas.append((data, produto, unidade, ceasa, _uf(ceasa), float(preco)))
    return celulas


async def test_matriz_oficial_do_dia_sai_celula_a_celula(monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(), GOLDEN)
    with sem_excecao(), warnings.catch_warnings(record=True) as avisos:
        warnings.simplefilter("always")
        frame = await ceasa_precos()
    assert_replay_served(visto)
    assert not [aviso for aviso in avisos if "fora da tabela" in str(aviso.message)]
    esperado = _oficial()
    assert len(esperado) == 1974
    publicado = [
        (
            linha.data.date().isoformat(),
            linha.produto,
            linha.unidade,
            linha.ceasa,
            linha.ceasa_uf,
            linha.preco,
        )
        for linha in frame.itertuples()
    ]
    assert sorted(publicado) == sorted(esperado)


def test_lista_de_ceasas_confere_o_catalogo_oficial():
    catalogo = [
        nome
        for _, nome in json.loads((GOLDEN / "ceasas_response.json").read_text(encoding="utf-8"))[
            "resultset"
        ]
    ]
    assert lista_ceasas() == [{"nome": nome, "uf": _uf(nome)} for nome in sorted(catalogo)]

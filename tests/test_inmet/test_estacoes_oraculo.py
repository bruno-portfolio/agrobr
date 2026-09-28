from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from agrobr import inmet
from agrobr.inmet import client
from tests.helpers import assert_replay_served, install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/inmet/estacoes_20260924"


def _caso(tipo: str) -> dict:
    return {
        "requests": [
            {
                "match": {"path": f"{client.BASE_URL}/estacoes/{tipo}", "params": {}, "skip": 0},
                "file": f"estacoes_{tipo}.json",
                "content_type": "application/json",
            }
        ]
    }


def _numero(valor: str | None) -> float:
    return math.nan if valor in (None, "") else float(valor)


def _mesmo(obtido: float, esperado: float) -> bool:
    return (math.isnan(esperado) and obtido != obtido) or obtido == esperado


@pytest.mark.parametrize("tipo", ["T", "M"])
@pytest.mark.parametrize(("apenas_operantes", "uf"), [(True, None), (False, None), (True, "MG")])
async def test_catalogo_de_estacoes_confere_o_inmet(tipo, apenas_operantes, uf, monkeypatch):
    visto = install_replay_http(monkeypatch, _caso(tipo), GOLDEN)
    with sem_excecao():
        frame, meta = await inmet.estacoes(
            tipo, uf=uf, apenas_operantes=apenas_operantes, return_meta=True
        )
    assert_replay_served(visto)
    oficial = json.loads((GOLDEN / f"estacoes_{tipo}.json").read_text(encoding="utf-8"))
    esperado = [
        registro
        for registro in oficial
        if (not apenas_operantes or registro["CD_SITUACAO"] == "Operante")
        and (uf is None or registro["SG_ESTADO"] == uf)
    ]
    assert esperado
    assert list(frame.index) == list(range(len(frame)))
    publicado = frame.to_dict("records")
    assert len(publicado) == len(esperado)
    for linha, registro in zip(publicado, esperado, strict=True):
        assert (
            linha["codigo"],
            linha["nome"],
            linha["uf"],
            linha["situacao"],
            linha["tipo"],
            linha["inicio_operacao"],
        ) == (
            registro["CD_ESTACAO"],
            registro["DC_NOME"],
            registro["SG_ESTADO"],
            registro["CD_SITUACAO"],
            registro["TP_ESTACAO"],
            registro["DT_INICIO_OPERACAO"],
        )
        for coluna, campo in (
            ("latitude", "VL_LATITUDE"),
            ("longitude", "VL_LONGITUDE"),
            ("altitude", "VL_ALTITUDE"),
        ):
            assert _mesmo(linha[coluna], _numero(registro[campo])), (coluna, registro["CD_ESTACAO"])
    assert meta.source_url == f"{client.BASE_URL}/estacoes/{tipo}"

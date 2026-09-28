from __future__ import annotations

import hashlib
import json
from pathlib import Path

from agrobr import contracts, datasets
from agrobr.ibge.censo_api import censo_agro
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/ibge"
SIDRA = GOLDEN / "censo_2017_oficial_20260922/sidra_6857_df_municipio.json"
ORACULO = GOLDEN / "censo_2017_total_20260924"
URL = "https://apisidra.ibge.gov.br/values/t/6857/n6/in%20N3%2053/h/n/p/all/v/2372,2373/c12604/all"
VARIAVEIS = {"2372": "estabelecimentos", "2373": "area"}


def _oraculo(arquivo: str) -> dict[tuple[str, str], float]:
    manifest = json.loads((ORACULO / "manifest.json").read_bytes())
    registro = next(a for a in manifest["arquivos"] if a["file"] == arquivo)
    corpo = (ORACULO / arquivo).read_bytes()
    assert hashlib.sha256(corpo).hexdigest() == registro["sha256"]
    celulas = {}
    for variavel in json.loads(corpo):
        for resultado in variavel["resultados"]:
            metodo = next(c for c in resultado["classificacoes"] if c["id"] == "12604")
            (categoria,) = metodo["categoria"].values()
            for serie in resultado["series"]:
                celulas[(categoria, VARIAVEIS[variavel["id"]])] = float(serie["serie"]["2017"])
    return celulas


def _servir_sidra(monkeypatch) -> dict[str, list[str]]:
    path, params, skip = helpers.replay_signature(URL)
    pedido = {
        "match": {"path": path, "params": dict(params), "skip": skip},
        "file": SIDRA.name,
        "content_type": "application/json",
    }
    return helpers.install_replay_http(monkeypatch, {"requests": [pedido]}, SIDRA.parent)


async def test_irrigacao_publica_a_linha_total_da_fonte(monkeypatch):
    seen = _servir_sidra(monkeypatch)

    with helpers.sem_excecao():
        frame = await censo_agro("irrigacao", ano=2017, uf="DF", nivel="municipio")

    helpers.assert_replay_served(seen)
    oraculo = _oraculo("api_v3_6857_df_municipio.json")
    assert len(oraculo) == 24
    assert {(r.categoria, r.variavel): r.valor for r in frame.itertuples()} == oraculo
    totais = {chave: valor for chave, valor in oraculo.items() if chave[0] == "Total"}
    assert _oraculo("api_v3_6857_brasilia_total.json") == totais
    metodos = frame[frame["categoria"] != "Total"].groupby("variavel")["valor"].sum()
    assert totais[("Total", "area")] == metodos["area"] == 25626
    assert totais[("Total", "estabelecimentos")] == 2726 < metodos["estabelecimentos"] == 3224


async def test_dataset_publica_o_total_no_contrato(monkeypatch):
    seen = _servir_sidra(monkeypatch)

    with helpers.sem_excecao():
        frame, meta = await datasets.censo_agropecuario(
            "irrigacao", ano=2017, uf="DF", nivel="municipio", return_meta=True
        )

    helpers.assert_replay_served(seen)
    assert sorted(frame.loc[frame["categoria"] == "Total", "variavel"]) == [
        "area",
        "estabelecimentos",
    ]
    assert meta.contract_version == contracts.get_contract("censo_agropecuario").version == "1.2"

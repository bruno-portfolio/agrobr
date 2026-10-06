from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr import conab, datasets
from agrobr.conab.ceasa import models
from tests import helpers

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/conab_ceasa/precos_20261006"


@pytest.mark.parametrize("dataset", [False, True], ids=["fonte", "dataset"])
async def test_ceasa_catalogo_parcial_preserva_precos_publicados(monkeypatch, dataset):
    precos = json.loads((GOLDEN / "precos_response.json").read_bytes())
    catalogo = json.loads((GOLDEN / "ceasas_response.json").read_bytes())
    nomes_catalogo = {nome for _, nome in catalogo["resultset"]}
    assert len(nomes_catalogo) == 22
    assert len(precos["metadata"][1:]) == 43
    assert "CEAGESP - MARILIA" not in nomes_catalogo
    assert "CEAGESP - SAO PAULO" not in nomes_catalogo
    visto = helpers.install_replay_http(
        monkeypatch,
        {
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
                    ("MDXceasa", "ceasas_response.json"),
                )
            ]
        },
        GOLDEN,
    )
    consultar = datasets.preco_atacado if dataset else conab.ceasa_precos

    frame = await consultar(produto="BATATA")

    helpers.assert_replay_served(visto)
    assert len(visto["served"]) == 1
    assert len(frame) == frame["ceasa"].nunique() == 43
    assert set(frame["produto"]) == {"BATATA"}
    assert set(frame["unidade"]) == {"KG"}
    assert set(frame["categoria"]) == {"HORTALICAS"}
    assert str(frame["preco"].dtype) == "float64"
    linha_oficial = next(linha for linha in precos["resultset"] if linha[0] == "BATATA (KG)")
    assert frame["preco"].tolist() == linha_oficial[1:]
    amostras = {
        "AMA/BA - JUAZEIRO": ("2026-10-05", "BA", 4.2),
        "CEAGESP - MARILIA": ("2026-03-19", "SP", 2.2),
        "CEAGESP - SAO PAULO": ("2026-10-02", "SP", 3.33),
        "CEASA/BA - PAULO AFONSO": ("2023-08-02", "BA", 3.6),
        "CEASAMINAS - UBERABA": ("2026-08-31", "MG", 2.4),
    }
    assert {
        linha.ceasa: (linha.data.strftime("%Y-%m-%d"), linha.ceasa_uf, linha.preco)
        for linha in frame.itertuples(index=False)
        if linha.ceasa in amostras
    } == amostras

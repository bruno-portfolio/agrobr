from __future__ import annotations

import warnings

from agrobr.alt.antt_pedagio import api
from scripts import reconciliar_antt_pedagio as reconciliacao
from tests.helpers import install_anttpedagio_source
from tests.test_antt_pedagio import oficial


async def test_mensal_2023_oficial_reconcilia_com_o_texto_normalizado(monkeypatch):
    trafego, pracas = oficial.load("mensal_2023.csv"), oficial.load("pracas.csv")
    install_anttpedagio_source(monkeypatch, {"volume-2023.csv": trafego}, plazas=pracas)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        frame, meta = await api.fluxo_pedagio(ano=2023, return_meta=True)
    assert set(frame["sentido"]) == {"CRESCENTE", "DECRESCENTE"}
    esperado = reconciliacao.oracle(trafego, "mensal")
    assert reconciliacao.compare_traffic(frame, meta, esperado)["status"] == "ok"
    enriquecimento = reconciliacao.compare_enrichment(frame, reconciliacao.plaza_rows(pracas))
    assert enriquecimento["status"] == "ok"
    assert enriquecimento["linked_rows"] == int(frame["uf"].notna().sum()) > 0

from __future__ import annotations

import warnings

import httpx
import pytest

from agrobr.alt.antt_pedagio import api
from scripts import reconciliacao_semanal as semanal
from scripts import reconciliar_antt_pedagio as reconciliacao
from tests.helpers import install_anttpedagio_source
from tests.test_antt_pedagio import oficial


@pytest.mark.parametrize(("status", "indisponivel"), [(503, True), (429, True), (404, False)])
def test_fetch_com_erro_http_nao_entrega_o_corpo_ao_parser(status, indisponivel, tmp_path):
    transport = httpx.MockTransport(lambda _: httpx.Response(status, text="<html>fora</html>"))
    with (
        httpx.Client(transport=transport) as http,
        pytest.raises(httpx.HTTPStatusError) as erro,
    ):
        reconciliacao.fetch(http, reconciliacao.CATALOG.format("x"), tmp_path / "ckan.json")
    assert semanal._fonte_fora(type(erro.value).__name__, str(erro.value)) is indisponivel


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

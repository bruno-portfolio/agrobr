from __future__ import annotations

import pytest

from agrobr.alt.anp_diesel import api
from tests.helpers import assert_replay_served, sem_excecao
from tests.test_anp_diesel import test_reconciliacao_r11 as r11

CASOS = {caso["id"]: caso for caso in r11.MANIFEST["cases"]}
SEMANA = {"produto": "DIESEL S10", "inicio": "2026-09-06", "fim": "2026-09-06"}


async def test_preco_da_uf_e_do_brasil_nao_e_media_por_postos_dos_niveis_de_baixo(monkeypatch):
    visto = r11.install_inputs(monkeypatch)
    with sem_excecao():
        municipios = await api.precos_diesel(uf="AL", nivel="municipio", **SEMANA)
        uf = await api.precos_diesel(uf="AL", nivel="uf", **SEMANA)
        ufs = await api.precos_diesel(nivel="uf", **SEMANA)
        brasil = await api.precos_diesel(nivel="brasil", **SEMANA)
    assert_replay_served(visto)
    oraculo_uf = next(
        linha for linha in CASOS["uf_ultima_semana_semanal"]["expected"] if linha["uf"] == "AL"
    )
    oraculo_municipios = [
        linha
        for linha in CASOS["municipio_ultima_semana_semanal"]["expected"]
        if linha["uf"] == "AL"
    ]
    oraculo_brasil = CASOS["brasil_ultima_semana_semanal"]["expected"][0]
    assert (uf["preco_venda"].item(), uf["n_postos"].item()) == (
        oraculo_uf["preco_venda"],
        oraculo_uf["n_postos"],
    )
    assert sorted(
        zip(municipios["municipio"], municipios["preco_venda"], municipios["n_postos"])
    ) == sorted(
        (linha["municipio"], linha["preco_venda"], linha["n_postos"])
        for linha in oraculo_municipios
    )
    assert (brasil["preco_venda"].item(), brasil["n_postos"].item()) == (
        oraculo_brasil["preco_venda"],
        oraculo_brasil["n_postos"],
    )
    assert municipios["n_postos"].sum() == uf["n_postos"].item()
    assert ufs["n_postos"].sum() == brasil["n_postos"].item()
    por_postos = (municipios["preco_venda"] * municipios["n_postos"]).sum() / municipios[
        "n_postos"
    ].sum()
    esperado = sum(linha["preco_venda"] * linha["n_postos"] for linha in oraculo_municipios) / sum(
        linha["n_postos"] for linha in oraculo_municipios
    )
    assert por_postos == pytest.approx(esperado, abs=1e-9)
    assert por_postos - uf["preco_venda"].item() > 0.3

from __future__ import annotations

import copy
import json
import warnings
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pandas as pd
import pytest

from agrobr import conab, datasets
from agrobr.conab.ceasa import client
from tests.helpers import install_replay_http, sem_excecao

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data"
OFICIAL = GOLDEN / "conab_ceasa/prohort_grupos_20260923"
R9 = GOLDEN / "reconciliacao_mercados_credito_20260918"
MANIFESTO = json.loads((OFICIAL / "manifest.json").read_text(encoding="utf-8"))
CASO_R9 = next(
    caso
    for caso in json.loads((R9 / "manifest.json").read_text(encoding="utf-8"))["cases"]
    if caso["id"] == "ceasa_precos_20260213"
)


def _resultset(nome: str) -> list[list[str]]:
    return json.loads((OFICIAL / nome).read_text(encoding="utf-8"))["resultset"]


def _categoria_oficial() -> dict[str, str]:
    grupos = {nome for _, nome in _resultset("grupo.json")}
    painel = {
        produto: grupo
        for grupo in grupos
        for _, produto in _resultset(f"produto_{grupo.lower()}.json")
    }
    esperado = {}
    for _, produto in _resultset("produtos_preco_diario.json"):
        casados = [p for p in painel if produto == p or produto.startswith(f"{p} ")]
        if casados:
            esperado[produto] = painel[max(casados, key=len)]
    return esperado


def test_categorias_seguem_o_painel_oficial_do_prohort():
    oficial = _categoria_oficial()
    decisao = MANIFESTO["decisao"]
    catalogo = {produto for _, produto in _resultset("produtos_preco_diario.json")}
    publicado = {
        produto: categoria
        for categoria, produtos in conab.ceasa_categorias().items()
        for produto in produtos
    }
    assert set(publicado) == catalogo
    assert {produto: publicado[produto] for produto in oficial} == oficial
    assert sorted(catalogo - set(oficial) - {"OVOS"}) == decisao["sem_grupo_oficial"]
    assert {
        produto: publicado[produto] for produto in decisao["sem_grupo_oficial"]
    } == dict.fromkeys(decisao["sem_grupo_oficial"], "HORTALICAS")
    assert publicado["OVOS"] == "OVOS"


async def test_precos_oficiais_saem_com_a_categoria_do_prohort(monkeypatch: pytest.MonkeyPatch):
    oficial = _categoria_oficial()
    install_replay_http(monkeypatch, CASO_R9, R9)
    with sem_excecao():
        frame = await datasets.preco_atacado()
    assert isinstance(frame, pd.DataFrame)
    categorias = frame.groupby("produto")["categoria"].unique().map(list).to_dict()
    assert {produto: categorias[produto] for produto in oficial if produto in categorias} == {
        produto: [grupo] for produto, grupo in oficial.items() if produto in categorias
    }
    assert categorias["COCO VERDE"] == ["FRUTAS"]
    assert categorias["OVOS"] == ["OVOS"]
    assert (
        not frame[frame["categoria"] == "HORTALICAS"]["produto"].isin(["COCO VERDE", "OVOS"]).any()
    )


async def test_produto_fora_da_tabela_sai_sem_categoria_e_com_um_aviso():
    precos = json.loads(
        (GOLDEN / "conab_ceasa/precos_sample/precos_response.json").read_text(encoding="utf-8")
    )
    alterado = copy.deepcopy(precos)
    linha = next(row for row in alterado["resultset"] if row[0] == "TOMATE (KG)")
    linha[0] = "PITAYA (KG)"
    url = "https://pentahoportaldeinformacoes.conab.gov.br/pentaho/plugin/cda/api/doQuery"
    with (
        patch.object(client, "fetch_precos", new_callable=AsyncMock, return_value=(alterado, url)),
        sem_excecao(),
        warnings.catch_warnings(record=True) as avisos,
    ):
        warnings.simplefilter("always")
        frame = await datasets.preco_atacado()
    pitaya = frame[frame["produto"] == "PITAYA"]
    assert len(pitaya) == sum(preco is not None for preco in linha[1:])
    assert pitaya["categoria"].isna().all()
    assert frame[frame["produto"] != "PITAYA"]["categoria"].notna().all()
    mensagens = [
        str(aviso.message)
        for aviso in avisos
        if "fora da tabela de categorias" in str(aviso.message)
    ]
    assert mensagens == [
        "conab_ceasa: produtos fora da tabela de categorias do agrobr saem com categoria nula: ['PITAYA']"
    ]

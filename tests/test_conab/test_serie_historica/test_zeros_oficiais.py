from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest
import xlrd

from agrobr import conab, datasets
from agrobr.conab.serie_historica import client as serie_client
from agrobr.conab.serie_historica import models as serie_models
from tests import helpers

GOLDEN = Path(__file__).parents[2] / "golden_data/conab"
ZEROS = GOLDEN / "serie_historica_zeros_20260923"
MANIFEST = json.loads((ZEROS / "manifest.json").read_text(encoding="utf-8"))
TRIGO = GOLDEN / "serie_historica_20260917/trigo.xls"
TRIGO_URL = serie_client.get_xls_url("trigo")
CAMPO = {
    "Área": "area_plantada_mil_ha",
    "Produtividade": "produtividade_kg_ha",
    "Produção": "producao_mil_ton",
}
UFS = set(serie_models.UFS_BRASIL)


def servir(monkeypatch: pytest.MonkeyPatch, arquivo: Path, url: str) -> list[str]:
    conteudo = arquivo.read_bytes()
    original = httpx.AsyncClient
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return httpx.Response(
            200 if str(request.url) == url else 404,
            content=conteudo,
            headers={"Content-Type": "application/vnd.ms-excel"},
            request=request,
        )

    def fabrica(**kwargs: Any) -> httpx.AsyncClient:
        return original(transport=httpx.MockTransport(responder), **kwargs)

    monkeypatch.setattr(serie_client.httpx, "AsyncClient", fabrica)
    return pedidos


def celula(arquivo: Path, aba: str, a1: str) -> float:
    coluna, linha = re.fullmatch(r"([A-Z]+)(\d+)", a1).groups()
    indice = 0
    for letra in coluna:
        indice = indice * 26 + ord(letra) - 64
    planilha = xlrd.open_workbook(str(arquivo)).sheet_by_name(aba)
    assert planilha.cell_type(int(linha) - 1, indice - 1) == xlrd.XL_CELL_NUMBER
    return float(planilha.cell_value(int(linha) - 1, indice - 1))


@pytest.mark.parametrize("item", MANIFEST["arquivos"], ids=lambda item: item["produto"])
async def test_zero_publicado_na_planilha_sai_zero(item, monkeypatch):
    servir(monkeypatch, ZEROS / item["file"], item["url"])
    with helpers.sem_excecao():
        fonte = await conab.serie_historica(**item["consulta"])
        dataset = await datasets.serie_historica_safra(**item["consulta"])
    assert len(fonte) == len(dataset) == 1
    for alvo in item["celulas"]:
        oficial = celula(ZEROS / item["file"], alvo["aba"], alvo["celula"])
        assert oficial == alvo["valor"]
        assert fonte.iloc[0][CAMPO[alvo["aba"]]] == oficial
        assert dataset.iloc[0][CAMPO[alvo["aba"]]] == oficial


async def test_media_do_consumidor_conta_o_zero_publicado(monkeypatch):
    item = next(item for item in MANIFEST["arquivos"] if item["produto"] == "amendoim_2")
    servir(monkeypatch, ZEROS / item["file"], item["url"])
    with helpers.sem_excecao():
        df = await conab.serie_historica("amendoim_2", inicio=2010, fim=2011, uf="BA")
    assert df.sort_values("safra")["producao_mil_ton"].tolist() == [6.2, 0.0]
    assert df["producao_mil_ton"].mean() == pytest.approx(3.1)


async def test_safra_zerada_em_todas_as_ufs_nao_foi_levantada(monkeypatch):
    planilha = xlrd.open_workbook(str(TRIGO)).sheet_by_name("Produção")
    safras = [str(planilha.cell_value(5, c)) for c in range(planilha.ncols)]
    linhas = [r for r in range(6, planilha.nrows) if str(planilha.cell_value(r, 0)).strip() in UFS]
    assert {planilha.cell_value(r, safras.index("1976")) for r in linhas} == {0.0}
    assert any(planilha.cell_value(r, safras.index("1977")) for r in linhas)
    servir(monkeypatch, TRIGO, TRIGO_URL)
    with helpers.sem_excecao():
        df = await conab.serie_historica("trigo", inicio=1976, fim=1977)
    assert set(df["safra"]) == {"1977"}
    assert sorted(df["uf"]) == sorted(str(planilha.cell_value(r, 0)).strip() for r in linhas)

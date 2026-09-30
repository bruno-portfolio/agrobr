from __future__ import annotations

import json
import re
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from agrobr import datasets
from agrobr.mapbiomas import client, models
from tests.helpers import mapbiomas_workbook_bundle, sem_excecao

GOLDEN = Path(__file__).parents[1] / "golden_data/mapbiomas/legenda_20260918"
ORACULO = json.loads((GOLDEN / "legend_code.json").read_text(encoding="utf-8"))
TRADUZIDAS_PELO_AGROBR = {10: {0, 13, 75}, 11: {0, 13}}
RECORTE_CLASSE_50 = GOLDEN / "col10_classe50_recorte.xlsx"
ORACULO_CLASSE_50 = json.loads((GOLDEN / "col10_classe50_oracle.json").read_text(encoding="utf-8"))


def _rotulo(texto: str) -> str:
    return re.sub(r"^\s*[\d.]+\s*", "", texto).strip()


@pytest.mark.parametrize("colecao", ORACULO["colecoes"], ids=lambda item: str(item["colecao"]))
def test_rotulos_publicados_seguem_a_legenda_oficial(colecao):
    numero = colecao["colecao"]
    oficiais = {
        int(linha["codigo"]["valor"]): _rotulo(linha["pt"]["valor"]) for linha in colecao["linhas"]
    }
    divergentes = {
        codigo: (models.classe_para_nome(codigo, numero), rotulo)
        for codigo, rotulo in oficiais.items()
        if models.classe_para_nome(codigo, numero) != rotulo
    }
    assert not divergentes, divergentes
    assert set(colecao["classes_nos_dados"]) - set(oficiais) <= TRADUZIDAS_PELO_AGROBR[numero]
    assert all(codigo in models.legenda_colecao(numero) for codigo in colecao["classes_nos_dados"])


@pytest.mark.parametrize(
    ("tipo", "aba", "filtro", "coluna_rotulo", "coluna_periodo"),
    [
        ("cobertura", "COVERAGE_10", {"classe_id": 50}, "classe", "ano"),
        ("transicao", "TRANSITION_10", {"classe_para_id": 50}, "classe_para", "periodo"),
    ],
)
async def test_colecao_10_estadual_usa_a_legenda_da_propria_colecao(
    tipo, aba, filtro, coluna_rotulo, coluna_periodo
):
    legenda_10 = {
        int(linha["codigo"]["valor"]): _rotulo(linha["pt"]["valor"])
        for colecao in ORACULO["colecoes"]
        if colecao["colecao"] == 10
        for linha in colecao["linhas"]
    }
    publicados = {
        celula["cabecalho"]: celula["valor"]
        for celula in ORACULO_CLASSE_50["abas"][aba]["celulas"].values()
    }
    periodos = {
        (str(nome)[1:].replace("_", "-") if str(nome).startswith("p") else nome): valor
        for nome, valor in publicados.items()
        if isinstance(nome, int) or str(nome).startswith("p")
    }
    bundle = mapbiomas_workbook_bundle(
        RECORTE_CLASSE_50.read_bytes(), client._build_xlsx_url("BIOME_STATE", 10)
    )
    with (
        patch.object(client, "fetch_biome_state_bundle", new=AsyncMock(return_value=bundle)),
        sem_excecao(),
    ):
        frame = await datasets.uso_do_solo(tipo=tipo, colecao=10, **filtro)
    assert set(frame[coluna_rotulo]) == {legenda_10[50]}
    assert set(frame["uf"]) == {"MA"} and set(frame["bioma"]) == {publicados["biome"]}
    assert dict(zip(frame[coluna_periodo], frame["area_ha"], strict=True)) == periodos

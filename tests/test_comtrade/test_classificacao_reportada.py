from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr import comtrade
from agrobr.comtrade import api, models
from agrobr.exceptions import InvalidParameterError
from tests.helpers import levanta_exatamente, sem_excecao
from tests.test_comtrade.test_aliases_hs import _servidor

RAIZ = Path(__file__).resolve().parents[1] / "golden_data/comtrade"
GOLDEN = RAIZ / "classificacao_br_20260926"
REFERENCIA = RAIZ / "aliases_20260925/referencia"
REVISOES = ("H0", "H1", "H2", "H3", "H4", "H5", "H6")
DISPONIBILIDADE = json.loads((GOLDEN / "disponibilidade_br_1996_2024.json").read_bytes())["data"]


def _codigos(revisao: str) -> set[str]:
    dados = json.loads((REFERENCIA / f"{revisao}.json").read_text(encoding="utf-8"))
    return {str(entrada["id"]) for entrada in dados["results"]}


@pytest.fixture(scope="module")
def referencia():
    return {revisao: _codigos(revisao) for revisao in REVISOES}


def test_classificacao_do_brasil_e_a_da_disponibilidade_oficial():
    assert {int(item["period"]): item["classificationCode"] for item in DISPONIBILIDADE} == (
        models.CLASSIFICACAO_REPORTADA_BR
    )
    assert {
        (item["reporterCode"], item["isOriginalClassification"]) for item in DISPONIBILIDADE
    } == {(76, True)}


def test_vigencia_dos_codigos_segue_a_referencia_oficial(referencia):
    for codigo in sorted({c for codigos in models.HS_PRODUTOS_AGRO.values() for c in codigos}):
        vigente = tuple(revisao for revisao in REVISOES if codigo in referencia[revisao])
        assert vigente == models.HS_VIGENCIA_DO_CODIGO.get(codigo, REVISOES), codigo


def test_so_o_frango_de_1996_fica_sem_codigo_na_classificacao_do_brasil(referencia):
    esperado = {
        (alias, int(item["period"]))
        for alias, codigos in models.HS_PRODUTOS_AGRO.items()
        for item in DISPONIBILIDADE
        if not set(codigos) & referencia[item["classificationCode"]]
    }
    recusados = set()
    for alias in models.HS_PRODUTOS_AGRO:
        for item in DISPONIBILIDADE:
            try:
                models.validate_hs_periods(alias, [str(item["period"])], 76)
            except InvalidParameterError:
                recusados.add((alias, int(item["period"])))
    assert recusados == esperado == {("carne_frango", 1996)}


@pytest.mark.parametrize("periodo", [1996, "1996-1997"], ids=["ano", "intervalo"])
async def test_frango_de_1996_no_brasil_recusado_antes_da_rede(periodo, monkeypatch):
    pedidos = _servidor(monkeypatch, [])
    with levanta_exatamente(
        InvalidParameterError,
        r"carne_frango em 1996: o Brasil reportou na H0, onde nenhum código do alias existe "
        r"\(020711, 020712, 020713, 020714\)\. Na H0, só 020721",
    ):
        await comtrade.comercio("carne_frango", periodo=periodo)
    assert pedidos == []


async def test_codigos_da_dica_trazem_o_frango_publicado_em_1996(monkeypatch):
    linhas = json.loads((GOLDEN / "frango_h0_br_1996.json").read_bytes())["data"]
    _servidor(monkeypatch, linhas)
    with sem_excecao():
        frame = await comtrade.comercio("020721,020741", periodo=1996)
    assert sorted(frame["hs_code"]) == ["020721", "020741"]
    assert set(frame["classificacao"]) == {"H0"}
    assert frame["volume_ton"].sum() == pytest.approx(554793.6)
    assert frame["valor_fob_usd"].sum() == 831012256.0


@pytest.mark.parametrize(
    ("produto", "periodo", "reporter"),
    [
        ("carne_frango", 1997, "BR"),
        ("carne_frango", 1996, "US"),
        ("soja", 1996, "BR"),
        ("suco_laranja", 2001, "BR"),
        ("carne_frango", 2025, "BR"),
    ],
    ids=["frango_1997", "frango_eua", "soja_parcial", "suco_parcial", "fora_da_disponibilidade"],
)
def test_cobertura_parcial_ou_classificacao_desconhecida_segue(produto, periodo, reporter):
    with sem_excecao():
        selecao = api.prepare_query(produto, reporter=reporter, periodo=periodo)
    assert selecao.hs_codes == models.resolve_hs(produto)

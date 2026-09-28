from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import comexstat, datasets
from agrobr.comexstat import models, query
from agrobr.exceptions import ContractViolationError, InvalidParameterError
from tests.helpers import comexstat_csv, install_comexstat_http

GOLDEN = Path(__file__).parents[1] / "golden_data/comexstat/ncm_oficial_20260922"

CODIGOS_POR_ALIAS: dict[str, set[str]] = {
    "soja": {"12010090", "12019000"},
    "soja_grao": {"12010090", "12019000"},
    "soja_semeadura": {"12010010", "12011000"},
    "oleo_soja": {"15071000", "15079010", "15079011", "15079019", "15079090"},
    "oleo_soja_bruto": {"15071000"},
    "farelo_soja": {"23040010", "23040090"},
    "milho": {"10051000", "10059010", "10059090"},
    "arroz": {
        "10061010",
        "10061091",
        "10061092",
        "10062010",
        "10062020",
        "10063011",
        "10063019",
        "10063021",
        "10063029",
        "10064000",
    },
    "trigo": {
        "10011010",
        "10011090",
        "10011100",
        "10011900",
        "10019010",
        "10019090",
        "10019100",
        "10019900",
    },
    "algodao": {"52010010", "52010020", "52010090", "52030000"},
    "algodao_cardado": {"52030000"},
    "cafe": {"09011110", "09011190", "09011200", "09012100", "09012200"},
    "acucar": {"17011100", "17011200", "17011300", "17011400", "17019100", "17019900"},
    "etanol": {
        "22071000",
        "22071010",
        "22071090",
        "22072010",
        "22072011",
        "22072019",
        "22072020",
    },
    "carne_bovina": {
        "02011000",
        "02012010",
        "02012020",
        "02012090",
        "02013000",
        "02021000",
        "02022010",
        "02022020",
        "02022090",
        "02023000",
    },
    "carne_frango": {
        "02071100",
        "02071200",
        "02071210",
        "02071220",
        "02071300",
        "02071400",
        "02071411",
        "02071412",
        "02071413",
        "02071419",
        "02071421",
        "02071422",
        "02071423",
        "02071424",
        "02071429",
        "02071431",
        "02071432",
        "02071433",
        "02071434",
        "02071439",
    },
    "carne_suina": {"02031100", "02031200", "02031900", "02032100", "02032200", "02032900"},
    "ureia": {"31021010", "31021090"},
    "sulfato_amonio": {"31022100"},
    "nitrato_amonio": {"31023000"},
    "ssp": {"31031900"},
    "tsp": {"31031100"},
    "kcl": {"31042010", "31042090"},
    "map": {"31054000"},
    "dap": {"31053000", "31053010", "31053090"},
    "npk": {"31052000"},
}

DESCRICOES_DA_TRANSICAO = {
    "12010090": "Outros grãos de soja, mesmo triturados",
    "12019000": "Soja, mesmo triturada, exceto para semeadura",
    "12010010": "Soja para semeadura",
    "12011000": "Soja, mesmo triturada, para semeadura",
    "22071000": "Álcool etílico não desnaturado, com volume de teor alcoólico >= 80%",
    "02071400": "Pedaços e miudezas, comestíveis de galos/galinhas, congelados",
    "02071422": "Peitos desossados de galinha, comestíveis, congelados",
    "31053010": "Hidrogeno-ortofosfato de diamônio (fosfato diamônico ou diamoniacal), com teor de "
    "arsênio superior ou igual a 6 mg/kg",
    "31053000": "Hidrogeno-ortofosfato de diamônio (fosfato diamônico ou diamoniacal)",
}


def _dicionario() -> dict[str, str]:
    texto = (GOLDEN / "NCM_recorte.csv").read_bytes().decode("cp1252")
    return {
        row["CO_NCM"]: row["NO_NCM_POR"]
        for row in csv.DictReader(io.StringIO(texto), delimiter=";")
    }


def _painel(nome: str) -> list[dict[str, str]]:
    return json.loads((GOLDEN / nome).read_text(encoding="utf-8"))["data"]["list"]


def _totais(linhas: list[dict[str, str]], codigos: set[str]) -> tuple[int, int]:
    escolhidas = [linha for linha in linhas if linha["coNcm"] in codigos]
    return (
        sum(int(linha["metricKG"]) for linha in escolhidas),
        sum(int(linha["metricFOB"]) for linha in escolhidas),
    )


def _saida(frame: pd.DataFrame) -> tuple[int, int]:
    return int(frame["kg_liquido"].sum()), round(float(frame["valor_fob_usd"].sum()))


def test_aliases_selecionam_os_codigos_do_produto_no_dicionario_oficial():
    dicionario = _dicionario()
    assert {codigo: dicionario[codigo] for codigo in DESCRICOES_DA_TRANSICAO} == (
        DESCRICOES_DA_TRANSICAO
    )
    agricolas = {
        codigo
        for codigo, descricao in dicionario.items()
        if codigo.startswith("3808") and "domissanit" not in descricao.lower()
    }
    esperado = {
        **CODIGOS_POR_ALIAS,
        "fertilizantes": {codigo for codigo in dicionario if codigo.startswith("31")},
        "defensivos": agricolas,
        "agrotoxicos": agricolas,
    }
    obtido = {
        produto: {codigo for codigo in dicionario if models.resolve_ncm(produto).seleciona(codigo)}
        for produto in models.NCM_PRODUTOS
    }
    assert obtido == esperado


@pytest.mark.parametrize(
    "produto", ["soja", "trigo", "acucar", "etanol", "milho", "cafe", "algodao"]
)
async def test_ano_de_transicao_soma_os_codigos_de_cada_periodo(monkeypatch, produto):
    install_comexstat_http(monkeypatch, (GOLDEN / "EXP_2012_01.csv").read_bytes())
    oficial = _painel("painel_export_2012_01.json")
    esperado = _totais(oficial, CODIGOS_POR_ALIAS[produto])
    frame = await comexstat.exportacao(produto, ano=2012)
    assert _saida(frame) == esperado
    assert set(frame["ncm"]) == {linha["coNcm"] for linha in oficial} & CODIGOS_POR_ALIAS[produto]
    if produto in datasets.info("exportacao")["products"]:
        try:
            agregado = await datasets.exportacao(produto, ano=2012)
        except ContractViolationError as exc:
            raise AssertionError(f"dataset fora do contrato: {exc}") from exc
        assert _saida(agregado) == esperado
        assert not agregado.duplicated(["ano", "mes", "produto", "uf"]).any()


@pytest.mark.parametrize("produto", ["ssp", "tsp"])
async def test_superfosfatos_sem_equivalente_antes_de_2017_recusados(monkeypatch, produto):
    calls = install_comexstat_http(
        monkeypatch, comexstat_csv(fluxo="importacao", ano=2016, rows=[])
    )
    with pytest.raises(InvalidParameterError, match=r"a partir de 2017: .*35 % de P2O5.*'310310'"):
        await comexstat.importacao(produto, ano=2016)
    assert calls == []
    assert query.build_query(fluxo="importacao", produto=produto, ano=2017).ano == 2017


@pytest.mark.parametrize("produto", ["cafe_arabica", " CAFE_CONILON "], ids=["arabica", "conilon"])
async def test_especie_de_cafe_recusada_porque_a_ncm_nao_separa_especie(monkeypatch, produto):
    descricoes = [
        descricao.lower() for codigo, descricao in _dicionario().items() if codigo[:4] == "0901"
    ]
    assert not any(
        especie in descricao
        for descricao in descricoes
        for especie in ("arábica", "arabica", "conilon", "robusta", "canephora")
    )
    calls = install_comexstat_http(monkeypatch, comexstat_csv(ano=2025, rows=[]))
    with pytest.raises(InvalidParameterError, match=r"não separa espécie de café.*produto='cafe'"):
        await comexstat.exportacao(produto, ano=2025)
    assert calls == []


@pytest.mark.parametrize("prefixo", ["0207", "020714", "22071010", "2304"])
async def test_prefixo_ncm_seleciona_os_codigos_que_comecam_por_ele(monkeypatch, prefixo):
    install_comexstat_http(monkeypatch, (GOLDEN / "EXP_2025_06.csv").read_bytes())
    oficial = _painel("painel_export_2025_06.json")
    codigos = {linha["coNcm"] for linha in oficial if linha["coNcm"].startswith(prefixo)}
    try:
        frame, meta = await comexstat.exportacao(prefixo, ano=2025, return_meta=True)
    except InvalidParameterError as exc:
        raise AssertionError(f"prefixo recusado: {exc}") from exc
    assert _saida(frame) == _totais(oficial, codigos)
    assert set(frame["ncm"]) == codigos
    assert meta.source_details["query"]["ncm_prefixos"] == [prefixo]


@pytest.mark.parametrize("produto", ["2", "123456789", "22a0", "2207.10", "etanol_anidro"])
async def test_produto_que_nao_e_alias_nem_prefixo_recusado(monkeypatch, produto):
    calls = install_comexstat_http(monkeypatch, comexstat_csv(ano=2025, rows=[]))
    with pytest.raises(InvalidParameterError, match="alias ou um prefixo NCM de 2 a 8 dígitos"):
        await comexstat.exportacao(produto, ano=2025)
    assert calls == []

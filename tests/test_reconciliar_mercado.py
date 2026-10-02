from __future__ import annotations

import gzip
import json
from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pandas as pd
import pytest

from agrobr.bcb import client as sicor_client
from agrobr.exceptions import SourceUnavailableError
from scripts import reconciliar_mercado as reconciliation
from tests.helpers import sem_excecao

GOLDEN = Path(__file__).parent / "golden_data"
SICOR = GOLDEN / "bcb/sicor_regiao_uf_20260925"
METADATA_XML = b"""<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx" Version="4.0">
 <edmx:DataServices>
  <Schema xmlns="http://docs.oasis-open.org/odata/ns/edm" Namespace="x">
   <ComplexType Name="TipoMoeda">
    <Property Name="simbolo" Type="Edm.String"/>
    <Property Name="nomeFormatado" Type="Edm.String"/>
    <Property Name="tipoMoeda" Type="Edm.String"/>
   </ComplexType>
   <EntityType Name="ExpectativaMercadoMensal">
    <Property Name="Indicador" Type="Edm.String"/>
    <Property Name="Media" Type="Edm.Decimal"/>
   </EntityType>
   <EntityContainer Name="C">
    <EntitySet Name="ExpectativaMercadoMensais" EntityType="x.ExpectativaMercadoMensal"/>
   </EntityContainer>
  </Schema>
 </edmx:DataServices>
</edmx:Edmx>"""


def test_compare_entity_sets_reports_missing_entity():
    published = ["CusteioRegiaoUFProduto", "InvestRegiaoUFProduto", "ComercRegiaoUFProduto"]
    assert reconciliation.compare_entity_sets(published, published)["status"] == "ok"
    result = reconciliation.compare_entity_sets(published, ["InvestimentoRegiaoUFProduto"])
    assert result["status"] == "mismatch"
    assert "InvestimentoRegiaoUFProduto" in result["problems"][0]


def test_compare_odata_properties_resolves_sets_and_detects_changes():
    schema = reconciliation.odata_schema(METADATA_XML)
    moeda = reconciliation.ODATA_PROPERTIES["ptax"]["TipoMoeda"]
    mensal = {"Indicador": "Edm.String", "Media": "Edm.Decimal"}
    expected = {"TipoMoeda": moeda, "ExpectativaMercadoMensais": mensal}
    assert reconciliation.compare_odata_properties(schema, expected)["status"] == "ok"
    retyped = {"TipoMoeda": {**moeda, "simbolo": "Edm.Int32"}}
    assert reconciliation.compare_odata_properties(schema, retyped)["status"] == "mismatch"
    partial = {"TipoMoeda": {k: v for k, v in moeda.items() if k != "tipoMoeda"}}
    result = reconciliation.compare_odata_properties(schema, partial)
    assert result["status"] == "mismatch"
    assert "sem decisão" in result["problems"][0]
    assert reconciliation.compare_odata_properties(schema, {"Outro": moeda})["status"] == "mismatch"


def test_compare_socrata_columns_reports_renamed_field():
    columns = [{"fieldName": name} for name in ("open_interest_all", "m_money_positions_long_all")]
    assert reconciliation.compare_socrata_columns(columns, ["open_interest_all"])["status"] == "ok"
    result = reconciliation.compare_socrata_columns(columns, ["open_interest_all_v2"])
    assert result["status"] == "mismatch"


def test_compare_sgs_fields():
    valid = [{"data": "01/01/2024", "valor": "0.42"}]
    assert reconciliation.compare_sgs_fields(valid)["status"] == "ok"
    assert reconciliation.compare_sgs_fields([])["status"] == "mismatch"
    assert reconciliation.compare_sgs_fields([{"data": "01/01/2024"}])["status"] == "mismatch"


def test_compare_ceasa_alignment_on_captured_bodies_and_mutations():
    folder = GOLDEN / "conab_ceasa/precos_sample"
    precos = json.loads((folder / "precos_response.json").read_text(encoding="utf-8"))
    ceasas = json.loads((folder / "ceasas_response.json").read_text(encoding="utf-8"))
    result = reconciliation.compare_ceasa_alignment(precos, ceasas)
    assert result == {"status": "ok", "problems": [], "colunas": 43, "ceasas": 43, "linhas": 48}
    assert reconciliation.compare_ceasa_alignment({}, {})["status"] == "mismatch"
    swapped = json.loads(json.dumps(ceasas))
    swapped["resultset"][0], swapped["resultset"][1] = (
        swapped["resultset"][1],
        swapped["resultset"][0],
    )
    assert reconciliation.compare_ceasa_alignment(precos, swapped)["status"] == "mismatch"
    shorter = json.loads(json.dumps(ceasas))
    shorter["resultset"] = shorter["resultset"][:-1]
    assert reconciliation.compare_ceasa_alignment(precos, shorter)["status"] == "mismatch"


def test_compare_ceasa_catalog_acusa_produto_novo_no_prohort():
    folder = GOLDEN / "conab_ceasa/precos_sample"
    precos = json.loads((folder / "precos_response.json").read_text(encoding="utf-8"))
    assert reconciliation.compare_ceasa_catalog(precos) == {
        "status": "ok",
        "problems": [],
        "produtos": 48,
    }
    novo = json.loads(json.dumps(precos))
    novo["resultset"][0][0] = "PITAYA (KG)"
    assert reconciliation.compare_ceasa_catalog(novo) == {
        "status": "mismatch",
        "problems": ["produtos fora da tabela de categorias do agrobr: ['PITAYA']"],
        "produtos": 48,
    }


def _sicor(nome: str, decimal: bool = True) -> list[dict[str, Any]]:
    bruto = gzip.decompress((SICOR / nome).read_bytes())
    extra: dict[str, Any] = {"parse_float": Decimal} if decimal else {}
    return list(json.loads(bruto, **extra)["value"])


def test_n1_sicor_declara_as_propriedades_do_metadata_oficial():
    schema = reconciliation.odata_schema((SICOR / "metadata.xml").read_bytes())
    esperadas = reconciliation.ODATA_PROPERTIES["sicor"]
    assert sorted(esperadas) == [
        "CusteioInvestimentoComercialIndustrialSemFiltros",
        "CusteioRegiaoUFProduto",
        "RegiaoUF",
    ]
    assert reconciliation.compare_odata_properties(schema, esperadas) == {
        "status": "ok",
        "problems": [],
        "tipos": 17,
    }
    sem_area = {
        **esperadas,
        reconciliation.SICOR_MUNICIPIOS: {
            nome: tipo
            for nome, tipo in esperadas[reconciliation.SICOR_MUNICIPIOS].items()
            if nome != "AreaInvestimento"
        },
    }
    assert reconciliation.compare_odata_properties(schema, sem_area)["problems"] == [
        "CusteioInvestimentoComercialIndustrialSemFiltros: propriedades sem decisão: "
        "['AreaInvestimento']"
    ]


def test_n2_sicor_confere_regiao_uf_com_os_municipios_e_acusa_divergencia():
    regiao_uf = reconciliation.somar_sicor(_sicor("RegiaoUF_2023_01.json.gz"))
    municipios = _sicor("CusteioInvestimentoComercialIndustrialSemFiltros_2023_01.json.gz")
    rotulos = ("RegiaoUF", "SemFiltros")
    assert reconciliation.compare_sicor_totais(
        regiao_uf, reconciliation.somar_sicor(municipios), rotulos
    ) == {"status": "ok", "problems": [], "pares": 89}

    alterado = next(r for r in municipios if r["nomeUF"] == "MT" and r["VlCusteio"] > 0)
    alterado["VlCusteio"] += Decimal("0.01")
    divergente = reconciliation.somar_sicor(municipios)
    assert reconciliation.compare_sicor_totais(regiao_uf, divergente, rotulos) == {
        "status": "mismatch",
        "problems": [
            f"MT custeio: RegiaoUF {regiao_uf[('MT', 'custeio')]} × "
            f"SemFiltros {divergente[('MT', 'custeio')]}"
        ],
        "pares": 89,
    }
    assert reconciliation.compare_sicor_totais({}, {}, rotulos)["problems"] == [
        "RegiaoUF sem nenhum par UF × finalidade"
    ]


def test_n2_sicor_meses_da_safra_do_ultimo_mes_fechado():
    assert reconciliation.meses_consultados(date(2026, 9, 25)) == (
        (2026, 8),
        [(2026, 7), (2026, 8), (2026, 9)],
    )
    fechado, meses = reconciliation.meses_consultados(date(2026, 7, 10))
    assert (fechado, meses[0], meses[-1], len(meses)) == ((2026, 6), (2025, 7), (2026, 6), 12)
    fechado, meses = reconciliation.meses_consultados(date(2026, 1, 5))
    assert (fechado, meses[0], meses[-1], len(meses)) == ((2025, 12), (2025, 7), (2026, 1), 7)


async def test_n2_sicor_ponta_a_ponta_e_indisponibilidade(monkeypatch: pytest.MonkeyPatch):
    async def sicor_mes(_http: Any, _base: str, entidade: str, ano: int, mes: int) -> Any:
        return _sicor(f"{entidade}_{ano}_{mes:02d}.json.gz")

    ate_fevereiro = [
        registro
        for nome in sorted(SICOR.glob("RegiaoUF_*.json.gz"))
        if nome.stem.removesuffix(".json") <= "RegiaoUF_2023_02"
        for registro in _sicor(nome.name, decimal=False)
    ]
    monkeypatch.setattr(reconciliation, "_sicor_mes", sicor_mes)
    monkeypatch.setattr(
        sicor_client, "_fetch_odata", AsyncMock(return_value={"value": ate_fevereiro})
    )
    casos = await reconciliation.n2_sicor(None, "base", date(2023, 2, 10))
    assert [(caso["case"], caso["status"], caso["problems"]) for caso in casos] == [
        ("sicor_total_agrobr_x_regiaouf", "ok", []),
        ("sicor_regiaouf_x_semfiltros", "ok", []),
    ]
    assert casos[0]["safra"] == "2022/2023" and len(casos[0]["meses"]) == 8

    async def fora_do_ar(*_: Any) -> Any:
        raise SourceUnavailableError(source="bcb", last_error="HTTP 500 after 6 retries")

    monkeypatch.setattr(reconciliation, "_sicor_mes", fora_do_ar)
    with sem_excecao():
        casos = await reconciliation.n2_sicor(None, "base", date(2023, 2, 10))
    assert [(caso["case"], caso["status"]) for caso in casos] == [
        ("sicor_total_agrobr_x_regiaouf", "indisponivel"),
        ("sicor_regiaouf_x_semfiltros", "indisponivel"),
    ]


@pytest.mark.parametrize("entidade_com_dados", [None, "RegiaoUF", reconciliation.SICOR_MUNICIPIOS])
async def test_n2_sicor_mes_sem_publicacao_pendente(monkeypatch, entidade_com_dados):
    async def sicor_mes(_http, _base, entidade, ano, mes):
        if entidade == entidade_com_dados and (ano, mes) == (2023, 1):
            return _sicor(f"{entidade}_2023_01.json.gz")
        return []

    monkeypatch.setattr(reconciliation, "_sicor_mes", sicor_mes)
    monkeypatch.setattr(
        reconciliation.sicor_api,
        "credito_rural_total",
        AsyncMock(
            return_value=pd.DataFrame(columns=["uf", "finalidade", "qtd_contratos", "valor"])
        ),
    )
    casos = await reconciliation.n2_sicor(None, "base", date(2023, 2, 1))
    mensal = casos[1]
    assert mensal["mes"] == "2023-01"
    assert mensal["status"] == ("pendente" if entidade_com_dados is None else "mismatch")
    assert mensal["problems"]


async def test_n2_sicor_le_o_mes_pelo_filtro_e_recusa_o_teto(monkeypatch: pytest.MonkeyPatch):
    corpo = gzip.decompress((SICOR / "RegiaoUF_2023_01.json.gz").read_bytes())
    pedidos: list[str] = []

    def responder(request: httpx.Request) -> httpx.Response:
        pedidos.append(str(request.url))
        return httpx.Response(200, content=corpo, request=request)

    async with httpx.AsyncClient(transport=httpx.MockTransport(responder)) as http:
        registros = await reconciliation._sicor_mes(http, "https://sicor", "RegiaoUF", 2023, 1)
        assert pedidos == [
            "https://sicor/RegiaoUF?$format=json&$top=100000"
            "&$filter=AnoEmissao%20eq%20'2023'%20and%20MesEmissao%20eq%20'01'"
        ]
        assert registros == _sicor("RegiaoUF_2023_01.json.gz")
        assert isinstance(registros[0]["VlCusteio"], Decimal)
        monkeypatch.setattr(sicor_client, "SICOR_RECORD_LIMIT", len(registros))
        with pytest.raises(SourceUnavailableError, match="volume no teto"):
            await reconciliation._sicor_mes(http, "https://sicor", "RegiaoUF", 2023, 1)


def _sicor_por_rodada(monkeypatch: pytest.MonkeyPatch, alterar: Any) -> None:
    rodada = {"n": 1}

    async def sicor_mes(_http: Any, _base: str, entidade: str, ano: int, mes: int) -> Any:
        registros = alterar(
            rodada["n"], entidade, (ano, mes), _sicor(f"{entidade}_{ano}_{mes:02d}.json.gz")
        )
        if entidade == reconciliation.SICOR_MUNICIPIOS:
            rodada["n"] += 1
        return registros

    ate_fevereiro = [
        registro
        for nome in sorted(SICOR.glob("RegiaoUF_*.json.gz"))
        if nome.stem.removesuffix(".json") <= "RegiaoUF_2023_02"
        for registro in _sicor(nome.name, decimal=False)
    ]
    monkeypatch.setattr(reconciliation, "_sicor_mes", sicor_mes)
    monkeypatch.setattr(
        sicor_client, "_fetch_odata", AsyncMock(return_value={"value": ate_fevereiro})
    )


def _corta_janeiro(quantos: int) -> Any:
    def alterar(_rodada: int, entidade: str, periodo: tuple[int, int], registros: Any) -> Any:
        return registros[quantos:] if (entidade, periodo) == ("RegiaoUF", (2023, 1)) else registros

    return alterar


@pytest.mark.parametrize(
    ("cenario", "status", "nota"),
    [
        ("converge", "ok", "a diferença da 1ª leitura sumiu"),
        ("persiste", "mismatch", "diferença confirmada com a publicação igual"),
        ("falha", "indisponivel", "2ª leitura falhou"),
        ("muda", "indisponivel", "a publicação mudou entre as 2 leituras"),
    ],
)
async def test_n2_sicor_confirma_a_diferenca_com_uma_segunda_leitura(
    monkeypatch: pytest.MonkeyPatch, cenario: str, status: str, nota: str
):
    def alterar(rodada: int, entidade: str, periodo: tuple[int, int], registros: Any) -> Any:
        if rodada == 2 and cenario == "falha":
            raise SourceUnavailableError(source="bcb", last_error="HTTP 503 after 6 attempts")
        quantos = {"converge": 1 if rodada == 1 else 0, "persiste": 1, "falha": 1, "muda": rodada}[
            cenario
        ]
        return _corta_janeiro(quantos)(rodada, entidade, periodo, registros)

    _sicor_por_rodada(monkeypatch, alterar)
    with sem_excecao():
        casos = await reconciliation.n2_sicor(None, "base", date(2023, 2, 10))
    assert [caso["status"] for caso in casos] == [status, status]
    for caso in casos:
        texto = " ".join([caso.get("segunda_leitura", ""), *caso["problems"]])
        assert nota in texto, caso

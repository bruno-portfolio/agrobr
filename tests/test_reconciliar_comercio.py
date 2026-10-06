from __future__ import annotations

import copy
import io
import json
from pathlib import Path
from typing import Any

import openpyxl
import pytest

from agrobr.comexstat import models as comexstat_models
from agrobr.comtrade import models as comtrade_models
from scripts import reconciliar_comercio as reconciliation

GOLDEN = Path(__file__).parent / "golden_data"
MANIFEST = json.loads(
    (GOLDEN / "reconciliacao_comercio_exterior_20260918/manifest.json").read_text(encoding="utf-8")
)
WORKBOOK = (GOLDEN / "abiove/exportacao_sample/response.xlsx").read_bytes()
NCM_OFICIAL = reconciliation.ncm_dictionary(
    (GOLDEN / "comexstat/ncm_oficial_20260922/NCM_recorte.csv").read_bytes().decode("latin-1")
)


def comtrade_descriptions() -> dict[str, str]:
    return reconciliation.hs_reference(
        (GOLDEN / "comtrade/aliases_20260925/referencia/HS.json").read_bytes()
    )


def abiove_structure() -> list[dict[str, Any]]:
    case = next(case for case in MANIFEST["cases"] if case["source"] == "abiove")
    return copy.deepcopy(case["structure"])


def abiove_inventory() -> dict[str, Any]:
    return reconciliation.abiove_inventory(WORKBOOK)


def workbook_alterado(mutacao: str) -> bytes:
    workbook = openpyxl.load_workbook(io.BytesIO(WORKBOOK), data_only=True)
    if mutacao == "titulo":
        workbook.worksheets[0]["B9"] = "1.1 EXPORTACOES DE ARROZ"
    elif mutacao == "aba_nova":
        workbook.create_sheet("Rel_Exp2025_Portos")
    buffer = io.BytesIO()
    workbook.save(buffer)
    return buffer.getvalue()


def test_unit_dictionary_reads_official_slice():
    text = (GOLDEN / "comexstat/integridade20260908/NCM_UNIDADE.csv").read_text(encoding="latin-1")
    dictionary = reconciliation.unit_dictionary(text)
    assert "10" in dictionary
    assert reconciliation.compare_unit_dictionary(dictionary)["status"] == "ok"


def test_compare_unit_dictionary_reports_missing_or_renamed_kg():
    assert reconciliation.compare_unit_dictionary({})["status"] == "mismatch"
    renamed = {"10": {"CO_UNID": "10", "NO_UNID": "TONELADA", "SG_UNID": "T"}}
    assert reconciliation.compare_unit_dictionary(renamed)["status"] == "mismatch"


def test_ncm_dictionary_mantem_apenas_codigos_de_oito_digitos():
    text = (
        '"CO_NCM";"CO_UNID";"NO_NCM_POR"\n'
        '"12019000";"10";"SOJA, MESMO TRITURADA, EXCETO PARA SEMEADURA"\n'
        '"1201";"10";"CAPITULO AGREGADO"\n'
        '"1201900A";"10";"CODIGO INVALIDO"\n'
    )
    assert reconciliation.ncm_dictionary(text) == {
        "12019000": "SOJA, MESMO TRITURADA, EXCETO PARA SEMEADURA"
    }


def test_compare_ncm_map_confronta_dicionario_oficial():
    result = reconciliation.compare_ncm_map(NCM_OFICIAL)
    assert result["status"] == "ok", result["problems"]
    contagem = {entry["produto"]: (entry["posicoes"], entry["ncms"]) for entry in result["entries"]}
    assert contagem == {
        "soja": (["1201"], 2),
        "milho": (["1005"], 3),
        "cafe": (["0901"], 5),
        "algodao": (["5201", "5203"], 4),
        "acucar": (["1701"], 6),
        "farelo_soja": (["2304"], 2),
        "oleo_soja": (["1507"], 5),
    }


@pytest.mark.parametrize("ncm", ["99999999", "15071000", "23040010", "10059010", "1507"])
def test_compare_ncm_map_reprova_ncm_trocado(ncm: str, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setitem(comexstat_models.NCM_PRODUTOS, "soja", comexstat_models.SelecaoNcm((ncm,)))
    result = reconciliation.compare_ncm_map(NCM_OFICIAL)
    assert result["status"] == "mismatch"
    assert any(problem.startswith("soja:") for problem in result["problems"])


def test_compare_ncm_map_reprova_prefixo_sem_codigo_no_dicionario():
    dictionary = {codigo: nome for codigo, nome in NCM_OFICIAL.items() if codigo != "12010090"}
    result = reconciliation.compare_ncm_map(dictionary)
    assert result["status"] == "mismatch"
    assert "soja: nenhum NCM de 8 dígitos começa por '12010090'" in result["problems"]


def test_compare_ncm_map_reprova_descricao_de_outra_mercadoria():
    dictionary = dict(NCM_OFICIAL)
    dictionary["12019000"] = "FARELO DE SOJA, EM PELLETS"
    result = reconciliation.compare_ncm_map(dictionary)
    assert result["status"] == "mismatch"
    assert any("12019000" in problem for problem in result["problems"])


def test_compare_ncm_map_reprova_ncm_novo_de_outra_mercadoria_no_prefixo():
    dictionary = dict(NCM_OFICIAL)
    dictionary["15079099"] = "OUTROS OLEOS DE SOJA"
    assert reconciliation.compare_ncm_map(dictionary)["status"] == "ok"
    dictionary["15079099"] = "FARINHAS E PELLETS, DA EXTRACAO DO OLEO DE SOJA"
    assert reconciliation.compare_ncm_map(dictionary)["status"] == "mismatch"


def test_hs_reference_remove_o_prefixo_do_codigo():
    payload = json.dumps(
        {
            "results": [
                {"id": "TOTAL", "text": "Total - All HS commodities"},
                {"id": "12", "text": "12 - Oil seeds"},
                {"id": "1201", "text": "1201 - Soya beans, whether or not broken"},
            ]
        }
    ).encode("utf-8")
    assert reconciliation.hs_reference(payload) == {"1201": "Soya beans, whether or not broken"}


def test_compare_hs_map_confronta_descricao_publicada():
    result = reconciliation.compare_hs_map(comtrade_descriptions())
    assert result["status"] == "ok", result["problems"]
    assert {entry["produto"] for entry in result["entries"]} == set(
        comtrade_models.HS_PRODUTOS_AGRO
    )
    assert all(entry["conferidos"] == entry["codes"] for entry in result["entries"])


@pytest.mark.parametrize("presente", [False, True])
def test_compare_hs_map_reprova_codigo_trocado(presente: bool, monkeypatch: pytest.MonkeyPatch):
    descriptions = comtrade_descriptions()
    if presente:
        descriptions["9999"] = "Commodities not specified according to kind"
    monkeypatch.setitem(comtrade_models.HS_PRODUTOS_AGRO, "soja", ["9999"])
    result = reconciliation.compare_hs_map(descriptions, produtos=["soja"])
    assert result["status"] == "mismatch"
    assert any("9999" in problem for problem in result["problems"])


def test_compare_comtrade_count_matches_manifest_rows():
    body = (GOLDEN / "comtrade/selecao_20260906/soy_br_omitted_count_01.json").read_bytes()
    rows = next(
        c["period"]["rows"] for c in MANIFEST["cases"] if c["id"] == "comtrade_soja_br_x_2023"
    )
    assert reconciliation.compare_comtrade_count(body, rows) == {
        "status": "ok",
        "problems": [],
        "count": rows,
    }
    assert reconciliation.compare_comtrade_count(body, rows + 1)["status"] == "mismatch"


def test_normalizar_titulo_neutraliza_ano_e_mes():
    assert reconciliation.normalizar_titulo("Rel_Exp2025") == reconciliation.normalizar_titulo(
        "Rel_Exp2026"
    )
    assert reconciliation.normalizar_titulo(
        "Exportações de milho – jan-dez (2025)"
    ) == reconciliation.normalizar_titulo("Exportações de milho – jan-dez (2026)")
    assert reconciliation.normalizar_titulo("Outros tipos de algodão") == "OUTROS TIPOS DE ALGODAO"
    assert reconciliation.normalizar_titulo("Exportações de soja em grão") != (
        reconciliation.normalizar_titulo("Exportações de arroz")
    )


def test_abiove_inventory_lista_abas_blocos_e_extremos():
    inventory = abiove_inventory()
    assert [aba["sheet"] for aba in inventory["abas"]] == ["Rel_Exp2025"]
    aba = inventory["abas"][0]
    assert aba["sheet_norm"] == "REL EXP<ANO>"
    titulos = {bloco["bloco"]: bloco["titulo_norm"] for bloco in aba["blocos"]}
    assert {"1.1", "1.2", "1.3", "1.4", "1.5.1", "1.5.2", "1.5.3", "1.5.4"} <= set(titulos)
    assert {"2.1.1.1", "3.2.1.3"} <= set(titulos)
    assert titulos["1.1"] == "EXPORTACOES DE SOJA EM GRAO"
    assert titulos["2.1.1.2"] == "EXPORTACOES DE SOJA EM GRAO <MES> <MES> EM TONELADAS"
    assert aba["ultima_coluna"] == "AD"
    assert aba["ultima_celula"]["cell"] == "O373"
    assert aba["ultima_celula"]["value"].startswith("Nota: dados dispon")


def test_compare_abiove_inventory_aceita_o_manifesto():
    result = reconciliation.compare_abiove_inventory(abiove_inventory(), abiove_structure())
    assert result["status"] == "ok", result["problems"]
    assert result["blocos_publicados"] == result["blocos_previstos"] == 47
    assert result["titulos_conferidos"] == 47
    assert result["abas_publicadas"] == ["Rel_Exp2025"]


@pytest.mark.parametrize("mutacao", ["titulo", "aba_nova"])
def test_compare_abiove_inventory_reprova_workbook_alterado(mutacao: str):
    controle = reconciliation.abiove_inventory(workbook_alterado("nenhuma"))
    assert reconciliation.compare_abiove_inventory(controle, abiove_structure())["status"] == "ok"
    inventory = reconciliation.abiove_inventory(workbook_alterado(mutacao))
    assert (
        reconciliation.compare_abiove_inventory(inventory, abiove_structure())["status"]
        == "mismatch"
    )


@pytest.mark.parametrize(
    "mutacao",
    ["bloco_nao_previsto", "bloco_ausente", "aba_ausente", "sem_destino", "sem_motivo", "extremo"],
)
def test_compare_abiove_inventory_reprova_mutacao(mutacao: str):
    inventory = abiove_inventory()
    structure = abiove_structure()
    aba = inventory["abas"][0]
    tabelas = [item for item in structure if item["kind"] == "sheet_table"]
    if mutacao == "bloco_nao_previsto":
        structure.remove(tabelas[0])
    elif mutacao == "bloco_ausente":
        aba["blocos"] = [bloco for bloco in aba["blocos"] if bloco["bloco"] != "1.1"]
    elif mutacao == "aba_ausente":
        inventory["abas"] = []
    elif mutacao == "sem_destino":
        next(item for item in tabelas if item["estado"] == "mapeada")["campo"] = None
    elif mutacao == "sem_motivo":
        next(item for item in tabelas if item["estado"] == "ignorada")["motivo"] = None
    else:
        next(item for item in structure if item["kind"] == "extent")["locator"]["ultima_coluna"] = (
            "K"
        )
    assert reconciliation.compare_abiove_inventory(inventory, structure)["status"] == "mismatch"

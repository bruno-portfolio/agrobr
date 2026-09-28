from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
from bs4 import BeautifulSoup

from scripts import reconciliar_preco_diario as reconciliation

GOLDEN = Path(__file__).parent / "golden_data" / "reconciliacao_r6_20260918"
MANIFEST = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
CASES = {case["id"]: case for case in MANIFEST["cases"]}


def _html(case: dict) -> str:
    return (GOLDEN / case["file"]).resolve().read_text(encoding="utf-8")


@pytest.mark.parametrize("case_id", sorted(cid for cid in CASES if cid.startswith("cepea_")))
def test_cepea_inventory_matches_manifest_structure(case_id: str):
    case = CASES[case_id]
    result = reconciliation.compare_cepea(case, reconciliation.inventory_cepea(_html(case)))
    assert result == {"case": case_id, "status": "ok", "problems": []}


@pytest.mark.parametrize("case_id", sorted(cid for cid in CASES if cid.startswith("noticias_")))
def test_noticias_agricolas_inventory_matches_manifest_structure(case_id: str):
    case = CASES[case_id]
    result = reconciliation.compare_na(case, reconciliation.inventory_na(_html(case)))
    assert result == {"case": case_id, "status": "ok", "problems": []}


def test_cepea_new_table_without_decision_is_reported():
    case = CASES["cepea_soja"]
    items = reconciliation.inventory_cepea(_html(case))
    items.append({**items[0], "title": "INDICADOR NOVO SEM DECISÃO"})
    result = reconciliation.compare_cepea(case, items)
    assert result["status"] == "mismatch"
    assert "INDICADOR NOVO SEM DECISÃO" in result["problems"][0]


def test_cepea_header_change_is_reported():
    case = CASES["cepea_soja"]
    items = reconciliation.inventory_cepea(_html(case))
    items[0]["headers"] = [*items[0]["headers"][:-1], "Valor EUR"]
    result = reconciliation.compare_cepea(case, items)
    assert result["status"] == "mismatch"
    assert "cabeçalho mudou" in result["problems"][0]


@pytest.mark.parametrize("mutation", ["changed_header", "extra_table"])
def test_na_unknown_header_is_reported(mutation: str):
    case = CASES["noticias_agricolas_soja"]
    soup = BeautifulSoup(_html(case), "lxml")
    table = soup.select("table.cot-fisicas")[0]
    if mutation == "extra_table":
        table = copy.deepcopy(table)
        soup.select("div.cotacao")[0].append(table)
    table.find_all("tr")[0].find_all(["th", "td"])[1].string = "Valor EUR"
    result = reconciliation.compare_na(case, reconciliation.inventory_na(str(soup)))
    assert result["status"] == "mismatch"
    assert "cabeçalho sem decisão" in result["problems"][0]


def test_cepea_repeated_title_is_ambiguous():
    case = CASES["cepea_soja"]
    soup = BeautifulSoup(_html(case), "lxml")
    title = soup.select("div.imagenet-table-titulo")[0]
    title.parent.append(copy.deepcopy(title))
    title.parent.append(copy.deepcopy(title.find_next("table")))
    result = reconciliation.compare_cepea(case, reconciliation.inventory_cepea(str(soup)))
    assert result["status"] == "mismatch"
    assert "título repetido" in result["problems"][0]

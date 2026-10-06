from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from scripts import reconciliar_boletins as audit

GOLDEN = Path(__file__).parent / "golden_data/reconciliacao_boletins_anec_anda_deral_20260918"
PROFILES = json.loads((GOLDEN / "structure_profiles.json").read_text(encoding="utf-8"))


@pytest.mark.parametrize("case", PROFILES["cases"], ids=lambda c: c["id"])
def test_inventario_todas_paginas_e_abas_preservadas(case):
    actual = audit.inventory((GOLDEN / case["file"]).read_bytes())
    assert audit.compare_inventory(actual, case["inventory"])["status"] == "ok"


@pytest.mark.parametrize("mutation", ["weekly_missing_sorghum", "monthly_missing_ddgs"])
def test_cabecalho_pdf_removido_exige_revisao(mutation):
    reference = next(c for c in PROFILES["cases"] if c["id"] == "anec_w34_2026")
    actual = audit.inventory((GOLDEN / f"anec/mutations/{mutation}.pdf").read_bytes())
    result = audit.compare_inventory(actual, reference["inventory"])
    assert result["status"] == "mismatch"
    assert len(result["problems"]) == 1


@pytest.mark.parametrize("mutation", ["weekly_value", "monthly_value", "yoy_wrong_year"])
def test_n1_ignora_literal_numerico_coberto_pelo_oraculo_n2(mutation):
    reference = next(c for c in PROFILES["cases"] if c["id"] == "anec_w34_2026")
    actual = audit.inventory((GOLDEN / f"anec/mutations/{mutation}.pdf").read_bytes())
    assert audit.compare_inventory(actual, reference["inventory"])["status"] == "ok"


@pytest.mark.parametrize("header", ["boa", "media", "ruim", "plantada", "colhida"])
def test_cabecalho_planilha_removido_exige_revisao(header):
    expected = audit.inventory((GOLDEN / "deral/mutations/control.xls").read_bytes())
    actual = audit.inventory((GOLDEN / f"deral/mutations/missing_{header}.xls").read_bytes())
    assert audit.compare_inventory(actual, expected)["status"] == "mismatch"


@pytest.mark.parametrize("field", ["pages", "sheets"])
def test_pagina_ou_aba_extra_nao_recebe_aceite_por_prefixo(field):
    expected = next(c["inventory"] for c in PROFILES["cases"] if field in c["inventory"])
    actual = copy.deepcopy(expected)
    actual[field].append({"name": "Novo produto"})
    assert audit.compare_inventory(actual, expected)["status"] == "mismatch"


def test_documentos_vazios_nao_comprovam_cobertura():
    assert audit.compare_inventory({}, {})["status"] == "mismatch"

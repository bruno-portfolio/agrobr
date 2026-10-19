from __future__ import annotations

import copy
from io import BytesIO
from pathlib import Path
from typing import Any

import pytest

from agrobr.exceptions import SourceUnavailableError
from scripts import reconciliar_serie_historica as reconciliation
from tests import helpers


def test_inventory_retains_published_period_and_unit():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "cafe.xls").read_bytes()
    structures, values = reconciliation.inventory_workbook(raw)
    assert structures[0]["header_row"] == 6
    assert structures[0]["published_unit"] == "Em hectares"
    assert structures[0]["periods"][-2]["raw"] == 2025
    assert structures[0]["periods"][-2]["estado"] == "mapeada"
    assert structures[0]["periods"][-1]["raw"] == "2026 (¹)"
    assert structures[0]["periods"][-1]["normalized"] == "2026"
    assert structures[0]["periods"][-1]["estado"] == "ignorada"
    assert structures[0]["periods"][-1]["motivo"] == "previsao"
    assert values["Área em produção"]["normalized_period"] == "2025"


def test_mapping_drift_fails_reconciliation():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
    reference = copy.deepcopy(
        next(
            case
            for case in helpers.load_serie_historica_manifest()["cases"]
            if case["id"] == "soja"
        )
    )
    reference["sheets"]["Área"]["multiplicador"] = 1000.0
    result = reconciliation.reconcile_workbook(raw, "soja", reference)
    assert result["status"] == "mismatch"
    assert "sheet_mapping_differs_from_reviewed_manifest" in result["differences"]


def test_unit_drift_fails_reconciliation():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
    reference = copy.deepcopy(
        next(
            case
            for case in helpers.load_serie_historica_manifest()["cases"]
            if case["id"] == "soja"
        )
    )
    for sample in reference["samples"]:
        for source in sample["raw_cells"]:
            if source["sheet"] == "Área":
                source["published_unit"] = "Em hectares"
    result = reconciliation.reconcile_workbook(raw, "soja", reference)
    assert result["status"] == "mismatch"
    assert "published_unit_differs:Área" in result["differences"]


def test_forecast_decision_drift_fails_reconciliation():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
    reference = copy.deepcopy(
        next(
            case
            for case in helpers.load_serie_historica_manifest()["cases"]
            if case["id"] == "soja"
        )
    )
    reference["period_columns"]["Área"][-1]["estado"] = "mapeada"
    reference["period_columns"]["Área"][-1]["motivo"] = None
    result = reconciliation.reconcile_workbook(raw, "soja", reference)
    assert result["status"] == "mismatch"
    assert "period_mapping_differs:Área" in result["differences"]


def _soja_reference_one_harvest_earlier() -> dict[str, Any]:
    reference = copy.deepcopy(
        next(
            case
            for case in helpers.load_serie_historica_manifest()["cases"]
            if case["id"] == "soja"
        )
    )
    for sheet, periods in reference["period_columns"].items():
        *history, forecast = periods[:-1]
        reference["period_columns"][sheet] = [
            *history,
            forecast
            | {
                "raw": f"{forecast['raw']} Previsão (¹)",
                "estado": "ignorada",
                "motivo": "previsao",
            },
        ]
    return reference


def test_harvest_rollover_keeps_reconciliation_ok():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
    result = reconciliation.reconcile_workbook(raw, "soja", _soja_reference_one_harvest_earlier())
    assert result["status"] == "ok"


def test_rewritten_history_fails_reconciliation():
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "soja.xls").read_bytes()
    reference = _soja_reference_one_harvest_earlier()
    reference["period_columns"]["Área"][10]["raw"] = "1986/87 (*)"
    result = reconciliation.reconcile_workbook(raw, "soja", reference)
    assert result["status"] == "mismatch"
    assert result["differences"] == ["period_mapping_differs:Área"]


@pytest.mark.parametrize(
    "published,expected_difference",
    [
        ("Em mil hectares", None),
        ("Em kg/ha", "unit_exception_changed:Produtividade"),
        ("Em t/ha", "unit_exception_changed:Produtividade"),
    ],
)
def test_declared_unit_exception_requires_exact_publication(
    published: str, expected_difference: str | None
):
    exception = next(
        item
        for item in helpers.load_serie_historica_manifest()["unit_exceptions"]
        if item["product"] == "arroz_sequeiro" and item["sheet"] == "Produtividade"
    )
    sheet = {"name": "Produtividade", "published_unit": published}
    assert reconciliation._unit_difference(sheet, "Em kg/ha", exception) == expected_difference


@pytest.mark.asyncio
async def test_sweep_continues_after_failure_and_shares_cotton_download(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    calls = []
    raw = (helpers.SERIE_HISTORICA_GOLDEN / "algodao.xls").read_bytes()

    async def download(product: str) -> tuple[BytesIO, dict[str, str]]:
        calls.append(product)
        if product == "soja":
            raise SourceUnavailableError(source="conab_serie_historica", last_error="offline")
        return BytesIO(raw), {"url": reconciliation.client.get_xls_url(product)}

    monkeypatch.setattr(reconciliation.client, "download_xls", download)
    report = await reconciliation.sweep(
        ["soja", "algodao", "algodao_pluma", "algodao_caroco"],
        helpers.load_serie_historica_manifest(),
        tmp_path / "inventory.json",
        delay=0,
    )
    assert calls == ["soja", "algodao"]
    assert report["failed_products"] == ["soja"]
    assert report["downloaded_urls_count"] == 1
    assert [result["status"] for result in report["results"]] == ["error", "ok", "ok", "ok"]
    assert not report["baseline_approved"]

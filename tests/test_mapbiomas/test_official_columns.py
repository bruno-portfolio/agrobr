from __future__ import annotations

import hashlib
import json
from io import BytesIO
from pathlib import Path

import openpyxl
import pytest

from agrobr.exceptions import ParseError
from agrobr.mapbiomas import parser

FIXTURE = Path(__file__).parents[1] / "golden_data/mapbiomas/collection11_official"


def test_official_coverage_preserves_all_year_values():
    data = (FIXTURE / "response.xlsx").read_bytes()
    provenance = json.loads((FIXTURE / "provenance.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(data).hexdigest() == provenance["excerpt_sha256"]
    original = provenance["sheets"]["COVERAGE_11"]
    expected = {
        int(header[1:]): value
        for header, value in zip(original["headers"], original["values"], strict=True)
        if isinstance(header, str) and header.startswith("y")
    }

    frame = parser.parse_cobertura_xlsx(data, colecao=11)

    assert set(frame["ano"]) == set(range(1985, 2026))
    assert dict(zip(frame["ano"], frame["area_ha"], strict=True)) == pytest.approx(expected)
    assert frame["classe_id"].unique().tolist() == [3]
    assert frame["estado"].unique().tolist() == ["PA"]
    assert frame["bioma"].unique().tolist() == ["Amazônia"]


def test_official_transition_preserves_all_period_values():
    data = (FIXTURE / "response.xlsx").read_bytes()
    original = json.loads((FIXTURE / "provenance.json").read_text(encoding="utf-8"))["sheets"][
        "TRANSITION_11"
    ]
    expected = {
        header[1:].replace("_", "-"): value
        for header, value in zip(original["headers"], original["values"], strict=True)
        if isinstance(header, str) and header.startswith("p")
    }

    frame = parser.parse_transicao_xlsx(data, colecao=11)

    assert dict(zip(frame["periodo"], frame["area_ha"], strict=True)) == pytest.approx(expected)
    assert "1985-1986" in frame["periodo"].tolist()
    assert "2024-2025" in frame["periodo"].tolist()
    assert frame["classe_de_id"].unique().tolist() == [0]
    assert frame["classe_para_id"].unique().tolist() == [3]
    assert frame["estado"].unique().tolist() == ["PA"]


@pytest.mark.parametrize("new_header", ["y2026", 1985])
def test_invalid_year_headers_are_rejected(new_header: str | int):
    workbook = openpyxl.load_workbook(FIXTURE / "response.xlsx")
    try:
        sheet = workbook["COVERAGE_11"]
        sheet.cell(1, 13).value = new_header
        content = BytesIO()
        workbook.save(content)
    finally:
        workbook.close()
    with pytest.raises(ParseError):
        parser.parse_cobertura_xlsx(content.getvalue(), colecao=11)

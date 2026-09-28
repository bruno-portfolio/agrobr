from __future__ import annotations

import hashlib
import io
import json
from pathlib import Path

import openpyxl
import pytest

from agrobr.abiove import parser
from agrobr.exceptions import ParseError

GOLDEN = Path(__file__).resolve().parents[1] / "golden_data/abiove/exportacao_sample"


@pytest.fixture(scope="module")
def official_workbook():
    content = (GOLDEN / "response.xlsx").read_bytes()
    metadata = json.loads((GOLDEN / "metadata.json").read_text(encoding="utf-8"))
    assert hashlib.sha256(content).hexdigest() == metadata["sha256"]
    workbook = openpyxl.load_workbook(io.BytesIO(content), data_only=True)
    return content, workbook["Rel_Exp2025"], metadata["api_url"]


@pytest.mark.parametrize("ano,volume_col,receita_col", [(2024, "F", "C"), (2025, "G", "D")])
@pytest.mark.parametrize("produto,row", [("grao", 12), ("farelo", 31), ("oleo", 50), ("milho", 69)])
def test_monthly_products_match_official_cells(
    official_workbook, ano, volume_col, receita_col, produto, row
):
    content, sheet, _ = official_workbook
    assert sheet[f"B{row}"].value == "Jan"
    assert sheet[f"{volume_col}{row - 1}"].value.year == ano
    frame = parser.parse_exportacao_excel(content, ano=ano)
    assert len(frame) == 48
    assert set(frame["produto"]) == {"grao", "farelo", "oleo", "milho"}
    assert not frame.duplicated(["ano", "mes", "produto"]).any()
    record = frame.loc[(frame["produto"] == produto) & (frame["mes"] == 1)].iloc[0]
    assert record["volume_ton"] == pytest.approx(sheet[f"{volume_col}{row}"].value * 1000)
    assert record["receita_usd_mil"] == pytest.approx(sheet[f"{receita_col}{row}"].value)


def test_requested_year_missing_does_not_relabel_another_year(official_workbook):
    content, _, _ = official_workbook
    with pytest.raises(ParseError, match="Nenhum dado"):
        parser.parse_exportacao_excel(content, ano=2023)

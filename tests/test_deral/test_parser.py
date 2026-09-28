"""Testes para o parser DERAL."""

from __future__ import annotations

import io
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock

import openpyxl
import pandas as pd
import pytest

from agrobr import datasets
from agrobr.deral import client
from agrobr.deral.parser import (
    filter_by_produto,
    parse_pc_xls,
)
from agrobr.exceptions import ParseError, SourceUnavailableError

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "deral" / "pc_sample"


@pytest.fixture
def unreadable_current_sheet(monkeypatch: pytest.MonkeyPatch) -> tuple[bytes, ValueError]:
    data = (GOLDEN_DIR.parent / "pc_20260915" / "response.xls").read_bytes()
    read_excel = pd.read_excel
    error = ValueError("Falha simulada ao ler a aba Atual")

    def fail_current_sheet(*args: Any, **kwargs: Any) -> pd.DataFrame:
        if kwargs.get("sheet_name") == "Atual":
            raise error
        return read_excel(*args, **kwargs)

    monkeypatch.setattr(pd, "read_excel", fail_current_sheet)
    return data, error


async def test_dataset_aba_ilegivel_propaga_falha(unreadable_current_sheet, monkeypatch):
    data, _ = unreadable_current_sheet
    monkeypatch.setattr(client, "fetch_pc_xls", AsyncMock(return_value=data))

    with pytest.raises(SourceUnavailableError, match="Falha ao ler a aba Atual"):
        await datasets.condicao_lavouras()


def _make_xls_bytes(sheets: dict[str, list[list]]) -> bytes:
    """Cria arquivo Excel em memória a partir de dict de sheets."""
    wb = openpyxl.Workbook()
    # Remover sheet padrão
    default_sheet = wb.active
    if default_sheet is not None:
        wb.remove(default_sheet)

    for name, rows in sheets.items():
        ws = wb.create_sheet(title=name)
        for row in rows:
            ws.append(row)

    buf = io.BytesIO()
    wb.save(buf)
    return buf.getvalue()


class TestParsePcXls:
    def test_empty_file(self):
        sheets = {"Sheet1": []}
        data = _make_xls_bytes(sheets)
        with pytest.raises(ParseError, match="Nenhum registro reconhecido"):
            parse_pc_xls(data)

    def test_invalid_bytes(self):
        with pytest.raises(ParseError, match="Falha ao abrir PC.xls"):
            parse_pc_xls(b"not a valid excel file")


class TestFilterByProduto:
    def test_empty_produto(self):
        df = pd.DataFrame([{"produto": "soja", "condicao": "boa", "pct": 70.0}])
        result = filter_by_produto(df, "")
        assert len(result) == 1

    def test_generic_product_includes_first_and_second_seasons(self):
        df = pd.DataFrame(
            [
                {"produto": "milho_1"},
                {"produto": "milho_2"},
                {"produto": "soja"},
            ]
        )

        result = filter_by_produto(df, "milho")

        assert result["produto"].tolist() == ["milho_1", "milho_2"]

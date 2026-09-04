"""Testes para o parser DERAL."""

import io
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from agrobr.deral.parser import (
    PARSER_VERSION,
    _extract_condicao_from_sheet,
    _find_data_referencia,
    filter_by_produto,
    parse_pc_xls,
)
from agrobr.exceptions import ParseError
from agrobr.normalize.numeric import safe_float

GOLDEN_DIR = Path(__file__).parent.parent / "golden_data" / "deral" / "pc_sample"


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


class TestSafeFloat:
    def test_integer(self):
        assert safe_float(42, strip="%") == 42.0

    def test_float(self):
        assert safe_float(3.14, strip="%") == 3.14

    def test_string(self):
        assert safe_float("75.5", strip="%") == 75.5

    def test_br_format(self):
        assert safe_float("1.234,56", strip="%") == 1234.56

    def test_percentage(self):
        assert safe_float("85%", strip="%") == 85.0

    def test_none(self):
        assert safe_float(None, strip="%") is None

    def test_dash(self):
        assert safe_float("-", strip="%") is None
        assert safe_float("–", strip="%") is None

    def test_empty(self):
        assert safe_float("", strip="%") is None

    def test_nd(self):
        assert safe_float("n.d.", strip="%") is None


class TestFindDataReferencia:
    def test_finds_date(self):
        df = pd.DataFrame(
            [
                ["Condição das Lavouras", "Referência: 15/01/2025", None],
                [None, None, None],
            ]
        )
        assert _find_data_referencia(df) == "15/01/2025"

    def test_no_date(self):
        df = pd.DataFrame([["Soja", "Paraná", None]])
        assert _find_data_referencia(df) == ""


class TestExtractCondicaoFromSheet:
    def test_finds_condicao(self):
        df = pd.DataFrame(
            [
                ["Condição", "Percentual"],
                ["Boa", 75.0],
                ["Média", 20.0],
                ["Ruim", 5.0],
            ]
        )
        records = _extract_condicao_from_sheet(df, "soja")
        condicoes = [r["condicao"] for r in records if r["condicao"]]
        assert "boa" in condicoes
        assert "media" in condicoes
        assert "ruim" in condicoes

    def test_finds_plantio(self):
        df = pd.DataFrame(
            [
                ["Progresso", "Percentual"],
                ["Plantio", 95.0],
            ]
        )
        records = _extract_condicao_from_sheet(df, "soja")
        plantio = [r for r in records if r.get("plantio_pct") is not None]
        assert len(plantio) >= 1
        assert plantio[0]["plantio_pct"] == 95.0

    def test_finds_colheita(self):
        df = pd.DataFrame(
            [
                ["Progresso", "Percentual"],
                ["Colheita", 30.0],
            ]
        )
        records = _extract_condicao_from_sheet(df, "milho")
        colheita = [r for r in records if r.get("colheita_pct") is not None]
        assert len(colheita) >= 1
        assert colheita[0]["colheita_pct"] == 30.0


class TestParsePcXls:
    def test_basic_parse(self):
        sheets = {
            "Soja": [
                ["Condição das Lavouras", "Data: 15/01/2025"],
                ["Condição", "Percentual"],
                ["Boa", 70.0],
                ["Média", 25.0],
                ["Ruim", 5.0],
            ],
        }
        data = _make_xls_bytes(sheets)
        df = parse_pc_xls(data)
        assert len(df) > 0
        assert "produto" in df.columns
        assert "condicao" in df.columns

    def test_multiple_sheets(self):
        sheets = {
            "Soja": [
                ["Condição", "%"],
                ["Boa", 70.0],
                ["Ruim", 10.0],
            ],
            "Milho": [
                ["Condição", "%"],
                ["Boa", 60.0],
                ["Média", 30.0],
            ],
        }
        data = _make_xls_bytes(sheets)
        df = parse_pc_xls(data)
        produtos = df["produto"].unique().tolist()
        assert "soja" in produtos
        assert "milho" in produtos

    def test_unknown_sheet_skipped(self):
        sheets = {
            "Resumo Geral": [
                ["Titulo", "Valor"],
                ["Total", 100],
            ],
        }
        data = _make_xls_bytes(sheets)
        with pytest.raises(ParseError, match="Nenhum registro reconhecido"):
            parse_pc_xls(data)

    def test_empty_file(self):
        sheets = {"Sheet1": []}
        data = _make_xls_bytes(sheets)
        with pytest.raises(ParseError, match="Nenhum registro reconhecido"):
            parse_pc_xls(data)

    def test_invalid_bytes(self):
        with pytest.raises(ParseError, match="Falha ao abrir PC.xls"):
            parse_pc_xls(b"not a valid excel file")

    def test_data_columns(self):
        sheets = {
            "Soja": [
                ["Condição", "%"],
                ["Boa", 80.0],
            ],
        }
        data = _make_xls_bytes(sheets)
        df = parse_pc_xls(data)
        expected_cols = {"produto", "data", "condicao", "pct", "plantio_pct", "colheita_pct"}
        assert expected_cols.issubset(set(df.columns))

    def test_real_golden_preserves_crop_seasons_and_primary_key(self):
        df = parse_pc_xls(GOLDEN_DIR.joinpath("response.xlsx").read_bytes())

        assert {"feijao_1", "feijao_2", "milho_1", "milho_2"}.issubset(set(df["produto"]))
        assert not df.duplicated(["produto", "data", "condicao"]).any()


class TestFilterByProduto:
    def test_filter(self):
        df = pd.DataFrame(
            [
                {"produto": "soja", "condicao": "boa", "pct": 70.0},
                {"produto": "milho", "condicao": "boa", "pct": 60.0},
                {"produto": "soja", "condicao": "media", "pct": 25.0},
            ]
        )
        result = filter_by_produto(df, "soja")
        assert len(result) == 2
        assert all(result["produto"] == "soja")

    def test_empty_df(self):
        df = pd.DataFrame(columns=["produto", "condicao", "pct"])
        result = filter_by_produto(df, "soja")
        assert result.empty

    def test_empty_produto(self):
        df = pd.DataFrame([{"produto": "soja", "condicao": "boa", "pct": 70.0}])
        result = filter_by_produto(df, "")
        assert len(result) == 1

    def test_nonexistent_produto(self):
        df = pd.DataFrame([{"produto": "soja", "condicao": "boa", "pct": 70.0}])
        result = filter_by_produto(df, "banana")
        assert result.empty

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


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

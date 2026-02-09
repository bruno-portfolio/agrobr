"""Testes para o parser DERAL."""

import io

import openpyxl
import pandas as pd

from agrobr.deral.parser import (
    PARSER_VERSION,
    _extract_condicao_from_sheet,
    _find_data_referencia,
    _safe_float,
    filter_by_produto,
    parse_pc_xls,
)


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
        assert _safe_float(42) == 42.0

    def test_float(self):
        assert _safe_float(3.14) == 3.14

    def test_string(self):
        assert _safe_float("75.5") == 75.5

    def test_br_format(self):
        assert _safe_float("1.234,56") == 1234.56

    def test_percentage(self):
        assert _safe_float("85%") == 85.0

    def test_none(self):
        assert _safe_float(None) is None

    def test_dash(self):
        assert _safe_float("-") is None
        assert _safe_float("–") is None

    def test_empty(self):
        assert _safe_float("") is None

    def test_nd(self):
        assert _safe_float("n.d.") is None


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
        df = parse_pc_xls(data)
        assert df.empty

    def test_empty_file(self):
        sheets = {"Sheet1": []}
        data = _make_xls_bytes(sheets)
        df = parse_pc_xls(data)
        assert df.empty
        assert "produto" in df.columns

    def test_invalid_bytes(self):
        df = parse_pc_xls(b"not a valid excel file")
        assert df.empty

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


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

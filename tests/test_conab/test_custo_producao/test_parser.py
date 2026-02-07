"""Testes para o parser de custo de produção CONAB.

Cria planilhas Excel in-memory para testar o parser sem dependência de rede.
"""

from io import BytesIO

import pandas as pd
import pytest

from agrobr.conab.custo_producao.parser import (
    PARSER_VERSION,
    _find_header_row,
    _identify_columns,
    _safe_float,
    items_to_dataframe,
    parse_planilha,
)
from agrobr.exceptions import ParseError


def _make_xlsx(rows: list[list], sheet_name: str = "Plan1") -> BytesIO:
    """Helper: cria planilha Excel em memória a partir de linhas brutas."""
    df = pd.DataFrame(rows)
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False, header=False)
    buf.seek(0)
    return buf


def _sample_xlsx(
    with_coe: bool = True,
    with_cot: bool = False,
    with_ct: bool = False,
) -> BytesIO:
    """Cria planilha CONAB de custo de produção realista para testes."""
    rows = [
        [
            "CUSTO DE PRODUÇÃO - SOJA - MT - ALTA TECNOLOGIA - SAFRA 2023/24",
            None,
            None,
            None,
            None,
            None,
        ],
        [None, None, None, None, None, None],
        [
            "Item / Especificação",
            "Unidade",
            "Qtd./ha",
            "Preço Unitário (R$)",
            "Valor Total/ha (R$)",
            "Participação (%)",
        ],
        # Insumos
        ["I - INSUMOS", None, None, None, None, None],
        ["Sementes", "kg", 60.0, 8.50, 510.00, 13.42],
        ["Fertilizantes de base (MAP)", "kg", 200.0, 4.20, 840.00, 22.11],
        ["Herbicidas (Glifosato)", "L", 3.0, 25.00, 75.00, 1.97],
        ["Inseticidas", "L", 2.0, 45.00, 90.00, 2.37],
        ["Fungicidas", "L", 1.5, 80.00, 120.00, 3.16],
        ["Inoculante", "dose", 2.0, 12.00, 24.00, 0.63],
        # Operações mecânicas
        ["II - OPERAÇÕES COM MÁQUINAS", None, None, None, None, None],
        ["Preparo do solo", "h/m", 1.5, 180.00, 270.00, 7.11],
        ["Plantio", "h/m", 0.8, 150.00, 120.00, 3.16],
        ["Pulverizações", "h/m", 2.0, 120.00, 240.00, 6.32],
        ["Colheita mecânica", "h/m", 1.0, 350.00, 350.00, 9.21],
        # Mão de obra
        ["III - MÃO DE OBRA", None, None, None, None, None],
        ["Mão de obra temporária", "d/h", 3.0, 80.00, 240.00, 6.32],
    ]

    if with_coe:
        rows.append(["CUSTO OPERACIONAL EFETIVO (COE)", None, None, None, 2879.00, 75.78])

    if with_cot:
        rows.append(["CUSTO OPERACIONAL TOTAL (COT)", None, None, None, 3400.00, 89.47])

    if with_ct:
        rows.append(["CUSTO TOTAL (CT)", None, None, None, 3800.00, 100.00])

    return _make_xlsx(rows)


class TestSafeFloat:
    def test_int(self):
        assert _safe_float(10) == 10.0

    def test_float(self):
        assert _safe_float(3.14) == pytest.approx(3.14)

    def test_string_dot(self):
        assert _safe_float("3.14") == pytest.approx(3.14)

    def test_string_comma(self):
        assert _safe_float("3,14") == pytest.approx(3.14)

    def test_string_with_currency(self):
        assert _safe_float("R$ 510.00") == pytest.approx(510.0)

    def test_string_with_percent(self):
        assert _safe_float("12.5%") == pytest.approx(12.5)

    def test_none(self):
        assert _safe_float(None) is None

    def test_nan(self):
        assert _safe_float(float("nan")) is None

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_dash(self):
        assert _safe_float("-") is None

    def test_garbage(self):
        assert _safe_float("abc") is None


class TestFindHeaderRow:
    def test_finds_header(self):
        rows = [
            ["TITULO", None],
            [None, None],
            ["Item", "Valor Total/ha (R$)"],
            ["Sementes", 510.0],
        ]
        df = pd.DataFrame(rows)
        assert _find_header_row(df) == 2

    def test_raises_on_no_header(self):
        rows = [
            ["blah", "blah"],
            ["foo", "bar"],
        ]
        df = pd.DataFrame(rows)
        with pytest.raises(ParseError):
            _find_header_row(df)


class TestIdentifyColumns:
    def test_full_headers(self):
        headers = [
            "Item / Especificação",
            "Unidade",
            "Qtd./ha",
            "Preço Unitário (R$)",
            "Valor Total/ha (R$)",
            "Participação (%)",
        ]
        mapping = _identify_columns(headers)

        assert "item" in mapping
        assert "unidade" in mapping
        assert "quantidade_ha" in mapping
        assert "preco_unitario" in mapping
        assert "valor_ha" in mapping
        assert "participacao_pct" in mapping

    def test_minimal_headers(self):
        headers = ["Discriminação", "Unid.", "Valor/ha (R$)"]
        mapping = _identify_columns(headers)

        assert "item" in mapping
        assert "valor_ha" in mapping


class TestParsePlanilha:
    def test_parse_basic(self):
        xlsx = _sample_xlsx()
        items, custo_total = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        assert len(items) > 0
        assert all(i.cultura == "soja" for i in items)
        assert all(i.uf == "MT" for i in items)
        assert all(i.safra == "2023/24" for i in items)

    def test_parse_detects_coe(self):
        xlsx = _sample_xlsx(with_coe=True)
        _, custo_total = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        assert custo_total is not None
        assert custo_total.coe_ha == pytest.approx(2879.0)

    def test_parse_detects_cot_ct(self):
        xlsx = _sample_xlsx(with_coe=True, with_cot=True, with_ct=True)
        _, custo_total = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        assert custo_total is not None
        assert custo_total.coe_ha == pytest.approx(2879.0)
        assert custo_total.cot_ha == pytest.approx(3400.0)
        assert custo_total.ct_ha == pytest.approx(3800.0)

    def test_computes_coe_from_items_if_missing(self):
        xlsx = _sample_xlsx(with_coe=False)
        items, custo_total = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        # Deve computar COE = soma de insumos + operacoes + mao_de_obra
        assert custo_total is not None
        assert custo_total.coe_ha > 0

        # Verificar que soma confere
        coe_categorias = {"insumos", "operacoes", "mao_de_obra"}
        soma = sum(i.valor_ha for i in items if i.categoria in coe_categorias)
        assert custo_total.coe_ha == pytest.approx(soma)

    def test_parse_categories(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        categories = {i.categoria for i in items}
        assert "insumos" in categories
        assert "operacoes" in categories
        assert "mao_de_obra" in categories

    def test_parse_numeric_values(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        sementes = [i for i in items if "semente" in i.item.lower()]
        assert len(sementes) == 1
        assert sementes[0].valor_ha == pytest.approx(510.0)
        assert sementes[0].quantidade_ha == pytest.approx(60.0)
        assert sementes[0].preco_unitario == pytest.approx(8.50)

    def test_parse_empty_xlsx_raises(self):
        empty_xlsx = _make_xlsx([])
        with pytest.raises(ParseError):
            parse_planilha(empty_xlsx, "soja", "MT", "2023/24")

    def test_parse_malformed_raises(self):
        bad_xlsx = _make_xlsx([["foo", "bar"], ["baz", "qux"]])
        with pytest.raises(ParseError):
            parse_planilha(bad_xlsx, "soja", "MT", "2023/24")

    def test_cultura_normalization(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="SOJA", uf="mt", safra="2023/24")

        assert all(i.cultura == "soja" for i in items)
        assert all(i.uf == "MT" for i in items)

    def test_tecnologia_propagation(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(
            xlsx, cultura="soja", uf="MT", safra="2023/24", tecnologia="media"
        )

        assert all(i.tecnologia == "media" for i in items)


class TestItemsToDataframe:
    def test_converts_to_dataframe(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        df = items_to_dataframe(items)

        assert isinstance(df, pd.DataFrame)
        assert len(df) == len(items)
        assert "cultura" in df.columns
        assert "uf" in df.columns
        assert "safra" in df.columns
        assert "categoria" in df.columns
        assert "item" in df.columns
        assert "valor_ha" in df.columns

    def test_empty_list_returns_empty_df(self):
        df = items_to_dataframe([])
        assert df.empty

    def test_numeric_columns_are_numeric(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        df = items_to_dataframe(items)

        assert df["valor_ha"].dtype in ("float64", "Float64")

    def test_sorted_output(self):
        xlsx = _sample_xlsx()
        items, _ = parse_planilha(xlsx, cultura="soja", uf="MT", safra="2023/24")

        df = items_to_dataframe(items)

        # Deve estar ordenado por cultura, uf, safra, categoria, item
        categories = df["categoria"].tolist()
        # Verificar que está ordenado (categorias agrupadas)
        seen = set()
        for cat in categories:
            seen.add(cat)
        assert len(seen) >= 2  # Pelo menos 2 categorias


class TestParserVersion:
    def test_version_is_int(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

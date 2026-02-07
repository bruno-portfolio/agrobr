"""Testes para o parser ANDA."""

import pandas as pd

from agrobr.anda.parser import (
    PARSER_VERSION,
    _detect_month,
    _is_uf,
    _parse_generic,
    _parse_indicadores,
    _parse_uf_cols,
    _parse_uf_rows,
    _safe_float,
    agregar_mensal,
    parse_entregas_table,
)


def _uf_rows_table():
    """Tabela com UFs nas linhas e meses nas colunas."""
    return [
        ["UF", "Jan", "Fev", "Mar"],
        ["MT", "150.000", "120.000", "80.000"],
        ["SP", "100.000", "90.000", "60.000"],
        ["PR", "80.000", "70.000", "50.000"],
        ["GO", "60.000", "55.000", "40.000"],
    ]


def _uf_cols_table():
    """Tabela com UFs nas colunas e meses nas linhas."""
    return [
        ["Mês", "MT", "SP", "PR", "GO"],
        ["Janeiro", "150.000", "100.000", "80.000", "60.000"],
        ["Fevereiro", "120.000", "90.000", "70.000", "55.000"],
        ["Março", "80.000", "60.000", "50.000", "40.000"],
    ]


def _generic_table():
    """Tabela genérica com colunas UF, Mês, Toneladas."""
    return [
        ["UF", "Mês", "Toneladas"],
        ["MT", "1", "150000"],
        ["MT", "2", "120000"],
        ["SP", "1", "100000"],
        ["PR", "3", "80000"],
    ]


class TestSafeFloat:
    def test_integer(self):
        assert _safe_float(42) == 42.0

    def test_float(self):
        assert _safe_float(3.14) == 3.14

    def test_string_br_format(self):
        assert _safe_float("150.000") == 150000.0

    def test_string_decimal_comma(self):
        assert _safe_float("1.234,56") == 1234.56

    def test_none(self):
        assert _safe_float(None) is None

    def test_dash(self):
        assert _safe_float("-") is None
        assert _safe_float("–") is None

    def test_empty(self):
        assert _safe_float("") is None

    def test_nd(self):
        assert _safe_float("n.d.") is None


class TestDetectMonth:
    def test_numeric(self):
        assert _detect_month("1") == 1
        assert _detect_month("12") == 12

    def test_name_full(self):
        assert _detect_month("Janeiro") == 1
        assert _detect_month("Fevereiro") == 2
        assert _detect_month("Dezembro") == 12

    def test_name_abbrev(self):
        assert _detect_month("Jan") == 1
        assert _detect_month("Fev") == 2
        assert _detect_month("Mar") == 3

    def test_invalid(self):
        assert _detect_month("UF") is None
        assert _detect_month("Total") is None

    def test_out_of_range(self):
        assert _detect_month("13") is None
        assert _detect_month("0") is None

    def test_acumulado_janeiro_a_dezembro(self):
        assert _detect_month("Janeiro a Dezembro") is None

    def test_acumulado_jan_a_dez(self):
        assert _detect_month("Jan a Dez") is None

    def test_acumulado_jan_dez_slash(self):
        assert _detect_month("Jan/Dez") is None

    def test_acumulado_total_do_ano(self):
        assert _detect_month("Total do Ano") is None

    def test_acumulado_acumulado(self):
        assert _detect_month("Acumulado") is None

    def test_acumulado_acumulado_no_ano(self):
        assert _detect_month("Acumulado no Ano") is None

    def test_acumulado_anual(self):
        assert _detect_month("Anual") is None

    def test_valid_months_still_work(self):
        """Garante que meses validos continuam funcionando apos filtro de acumulado."""
        assert _detect_month("Janeiro") == 1
        assert _detect_month("Dezembro") == 12
        assert _detect_month("Jun") == 6


class TestIsUf:
    def test_valid(self):
        assert _is_uf("MT")
        assert _is_uf("SP")
        assert _is_uf("PR")

    def test_invalid(self):
        assert not _is_uf("XX")
        assert not _is_uf("Total")
        assert not _is_uf("")


class TestParseUfRows:
    def test_basic(self):
        records = _parse_uf_rows(_uf_rows_table(), 2024, "total")

        assert len(records) == 12  # 4 UFs × 3 meses
        mt_jan = [r for r in records if r["uf"] == "MT" and r["mes"] == 1]
        assert len(mt_jan) == 1
        assert mt_jan[0]["volume_ton"] == 150000.0
        assert mt_jan[0]["ano"] == 2024

    def test_empty_table(self):
        records = _parse_uf_rows([["UF", "Jan"]], 2024, "total")
        assert records == []


class TestParseUfCols:
    def test_basic(self):
        records = _parse_uf_cols(_uf_cols_table(), 2024, "total")

        assert len(records) == 12  # 3 meses × 4 UFs
        sp_fev = [r for r in records if r["uf"] == "SP" and r["mes"] == 2]
        assert len(sp_fev) == 1
        assert sp_fev[0]["volume_ton"] == 90000.0


class TestParseGeneric:
    def test_basic(self):
        records = _parse_generic(_generic_table(), 2024, "total")

        assert len(records) == 4
        mt_records = [r for r in records if r["uf"] == "MT"]
        assert len(mt_records) == 2


class TestParseEntregasTable:
    def test_auto_detects_uf_rows(self):
        records = parse_entregas_table(_uf_rows_table(), 2024)
        assert len(records) == 12

    def test_auto_detects_uf_cols(self):
        records = parse_entregas_table(_uf_cols_table(), 2024)
        assert len(records) == 12

    def test_generic_fallback(self):
        records = parse_entregas_table(_generic_table(), 2024)
        assert len(records) == 4

    def test_empty_table(self):
        records = parse_entregas_table([], 2024)
        assert records == []

    def test_single_row(self):
        records = parse_entregas_table([["UF", "Jan"]], 2024)
        assert records == []

    def test_product_passthrough(self):
        records = parse_entregas_table(_uf_rows_table(), 2024, produto="ureia")
        for r in records:
            assert r["produto_fertilizante"] == "ureia"


class TestAgregarMensal:
    def test_basic(self):
        data = [
            {
                "ano": 2024,
                "mes": 1,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 150000,
            },
            {
                "ano": 2024,
                "mes": 1,
                "uf": "SP",
                "produto_fertilizante": "total",
                "volume_ton": 100000,
            },
            {
                "ano": 2024,
                "mes": 2,
                "uf": "MT",
                "produto_fertilizante": "total",
                "volume_ton": 120000,
            },
        ]
        df = pd.DataFrame(data)
        result = agregar_mensal(df)

        assert len(result) == 2
        jan = result[result["mes"] == 1].iloc[0]
        assert jan["volume_ton"] == 250000

    def test_empty(self):
        result = agregar_mensal(pd.DataFrame())
        assert result.empty


def _indicadores_single_section():
    """Tabela 'Principais Indicadores' com apenas uma secao (entregas)."""
    return [
        ["", "", "2021", "2022"],
        ["", "Janeiro", "3.397.952", "3.200.000"],
        ["", "Fevereiro", "2.800.000", "2.600.000"],
        ["", "Março", "3.100.000", "2.900.000"],
        ["", "Abril", "3.400.000", "3.100.000"],
        ["", "Maio", "3.300.000", "3.000.000"],
        ["", "Junho", "3.500.000", "3.200.000"],
        ["", "Julho", "4.100.000", "3.800.000"],
        ["", "Agosto", "4.200.000", "3.900.000"],
        ["", "Setembro", "4.500.000", "4.100.000"],
        ["", "Outubro", "5.300.000", "4.800.000"],
        ["", "Novembro", "4.800.000", "4.400.000"],
        ["", "Dezembro", "3.500.000", "3.100.000"],
        ["", "Janeiro a Dezembro", "45.897.952", "42.100.000"],
    ]


def _indicadores_multi_section():
    """Tabela multi-secao simulando PDF real (entregas + producao + importacao)."""
    return [
        # Secao 1: Entregas ao Mercado
        ["", "", "2021", "2022"],
        ["", "Janeiro", "3.397.952", "3.200.000"],
        ["", "Fevereiro", "2.800.000", "2.600.000"],
        ["", "Março", "3.100.000", "2.900.000"],
        ["", "Abril", "3.400.000", "3.100.000"],
        ["", "Maio", "3.300.000", "3.000.000"],
        ["", "Junho", "3.500.000", "3.200.000"],
        ["", "Julho", "4.100.000", "3.800.000"],
        ["", "Agosto", "4.200.000", "3.900.000"],
        ["", "Setembro", "4.500.000", "4.100.000"],
        ["", "Outubro", "5.300.000", "4.800.000"],
        ["", "Novembro", "4.800.000", "4.400.000"],
        ["", "Dezembro", "3.500.000", "3.100.000"],
        ["", "Janeiro a Dezembro", "45.897.952", "42.100.000"],
        # Secao 2: Producao Nacional (titulo longo = sinal de nova secao)
        ["Producao Nacional de Fertilizantes Intermediarios (em toneladas)", "", "", ""],
        ["", "", "2021", "2022"],
        ["", "Janeiro", "700.000", "650.000"],
        ["", "Fevereiro", "600.000", "550.000"],
        ["", "Março", "650.000", "600.000"],
        ["", "Abril", "680.000", "620.000"],
        ["", "Maio", "660.000", "610.000"],
        ["", "Junho", "700.000", "640.000"],
        ["", "Julho", "720.000", "660.000"],
        ["", "Agosto", "730.000", "670.000"],
        ["", "Setembro", "710.000", "650.000"],
        ["", "Outubro", "740.000", "680.000"],
        ["", "Novembro", "750.000", "690.000"],
        ["", "Dezembro", "700.000", "640.000"],
    ]


class TestParseIndicadores:
    def test_single_section_12_records(self):
        records = _parse_indicadores(_indicadores_single_section(), 2022, "total")
        assert len(records) == 12

    def test_single_section_values(self):
        records = _parse_indicadores(_indicadores_single_section(), 2022, "total")
        jan = [r for r in records if r["mes"] == 1][0]
        assert jan["volume_ton"] == 3200000.0
        dez = [r for r in records if r["mes"] == 12][0]
        assert dez["volume_ton"] == 3100000.0

    def test_single_section_uf_is_br(self):
        records = _parse_indicadores(_indicadores_single_section(), 2022, "total")
        assert all(r["uf"] == "BR" for r in records)

    def test_acumulado_excluded(self):
        """'Janeiro a Dezembro' nao deve gerar registro."""
        records = _parse_indicadores(_indicadores_single_section(), 2022, "total")
        assert len(records) == 12  # nao 13

    def test_multi_section_only_first(self):
        """Deve parar na primeira secao e ignorar producao/importacao."""
        records = _parse_indicadores(_indicadores_multi_section(), 2022, "total")
        assert len(records) == 12

    def test_multi_section_volume_is_entregas(self):
        """Volumes devem ser da secao entregas, nao producao."""
        records = _parse_indicadores(_indicadores_multi_section(), 2022, "total")
        jan = [r for r in records if r["mes"] == 1][0]
        # Entregas Janeiro = 3.200.000, Producao Janeiro = 650.000
        assert jan["volume_ton"] == 3200000.0

    def test_wrong_year_returns_empty(self):
        records = _parse_indicadores(_indicadores_single_section(), 2025, "total")
        assert records == []

    def test_empty_table(self):
        records = _parse_indicadores([], 2022, "total")
        assert records == []

    def test_product_passthrough(self):
        records = _parse_indicadores(_indicadores_single_section(), 2022, "ureia")
        assert all(r["produto_fertilizante"] == "ureia" for r in records)


class TestParserVersion:
    def test_version(self):
        assert isinstance(PARSER_VERSION, int)
        assert PARSER_VERSION >= 1

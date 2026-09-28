from __future__ import annotations

import math
from decimal import Decimal

import pytest

from agrobr.normalize.numeric import parse_numeric_br, safe_float
from tests.helpers import collect_failures


class TestPassthrough:
    def test_bool_false(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_int", 42, 42.0),
                ("test_float", 42.5, 42.5),
                ("test_zero_int", 0, 0.0),
                ("test_bool_true", True, 1.0),
                ("test_bool_false", False, 0.0),
            ]:
                with check(case):
                    assert parse_numeric_br(value) == expected
            with check("test_nan_passthrough"):
                result = parse_numeric_br(float("nan"))
                assert result is not None
                assert math.isnan(result)


class TestFormatoBR:
    def test_decimal_br_pequeno(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_milhar_e_decimal", "1.234,56", 1234.56),
                ("test_virgula_decimal_sem_milhar", "1234,56", 1234.56),
                ("test_negativo_br", "-1.234,56", -1234.56),
                ("test_multiplos_grupos_milhar", "1.234.567.890,99", 1234567890.99),
                ("test_decimal_br_pequeno", "0,001", 0.001),
                ("test_valor_real_anp", "3517,6", 3517.6),
            ]:
                with check(case):
                    assert parse_numeric_br(value) == expected
            with check("test_valor_real_anp_milhar"):
                assert parse_numeric_br("500.000,50") == pytest.approx(500000.50)


class TestStringsSimples:
    def test_formato_us_passthrough(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_inteiro_string", "50000", 50000.0),
                ("test_zero_string", "0", 0.0),
                ("test_negativo_dot", "-42.5", -42.5),
                ("test_formato_us_passthrough", "1234.56", 1234.56),
            ]:
                with check(case):
                    assert parse_numeric_br(value) == expected


class TestInvalidos:
    def test_em_dash(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_texto", "abc", None),
                ("test_en_dash", "–", None),
                ("test_em_dash", "—", None),
                ("test_so_virgula", ",", None),
                ("test_so_ponto", ".", None),
                ("test_formato_us_milhares", "1,234,567", None),
            ]:
                with check(case):
                    assert parse_numeric_br(value) is expected


class TestSafeFloatGuards:
    @pytest.mark.parametrize("value", ["NaN", " nan ", "+NaN", "-NaN", Decimal("NaN")])
    @pytest.mark.parametrize("nan_as_none", [True, False])
    def test_nan_conversion_respects_option(self, value, nan_as_none):
        result = safe_float(value, nan_as_none=nan_as_none)
        if nan_as_none:
            assert result is None
        else:
            assert result is not None and math.isnan(result)

    def test_empty_string(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_none", None, None),
                ("test_nan_as_none", float("nan"), None),
                ("test_empty_string", "", None),
                ("test_whitespace", "   ", None),
            ]:
                with check(case):
                    assert safe_float(value) is expected
            with check("test_nan_passthrough"):
                result = safe_float(float("nan"), nan_as_none=False)
                assert result is not None
                assert math.isnan(result)
            for case, value, expected in [
                ("test_int", 42, 42.0),
                ("test_float", 3.14, 3.14),
            ]:
                with check(case):
                    assert safe_float(value) == expected


class TestSafeFloatNullMarkers:
    def test_custom_markers(self):
        with collect_failures() as check:
            with check("test_custom_markers"):
                assert safe_float("N/A", null_markers=frozenset({"n/a"})) is None
            with check("test_custom_markers_numeric_sentinel"):
                assert safe_float("99", null_markers=frozenset({"99"})) is None
                assert safe_float("99") == 99.0


class TestSafeFloatZeroAsNone:
    def test_zero_float(self):
        with collect_failures() as check:
            for case, value, treat_zero_as_none, expected in [
                ("test_zero_float", 0.0, True, None),
                ("test_zero_int", 0, True, None),
                ("test_zero_string", "0", True, None),
            ]:
                with check(case):
                    assert safe_float(value, treat_zero_as_none=treat_zero_as_none) is expected
            with check("test_zero_preserved_by_default"):
                assert safe_float(0) == 0.0
                assert safe_float("0") == 0.0


class TestSafeFloatBRFormat:
    def test_abiove_3digit_heuristic(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_comma_and_dot", "1.234,56", 1234.56),
                ("test_comma_only", "1234,56", 1234.56),
                ("test_multiple_dots", "1.234.567", 1234567.0),
                ("test_decimal_passthrough_2digits", "3.14", 3.14),
                ("test_decimal_passthrough_4digits", "3.1416", 3.1416),
                ("test_spaces_stripped", "  1.234,56  ", 1234.56),
                ("test_internal_spaces", "1 234,56", 1234.56),
            ]:
                with check(case):
                    assert safe_float(value) == expected
            with check("test_abiove_3digit_heuristic"):
                assert safe_float("150.000") == 150000.0
                assert safe_float("12.500") == 12500.0


class TestSafeFloatInvalid:
    def test_only_comma(self):
        with collect_failures() as check:
            for case, value, expected in [
                ("test_text", "abc", None),
                ("test_only_comma", ",", None),
                ("test_only_dot", ".", None),
            ]:
                with check(case):
                    assert safe_float(value) is expected


@pytest.mark.parametrize(
    ("valor", "esperado"),
    [
        (1.234, 1.234),
        (2.5, 2.5),
        ("12.5", 12.5),
        ("1.234", 1234.0),
        ("1.234.567", 1234567.0),
        ("1.234,56", 1234.56),
        ("1234,56", 1234.56),
    ],
)
def test_safe_float_separadores_e_tipos(valor, esperado):
    assert safe_float(valor) == esperado

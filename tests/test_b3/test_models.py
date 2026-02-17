from __future__ import annotations

import pytest

from agrobr.b3.models import (
    B3_CONTRATOS_AGRO,
    B3_CONTRATOS_AGRO_INV,
    COLUNAS_OI_SAIDA,
    MONTH_CODES,
    TICKER_PARA_CONTRATO,
    TICKERS_AGRO,
    TICKERS_AGRO_OI,
    UNIDADES,
    parse_numero_br,
    parse_vencimento,
)


class TestContratosAgro:
    def test_all_tickers_unique(self):
        assert len(B3_CONTRATOS_AGRO) == len(set(B3_CONTRATOS_AGRO.values()))

    def test_tickers_agro_matches_values(self):
        assert set(B3_CONTRATOS_AGRO.values()) == TICKERS_AGRO

    def test_ticker_para_contrato_is_inverse(self):
        for nome, ticker in B3_CONTRATOS_AGRO.items():
            assert TICKER_PARA_CONTRATO[ticker] == nome

    def test_all_tickers_have_unidades(self):
        for ticker in TICKERS_AGRO:
            assert ticker in UNIDADES


class TestMonthCodes:
    def test_twelve_months(self):
        assert len(MONTH_CODES) == 12

    def test_values_1_to_12(self):
        assert set(MONTH_CODES.values()) == set(range(1, 13))

    def test_standard_codes(self):
        assert MONTH_CODES["F"] == 1
        assert MONTH_CODES["Z"] == 12
        assert MONTH_CODES["H"] == 3
        assert MONTH_CODES["N"] == 7


class TestParseVencimento:
    def test_g25(self):
        assert parse_vencimento("G25") == (2025, 2)

    def test_h25(self):
        assert parse_vencimento("H25") == (2025, 3)

    def test_z26(self):
        assert parse_vencimento("Z26") == (2026, 12)

    def test_f30(self):
        assert parse_vencimento("F30") == (2030, 1)

    def test_strips_whitespace(self):
        assert parse_vencimento(" K25 ") == (2025, 5)

    def test_invalid_letter_raises(self):
        with pytest.raises(KeyError):
            parse_vencimento("A25")

    def test_invalid_format_raises(self):
        with pytest.raises((ValueError, IndexError)):
            parse_vencimento("")


class TestParseNumeroBr:
    def test_simple_decimal(self):
        assert parse_numero_br("311,45") == 311.45

    def test_thousands_separator(self):
        assert parse_numero_br("1.006,50") == 1006.50

    def test_millions(self):
        assert parse_numero_br("1.066.587,4000") == 1066587.4000

    def test_negative(self):
        assert parse_numero_br("-2,40") == -2.40

    def test_integer_no_separator(self):
        assert parse_numero_br("63") == 63.0

    def test_four_decimals(self):
        assert parse_numero_br("23,7379") == pytest.approx(23.7379)

    def test_empty_returns_none(self):
        assert parse_numero_br("") is None

    def test_dash_returns_none(self):
        assert parse_numero_br("-") is None

    def test_whitespace_stripped(self):
        assert parse_numero_br("  311,45  ") == 311.45


class TestColunasOiSaida:
    def test_is_list(self):
        assert isinstance(COLUNAS_OI_SAIDA, list)

    def test_has_required_columns(self):
        required = ["data", "ticker", "ticker_completo", "tipo", "posicoes_abertas"]
        for col in required:
            assert col in COLUNAS_OI_SAIDA

    def test_eleven_columns(self):
        assert len(COLUNAS_OI_SAIDA) == 11


class TestTickersAgroOi:
    def test_is_set(self):
        assert isinstance(TICKERS_AGRO_OI, set)

    def test_contains_main_agro(self):
        for t in ["BGI", "CCM", "ETH", "ICF", "SJC"]:
            assert t in TICKERS_AGRO_OI

    def test_subset_of_tickers_agro_plus_cnl(self):
        assert TICKERS_AGRO_OI.issubset(TICKERS_AGRO | {"CNL"})


class TestContratosAgroInv:
    def test_is_inverse_of_contratos(self):
        for nome, ticker in B3_CONTRATOS_AGRO.items():
            assert B3_CONTRATOS_AGRO_INV[ticker] == nome

    def test_same_length(self):
        assert len(B3_CONTRATOS_AGRO_INV) == len(B3_CONTRATOS_AGRO)

    def test_bgi_maps_to_boi(self):
        assert B3_CONTRATOS_AGRO_INV["BGI"] == "boi"

    def test_ccm_maps_to_milho(self):
        assert B3_CONTRATOS_AGRO_INV["CCM"] == "milho"

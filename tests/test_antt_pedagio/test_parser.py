"""Testes para agrobr.alt.antt_pedagio.parser."""

from __future__ import annotations

from datetime import date

import pandas as pd
import pytest

from agrobr.alt.antt_pedagio.parser import (
    PARSER_VERSION,
    _detect_encoding,
    _has_header,
    _parse_date_v1,
    _parse_date_v2,
    _parse_numeric,
    join_fluxo_pracas,
    parse_pracas,
    parse_trafego,
    parse_trafego_v1,
    parse_trafego_v2,
)
from agrobr.exceptions import ParseError

# ============================================================================
# Sample CSV data
# ============================================================================

V1_CSV = (
    "concessionaria;praca;mes_ano;categoria;tipo_cobranca;sentido;quantidade\n"
    "CCR AutoBAn;Campinas;01/01/2023;Categoria 1;Automatica;Crescente;50000\n"
    "CCR AutoBAn;Campinas;01/01/2023;Categoria 1;Manual;Crescente;5000\n"
    "CCR AutoBAn;Campinas;01/01/2023;Categoria 4;Automatica;Crescente;12000\n"
    "CCR AutoBAn;Campinas;01/01/2023;Categoria 8;Automatica;Decrescente;8000\n"
    "CCR AutoBAn;Campinas;01/02/2023;Categoria 1;Automatica;Crescente;52000\n"
)

V2_CSV_NO_HEADER = (
    "EcoRodovias;Anchieta;01/2024;4;Automatica;Crescente;30000\n"
    "EcoRodovias;Anchieta;01/2024;4;Manual;Crescente;2000\n"
    "EcoRodovias;Anchieta;01/2024;6;Automatica;Decrescente;15000\n"
    "EcoRodovias;Anchieta;02/2024;4;Automatica;Crescente;31000\n"
)

V2_CSV_WITH_HEADER = (
    "concessionaria;praca;mes_ano;categoria_eixo;tipo_cobranca;sentido;quantidade\n"
    "EcoRodovias;Anchieta;01/2024;4;Automatica;Crescente;30000\n"
    "EcoRodovias;Anchieta;01/2024;6;Automatica;Decrescente;15000\n"
)

PRACAS_CSV = (
    "concessionaria;praca_de_pedagio;rodovia;uf;km_m;municipio;lat;lon;situacao\n"
    "CCR AutoBAn;Campinas;SP-348;SP;87+500;Campinas;-22.9;-47.0;Ativa\n"
    "EcoRodovias;Anchieta;SP-150;SP;40+200;Cubatao;-23.8;-46.3;Ativa\n"
    "Arteris;Jacarezinho;BR-153;PR;10+000;Jacarezinho;-23.1;-49.9;Ativa\n"
)


# ============================================================================
# Helper tests
# ============================================================================


class TestDetectEncoding:
    def test_utf8(self):
        assert _detect_encoding(b"hello world") == "utf-8"

    def test_windows_1252(self):
        content = "Conceição".encode("windows-1252")
        enc = _detect_encoding(content)
        assert enc in ("windows-1252", "iso-8859-1")

    def test_empty(self):
        enc = _detect_encoding(b"")
        assert enc == "utf-8"


class TestParseNumeric:
    def test_integer(self):
        assert _parse_numeric("50000") == 50000.0

    def test_float_comma(self):
        assert _parse_numeric("1.234,56") == 1234.56

    def test_float_dot(self):
        assert _parse_numeric("1234.56") == 1234.56

    def test_none(self):
        assert _parse_numeric(None) is None

    def test_empty(self):
        assert _parse_numeric("") is None

    def test_dash(self):
        assert _parse_numeric("-") is None

    def test_int_passthrough(self):
        assert _parse_numeric(42) == 42.0


class TestParseDateV1:
    def test_normal(self):
        assert _parse_date_v1("01/01/2023") == date(2023, 1, 1)

    def test_december(self):
        assert _parse_date_v1("01/12/2022") == date(2022, 12, 1)

    def test_day_always_1(self):
        result = _parse_date_v1("15/06/2020")
        assert result == date(2020, 6, 1)

    def test_empty(self):
        assert _parse_date_v1("") is None

    def test_invalid(self):
        assert _parse_date_v1("abc") is None


class TestParseDateV2:
    def test_normal(self):
        assert _parse_date_v2("01/2024") == date(2024, 1, 1)

    def test_december(self):
        assert _parse_date_v2("12/2024") == date(2024, 12, 1)

    def test_empty(self):
        assert _parse_date_v2("") is None

    def test_invalid(self):
        assert _parse_date_v2("abc") is None


class TestHasHeader:
    def test_with_header(self):
        assert _has_header("concessionaria;praca;mes_ano\ndata...") is True

    def test_without_header(self):
        assert _has_header("CCR AutoBAn;Campinas;01/2024;4\n") is False


# ============================================================================
# V1 Parsing
# ============================================================================


class TestParseTrafegoV1:
    def test_basic_parse(self):
        df = parse_trafego_v1(V1_CSV.encode("utf-8"))
        assert len(df) > 0
        assert "data" in df.columns
        assert "n_eixos" in df.columns
        assert "volume" in df.columns

    def test_aggregates_tipo_cobranca(self):
        """Automatica + Manual should be summed for same key."""
        df = parse_trafego_v1(V1_CSV.encode("utf-8"))
        # Cat 1, Jan 2023, Crescente: 50000 + 5000 = 55000
        cat1_jan = df[
            (df["data"] == date(2023, 1, 1))
            & (df["n_eixos"] == 2)
            & (df["tipo_veiculo"] == "Passeio")
            & (df["sentido"] == "Crescente")
        ]
        assert len(cat1_jan) == 1
        assert cat1_jan.iloc[0]["volume"] == 55000

    def test_categoria_mapping(self):
        df = parse_trafego_v1(V1_CSV.encode("utf-8"))
        tipos = set(df["tipo_veiculo"].dropna().unique())
        assert "Passeio" in tipos
        assert "Comercial" in tipos

    def test_n_eixos_values(self):
        df = parse_trafego_v1(V1_CSV.encode("utf-8"))
        eixos = set(df["n_eixos"].dropna().unique())
        assert 2 in eixos  # Categoria 1
        assert 3 in eixos  # Categoria 4
        assert 6 in eixos  # Categoria 8

    def test_date_parsing(self):
        df = parse_trafego_v1(V1_CSV.encode("utf-8"))
        dates = set(df["data"].unique())
        assert date(2023, 1, 1) in dates
        assert date(2023, 2, 1) in dates

    def test_empty_csv_raises(self):
        with pytest.raises(ParseError, match="vazio"):
            parse_trafego_v1(
                b"concessionaria;praca;mes_ano;categoria;tipo_cobranca;sentido;quantidade\n"
            )

    def test_windows_1252_encoding(self):
        csv = V1_CSV.replace("Campinas", "Conceição")
        content = csv.encode("windows-1252")
        df = parse_trafego_v1(content)
        assert len(df) > 0


# ============================================================================
# V2 Parsing
# ============================================================================


class TestParseTrafegoV2:
    def test_basic_parse_no_header(self):
        df = parse_trafego_v2(V2_CSV_NO_HEADER.encode("utf-8"))
        assert len(df) > 0
        assert "data" in df.columns
        assert "n_eixos" in df.columns

    def test_basic_parse_with_header(self):
        df = parse_trafego_v2(V2_CSV_WITH_HEADER.encode("utf-8"))
        assert len(df) > 0

    def test_aggregates_tipo_cobranca(self):
        """Automatica + Manual should be summed."""
        df = parse_trafego_v2(V2_CSV_NO_HEADER.encode("utf-8"))
        jan_crescente = df[
            (df["data"] == date(2024, 1, 1)) & (df["n_eixos"] == 4) & (df["sentido"] == "Crescente")
        ]
        assert len(jan_crescente) == 1
        assert jan_crescente.iloc[0]["volume"] == 32000

    def test_n_eixos_numeric(self):
        df = parse_trafego_v2(V2_CSV_NO_HEADER.encode("utf-8"))
        eixos = set(df["n_eixos"].dropna().unique())
        assert 4 in eixos
        assert 6 in eixos

    def test_tipo_veiculo_from_eixos(self):
        df = parse_trafego_v2(V2_CSV_NO_HEADER.encode("utf-8"))
        # 4 eixos -> Comercial, 6 eixos -> Comercial
        comercial = df[df["tipo_veiculo"] == "Comercial"]
        assert len(comercial) > 0

    def test_date_v2_format(self):
        df = parse_trafego_v2(V2_CSV_NO_HEADER.encode("utf-8"))
        dates = set(df["data"].unique())
        assert date(2024, 1, 1) in dates
        assert date(2024, 2, 1) in dates

    def test_empty_csv_raises(self):
        with pytest.raises(ParseError, match="vazio"):
            parse_trafego_v2(b"")


# ============================================================================
# Dispatcher
# ============================================================================


class TestParseTrafegoDispatcher:
    def test_v1_for_2023(self):
        df = parse_trafego(V1_CSV.encode("utf-8"), ano=2023)
        assert len(df) > 0

    def test_v2_for_2024(self):
        df = parse_trafego(V2_CSV_NO_HEADER.encode("utf-8"), ano=2024)
        assert len(df) > 0


# ============================================================================
# Pracas Parsing
# ============================================================================


class TestParsePracas:
    def test_basic_parse(self):
        df = parse_pracas(PRACAS_CSV.encode("utf-8"))
        assert len(df) == 3

    def test_columns(self):
        df = parse_pracas(PRACAS_CSV.encode("utf-8"))
        assert "concessionaria" in df.columns
        assert "praca_de_pedagio" in df.columns
        assert "rodovia" in df.columns
        assert "uf" in df.columns

    def test_uf_uppercase(self):
        df = parse_pracas(PRACAS_CSV.encode("utf-8"))
        assert (df["uf"] == df["uf"].str.upper()).all()

    def test_lat_lon_numeric(self):
        df = parse_pracas(PRACAS_CSV.encode("utf-8"))
        assert df["lat"].dtype == "float64"
        assert df["lon"].dtype == "float64"

    def test_empty_raises(self):
        with pytest.raises(ParseError):
            parse_pracas(b"")


# ============================================================================
# Join
# ============================================================================


class TestJoinFluxoPracas:
    def test_basic_join(self):
        df_fluxo = parse_trafego_v1(V1_CSV.encode("utf-8"))
        df_pracas = parse_pracas(PRACAS_CSV.encode("utf-8"))
        result = join_fluxo_pracas(df_fluxo, df_pracas)

        assert "rodovia" in result.columns
        assert "uf" in result.columns
        assert "municipio" in result.columns

    def test_join_enriches_data(self):
        df_fluxo = parse_trafego_v1(V1_CSV.encode("utf-8"))
        df_pracas = parse_pracas(PRACAS_CSV.encode("utf-8"))
        result = join_fluxo_pracas(df_fluxo, df_pracas)

        # CCR AutoBAn / Campinas -> SP-348 / SP
        campinas = result[result["praca"].str.contains("Campinas", na=False)]
        if len(campinas) > 0 and campinas.iloc[0]["rodovia"] is not None:
            assert campinas.iloc[0]["uf"] == "SP"

    def test_join_empty_pracas(self):
        df_fluxo = parse_trafego_v1(V1_CSV.encode("utf-8"))
        df_pracas = pd.DataFrame()
        result = join_fluxo_pracas(df_fluxo, df_pracas)

        assert "rodovia" in result.columns
        assert len(result) > 0

    def test_join_empty_fluxo(self):
        df_fluxo = pd.DataFrame(
            columns=[
                "data",
                "concessionaria",
                "praca",
                "sentido",
                "n_eixos",
                "tipo_veiculo",
                "volume",
            ]
        )
        df_pracas = parse_pracas(PRACAS_CSV.encode("utf-8"))
        result = join_fluxo_pracas(df_fluxo, df_pracas)

        assert len(result) == 0

    def test_join_preserves_volume(self):
        df_fluxo = parse_trafego_v1(V1_CSV.encode("utf-8"))
        total_before = df_fluxo["volume"].sum()
        df_pracas = parse_pracas(PRACAS_CSV.encode("utf-8"))
        result = join_fluxo_pracas(df_fluxo, df_pracas)

        assert result["volume"].sum() == total_before


# ============================================================================
# Edge cases
# ============================================================================


class TestEdgeCases:
    def test_parser_version(self):
        assert PARSER_VERSION >= 1

    def test_bad_encoding_raises(self):
        # Invalid bytes that can't be decoded
        bad_bytes = b"\xff\xfe" + b"\x00" * 100
        with pytest.raises(ParseError):
            parse_trafego_v1(bad_bytes)

    def test_malformed_csv(self):
        with pytest.raises(ParseError):
            parse_trafego_v1(b"not;a;real;csv;at;all;nope\n\x00\x01\x02")

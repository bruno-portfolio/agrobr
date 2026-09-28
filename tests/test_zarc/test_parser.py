from __future__ import annotations

import pandas as pd
import pytest

from agrobr import constants
from agrobr.exceptions import InvalidParameterError, ParseError
from agrobr.zarc import parser, query
from tests.helpers import zarc_csv


@pytest.mark.parametrize("content", [b"", b"\xef\xbb\xbf", b"\n", b"<html>Failure</html>"])
def test_invalid_empty_or_non_csv_body(content):
    with pytest.raises(ParseError):
        parser.parse_tabua_risco(content)


def test_header_only_is_typed_empty():
    result = parser.parse_tabua_risco_bundle(zarc_csv([]))
    assert result.frame.empty
    assert result.frame[list(constants.ZARC_INTEGER_COLUMNS)].dtypes.eq("Int64").all()
    assert result.frame[list(constants.ZARC_STRING_COLUMNS)].dtypes.eq(object).all()
    assert result.details["warnings"]


@pytest.mark.parametrize("change", ["missing", "extra", "duplicate"])
def test_invalid_header(change):
    columns = list(constants.ZARC_CSV_COLUMNS)
    if change == "missing":
        columns.remove("dec36")
    elif change == "extra":
        columns.append("future")
    else:
        columns[-1] = columns[-2]
    with pytest.raises(ParseError, match="Cabeçalho"):
        parser.parse_tabua_risco(zarc_csv(headers=columns))


def test_reordered_complete_header_preserves_values():
    normal = parser.parse_tabua_risco(zarc_csv([{"dec1": "20", "dec36": "50"}]))
    reordered = parser.parse_tabua_risco(
        zarc_csv(
            [{"dec1": "20", "dec36": "50"}], headers=list(reversed(constants.ZARC_CSV_COLUMNS))
        )
    )
    pd.testing.assert_frame_equal(normal, reordered)


@pytest.mark.parametrize("suffix", [b";extra", b'\n"unterminated', b"\n;\n"])
def test_bad_body_after_valid_record(suffix):
    with pytest.raises(ParseError):
        parser.parse_tabua_risco(zarc_csv().rstrip(b"\n") + suffix)


@pytest.mark.parametrize(
    "value",
    [
        " ",
        "NA",
        "NULL",
        "NaN",
        "inf",
        "-1",
        "20.0",
        "20,0",
        "2e1",
        "21",
        "60",
        "２０",
        "9223372036854775808",
    ],
)
def test_invalid_risk_outside_selected_uf(value):
    content = zarc_csv([{}, {"UF": "SP", "dec36": value}])
    with pytest.raises(ParseError, match="Registro 2.*dec36"):
        parser.parse_tabua_risco_bundle(content, query=query.build_query(uf="MT"))


@pytest.mark.parametrize(
    "field,value",
    [
        ("Cod_Solo", ""),
        ("Cod_Ciclo", ""),
        ("Cod_Solo", "4"),
        ("Cod_Ciclo", "23"),
        ("Cod_Solo", "1.5"),
        ("Cod_Cultura", ""),
        ("Cod_NM", "nan"),
        ("geocodigo", "123"),
        ("UF", "ZZ"),
        ("Nome_cultura", " "),
        ("Portaria", ""),
    ],
)
def test_invalid_dimension_outside_selected_uf(field, value):
    with pytest.raises(ParseError, match="Registro 2"):
        parser.parse_tabua_risco_bundle(
            zarc_csv([{}, {"UF": "SP", field: value}]), query=query.build_query(uf="MT")
        )


def test_risk_empty_zero_and_fifty_are_distinct():
    result = parser.parse_tabua_risco_bundle(zarc_csv([{"dec1": "", "dec2": "0", "dec3": "50"}]))
    assert pd.isna(result.frame.loc[0, "dec1"])
    assert result.frame.loc[0, "dec2"] == 0
    assert result.frame.loc[0, "dec3"] == 50
    assert result.details["risk_statistics"]["dec1"]["empty_count"] == 1
    assert result.details["risk_statistics"]["dec2"]["zero_count"] == 1


def test_numeric_surrounding_spaces_are_explicitly_accepted():
    frame = parser.parse_tabua_risco(
        zarc_csv([{"dec1": " 20 ", "Cod_Solo": " 1 ", "Cod_Ciclo": " 20 "}])
    )
    assert frame.loc[0, "dec1"] == 20
    assert frame.loc[0, "solo_codigo"] == 1
    assert frame.loc[0, "ciclo_codigo"] == 20


def test_original_text_and_leading_zeros_preserved():
    frame = parser.parse_tabua_risco(
        zarc_csv(
            [
                {
                    "Nome_cultura": " Soja ",
                    "municipio": "  Sorriso  ",
                    "Cod_Munic": "000123",
                    "Produtividade": "0,75",
                }
            ]
        )
    )
    assert frame.loc[0, "cultura"] == "soja"
    assert frame.loc[0, "cultura_original"] == " Soja "
    assert frame.loc[0, "municipio"] == "  Sorriso  "
    assert frame.loc[0, "municipio_sicor_codigo"] == "000123"
    assert frame.loc[0, "produtividade_texto"] == "0,75"


@pytest.mark.parametrize(
    "label,expected", [("PERENE", "perene"), ("OLERÍCOLA", "olericola"), ("SEM SAFRA", "sem_safra")]
)
def test_grouped_resource_preserves_three_modalities(label, expected):
    result = parser.parse_tabua_risco_bundle(
        zarc_csv([{"SafraIni": "", "SafraFin": label}]), expected_safra="perene"
    )
    assert result.frame.loc[0, "safra"] == expected
    assert result.frame.loc[0, "safra_inicio"] == ""
    assert result.frame.loc[0, "safra_fim"] == label


@pytest.mark.parametrize(
    "start,end",
    [
        ("", "2027"),
        ("2026", ""),
        ("2026", "2028"),
        ("2026", "PERENE"),
        ("", "unknown"),
        ("２０２６", "２０２７"),
    ],
)
def test_invalid_season_is_not_perennial(start, end):
    with pytest.raises(ParseError, match="SafraIni/Fin"):
        parser.parse_tabua_risco(zarc_csv([{"SafraIni": start, "SafraFin": end}]))


def test_mismatched_resource_is_rejected_before_filter():
    with pytest.raises(ParseError, match="difere do recurso"):
        parser.parse_tabua_risco_bundle(
            zarc_csv(), query=query.build_query(uf="SP"), expected_safra="2025/2026"
        )


def test_duplicate_rows_preserved_with_origin_positions():
    frame = parser.parse_tabua_risco(zarc_csv([{}, {}]))
    assert frame.registro_origem.tolist() == [1, 2]
    assert (
        frame.drop(columns="registro_origem")
        .iloc[0]
        .equals(frame.drop(columns="registro_origem").iloc[1])
    )


def test_origin_position_counts_blank_record_before_filter():
    content = zarc_csv([{"UF": "SP"}, {}]).splitlines(keepends=True)
    result = parser.parse_tabua_risco_bundle(
        content[0] + content[1] + b"\n" + content[2], query=query.build_query(uf="MT")
    )
    assert result.frame.registro_origem.tolist() == [3]
    assert result.details["source_rows"] == 3
    assert result.details["validated_rows"] == 2
    assert result.details["blank_physical_lines"] == 1


def test_multiline_field_preserves_record_position():
    frame = parser.parse_tabua_risco(zarc_csv([{"Portaria": "primeira\nsegunda"}, {}]))
    assert frame.portaria.tolist()[0] == "primeira\nsegunda"
    assert frame.registro_origem.tolist() == [1, 2]


@pytest.mark.parametrize("municipio", ["orr", "5107925", 5107925])
def test_municipality_selection_name_or_exact_code(municipio):
    result = parser.parse_tabua_risco_bundle(
        zarc_csv(), query=query.build_query(municipio=municipio)
    )
    assert len(result.frame) == 1


def test_municipality_selection_is_literal():
    result = parser.parse_tabua_risco_bundle(
        zarc_csv([{"municipio": "Anápolis (GO)"}]), query=query.build_query(municipio="(GO)")
    )
    assert len(result.frame) == 1


def test_cultura_catalogo_ausente_erro_apos_validar_populacao():
    with pytest.raises(InvalidParameterError, match="não encontrada"):
        parser.parse_tabua_risco_bundle(zarc_csv(), query=query.build_query(cultura="sisal"))


def test_cultura_catalogo_ausente_nao_esconde_populacao_invalida():
    with pytest.raises(ParseError, match="dec36"):
        parser.parse_tabua_risco_bundle(
            zarc_csv([{"dec36": "bad"}]), query=query.build_query(cultura="sisal")
        )


def test_fingerprint_ignores_values_and_detects_order():
    first = parser.parse_tabua_risco_bundle(zarc_csv()).details["layout_fingerprint"]["sha256"]
    other = parser.parse_tabua_risco_bundle(zarc_csv([{"dec1": "50"}])).details[
        "layout_fingerprint"
    ]["sha256"]
    reordered = parser.parse_tabua_risco_bundle(
        zarc_csv(headers=list(reversed(constants.ZARC_CSV_COLUMNS)))
    ).details["layout_fingerprint"]["sha256"]
    assert first == other
    assert first != reordered


def test_windows_1252_and_utf8_same_values():
    content = zarc_csv([{"Nome_cultura": "Feijão", "municipio": "Brasília"}])
    legacy = content.decode("utf-8-sig").encode("windows-1252")
    pd.testing.assert_frame_equal(
        parser.parse_tabua_risco(content), parser.parse_tabua_risco(legacy)
    )


def test_all_null_risk_polars_remains_nullable_int64():
    pl = pytest.importorskip("polars")
    frame = parser.parse_tabua_risco(zarc_csv([{"dec1": ""}]))
    converted = pl.from_pandas(frame)
    assert converted.schema["dec1"] == pl.Int64
    assert converted["dec1"].null_count() == 1

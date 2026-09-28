from __future__ import annotations

import csv
import io
import re
from datetime import date
from pathlib import Path

import pandas as pd
import pydantic
import pytest

from agrobr import constants
from agrobr.exceptions import ParseError
from agrobr.rnc import models, parser
from tests.helpers import collect_failures, levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data/rnc/selecao_20260907"


def csv_rows(family="registradas"):
    return list(csv.reader(io.StringIO((GOLDEN / f"{family}.csv").read_text(encoding="utf-8-sig"))))


def encode(rows):
    output = io.StringIO(newline="")
    csv.writer(output).writerows(rows)
    return output.getvalue().encode()


def record(family="registradas"):
    rows = csv_rows(family)
    rename = models.REGISTRADAS_RENAME if family == "registradas" else models.PROTEGIDAS_RENAME
    result = {rename[key]: value for key, value in zip(rows[0], rows[1], strict=True)}
    if family == "protegidas":
        result["termino_protecao_texto"] = result["termino_protecao"]
    return result


@pytest.mark.parametrize(
    "value",
    [
        "31/02/2024",
        "2024-01-01",
        "00/00/0000",
        "abc",
        "01/01/1000",
        "21/09/1677",
        "12/04/2262",
        "4/9/2026",
    ],
)
def test_invalid_nonempty_date_never_becomes_nat(value):
    rows = csv_rows()
    rows[1][rows[0].index("DATA DO REGISTRO")] = value
    with levanta_exatamente(ParseError, match=re.escape("campos inválidos ['data_registro']")):
        parser.parse_registradas_bundle(encode(rows))


@pytest.mark.parametrize("value", [date(2024, 2, 29), 20240229])
def test_external_model_rejects_nontext_date(value):
    raw = record()
    raw["data_registro"] = value
    with levanta_exatamente(pydantic.ValidationError, match="data publicada deve ser texto"):
        models.RncRegistrada.model_validate(raw)


def test_external_model_rejects_unknown_field():
    raw = {**record(), "campo_novo": "valor"}
    with levanta_exatamente(pydantic.ValidationError, match="campo_novo"):
        models.RncRegistrada.model_validate(raw)


def test_blank_required_text_is_rejected_by_field():
    required = {
        "registradas": ["nome_comum", "nome_cientifico", "grupo", "situacao", "nr_registro"],
        "protegidas": [
            "cultivar",
            "nome_cientifico",
            "nome_comum",
            "nr_processo",
            "situacao",
            "nr_certificado",
            "titular",
            "representante_legal",
        ],
    }
    with collect_failures() as check:
        for family, fields in required.items():
            rename = (
                models.REGISTRADAS_RENAME if family == "registradas" else models.PROTEGIDAS_RENAME
            )
            source = {value: key for key, value in rename.items()}
            parse = (
                parser.parse_registradas_bundle
                if family == "registradas"
                else parser.parse_protegidas_bundle
            )
            for field in fields:
                with check((family, field)):
                    rows = csv_rows(family)[:2]
                    rows[1][rows[0].index(source[field])] = "  "
                    with levanta_exatamente(
                        ParseError, match=re.escape(f"campos inválidos ['{field}']")
                    ):
                        parse(encode(rows))


@pytest.mark.parametrize("value", [True, 1, 1.0, None, b"123"])
def test_external_model_rejects_nontext_identifier(value):
    raw = record()
    raw["nr_registro"] = value
    with pytest.raises(pydantic.ValidationError):
        models.RncRegistrada.model_validate(raw)


@pytest.mark.parametrize("value", ["", constants.SNPC_CONDITIONAL_END, "29/02/2024"])
def test_termination_text_preserves_date_condition_and_absence(value):
    rows = csv_rows("protegidas")[:2]
    rows[1][7] = value
    result = parser.parse_protegidas_bundle(encode(rows))
    assert result.frame.iloc[0]["termino_protecao_texto"] == value
    assert pd.isna(result.frame.iloc[0]["termino_protecao"]) == (
        value in {"", constants.SNPC_CONDITIONAL_END}
    )
    assert result.details["termination_counts"]["blank"] == int(value == "")
    assert result.details["termination_counts"]["conditional"] == int(
        value == constants.SNPC_CONDITIONAL_END
    )


def test_conditional_end_is_not_accepted_as_registration_date():
    raw = record()
    raw["data_registro"] = constants.SNPC_CONDITIONAL_END
    with pytest.raises(pydantic.ValidationError):
        models.RncRegistrada.model_validate(raw)


def test_conflicting_termination_text_and_scalar_are_rejected():
    raw = record("protegidas")
    raw["termino_protecao"] = "01/01/2024"
    raw["termino_protecao_texto"] = constants.SNPC_CONDITIONAL_END
    with pytest.raises(pydantic.ValidationError, match="incompatível"):
        models.SnpcProtegida.model_validate(raw)


def test_missing_optional_legacy_column_is_now_layout_error():
    rows = csv_rows()
    index = rows[0].index("MANTENEDOR (REQUERENTE) (NOME)")
    for row in rows:
        row.pop(index)
    with pytest.raises(ParseError, match="ausentes"):
        parser.parse_registradas_bundle(encode(rows))


@pytest.mark.parametrize(
    ("mutation", "reason"),
    [
        ("duplicate_header", "colunas duplicadas"),
        ("short_row", "largura CSV incompatível"),
        ("long_row", "largura CSV incompatível"),
        ("all_empty_row", "campos inválidos"),
        ("duplicate_extra_column", "colunas duplicadas"),
        ("unknown_column", re.escape("colunas não reconhecidas ['CAMPO NOVO']")),
    ],
)
def test_structural_errors_are_explicit(mutation, reason):
    rows = csv_rows()[:2]
    if mutation == "duplicate_header":
        rows[0][1] = rows[0][0]
    elif mutation == "short_row":
        rows[1].pop()
    elif mutation == "long_row":
        rows[1].append("extra")
    elif mutation == "duplicate_extra_column":
        rows[0].append(rows[0][0])
        rows[1].append("Outra cultivar")
    elif mutation == "unknown_column":
        rows[0].append("CAMPO NOVO")
        rows[1].append("valor")
    else:
        rows[1] = [""] * len(rows[0])
    with levanta_exatamente(ParseError, match=reason):
        parser.parse_registradas_bundle(encode(rows))


@pytest.mark.parametrize("encoding", ["utf-8", "utf-8-sig", "windows-1252"])
def test_encoding_is_detected_and_published(encoding):
    text = io.StringIO(newline="")
    csv.writer(text).writerows(csv_rows())
    result = parser.parse_registradas_bundle(text.getvalue().encode(encoding))
    expected = parser.parse_registradas_bundle((GOLDEN / "registradas.csv").read_bytes())
    pd.testing.assert_frame_equal(result.frame, expected.frame)
    assert result.details["encoding"] == encoding


def test_blank_physical_line_is_counted_and_skipped():
    rows = csv_rows()[:3]
    result = parser.parse_registradas_bundle(encode(rows[:2]) + b"\r\n" + encode(rows[2:]))
    identity = rows[0].index("Nº REGISTRO")
    assert result.frame["nr_registro"].tolist() == [rows[1][identity], rows[2][identity]]
    assert result.details["blank_physical_rows"] == 1


@pytest.mark.parametrize(
    "family,identity", [("registradas", "nr_registro"), ("protegidas", "nr_processo")]
)
def test_duplicate_primary_identity_fails_without_dedup(family, identity):
    rows = csv_rows(family)[:2]
    rows.append(rows[1].copy())
    parse = (
        parser.parse_registradas_bundle
        if family == "registradas"
        else parser.parse_protegidas_bundle
    )
    with pytest.raises(ParseError, match=identity + " duplicado"):
        parse(encode(rows))


def test_fingerprint_tracks_layout_not_observations():
    rows = csv_rows()[:2]
    original = parser.parse_registradas_bundle(encode(rows))
    rows[1][0] = "Outro nome"
    modified = parser.parse_registradas_bundle(encode(rows))
    assert original.details["layout_fingerprint"] == modified.details["layout_fingerprint"]
    reordered = parser.parse_registradas_bundle(encode([list(reversed(row)) for row in rows]))
    assert reordered.details["layout_fingerprint"] != original.details["layout_fingerprint"]
    pd.testing.assert_frame_equal(reordered.frame, modified.frame)


def test_all_empty_dates_have_stable_datetime_ns_dtype():
    rows = csv_rows()[:2]
    rows[1][7] = rows[1][8] = ""
    result = parser.parse_registradas_bundle(encode(rows))
    for column in models.DATE_COLS_REG:
        assert str(result.frame[column].dtype) == "datetime64[ns]"
        assert result.frame[column].isna().all()
        assert result.details["date_statistics"][column]["empty_count"] == 1


@pytest.mark.parametrize(
    ("raw", "reason"),
    [
        (b'"unterminated', "inválido ou encoding incompatível"),
        (b"<html>failure</html>", "colunas ausentes"),
        (b"", "registradas vazio"),
        (b"   ", "registradas vazio"),
        (b"\xef\xbb\xbf", "registradas vazio"),
        (encode(csv_rows()[:1]), "sem registros"),
        (b"\xef\xbb\xbf" + encode(csv_rows()[:2]) + b"\xff", "encoding incompatível"),
    ],
)
def test_invalid_document_is_parse_error(raw, reason):
    with levanta_exatamente(ParseError, match=reason):
        parser.parse_registradas_bundle(raw)


@pytest.mark.parametrize(
    "text,total",
    [
        ("Sua pesquisa retornou 38335 registros", 38335),
        ("Sua pesquisa retornou <b>38.335</b>&nbsp;registros", 38335),
        ("Sua pesquisa retornou 0 registros", 0),
        ("Pesquisa sem mensagem", None),
        ("Sua pesquisa\n    retornou  38.335\tregistros.", 38335),
    ],
)
def test_public_search_total(text, total):
    assert parser.parse_reported_total(f"<p>{text}</p>".encode()) == total


@pytest.mark.parametrize(
    ("text", "reason"),
    [
        ("Sua pesquisa retornou 1.2 registros", "número ou estrutura inválidos"),
        ("Sua pesquisa retornou -2 registros", "número ou estrutura inválidos"),
        ("Sua pesquisa retornou muitos registros", "número ou estrutura inválidos"),
        (
            "Sua pesquisa retornou 1 registros. Sua pesquisa retornou 2 registros",
            "Mensagens conflitantes",
        ),
    ],
)
def test_malformed_or_conflicting_total_is_error(text, reason):
    with levanta_exatamente(ParseError, match=reason):
        parser.parse_reported_total(f"<p>{text}</p>".encode())


def test_repeated_identical_total_is_not_conflict():
    raw = b"<p>Sua pesquisa retornou 10 registros</p><p>Sua pesquisa retornou 10 registros</p>"
    assert parser.parse_reported_total(raw) == 10


def test_oversized_numeric_search_total_is_parse_error():
    raw = ("Sua pesquisa retornou " + "9" * 5000 + " registros").encode()
    with levanta_exatamente(ParseError, match="incompatível com a contagem") as raised:
        parser.parse_reported_total(raw)
    assert isinstance(raised.value.__cause__, ValueError)

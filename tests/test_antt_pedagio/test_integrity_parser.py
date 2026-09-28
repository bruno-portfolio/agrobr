from __future__ import annotations

import csv
import hashlib
import io
import itertools
import json
import re
import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import pydantic
import pytest

from agrobr.alt.antt_pedagio import _traffic, models, parser
from agrobr.exceptions import (
    InvalidParameterError,
    ParseError,
    ResourceLimitError,
)

HEADER = "concessionaria;mes_ano;sentido;praca;tipo_cobranca;categoria_eixo;tipo_de_veiculo;volume_total\n"
OFFICIAL = Path(__file__).resolve().parents[1] / "golden_data/antt_pedagio/integridade20260908"


def content(*rows: str) -> io.BytesIO:
    return io.BytesIO((HEADER + "\n".join(rows) + ("\n" if rows else "")).encode())


def test_daily_block_published_twice_keeps_one_copy():
    rows = ("X;01/02/2026;C;P;Manual;3;Comercial;1,00", "X;02/02/2026;C;P;Manual;3;Comercial;2,00")
    result = parser.parse_trafego_file(content(*rows, *rows), ano=2026, frequencia="diaria")
    assert result.frame["volume"].tolist() == [1, 2]
    assert result.diagnostics["duplicated_blocks"] == [
        {"concessionaria": "X", "mes": "2026-02", "rows": 4, "first_line": 2, "last_line": 5}
    ]


@pytest.mark.parametrize(
    "value",
    [
        "",
        " ",
        "abc",
        "NULL",
        "NaN",
        "Infinity",
        "9223372036854775808",
        "1e3",
        "1.234,00",
    ],
)
def test_invalid_volume_fails_before_filter(value):
    with pytest.raises(ParseError, match="Registro 2.*volume"):
        parser.parse_trafego_file(
            content(
                "X;01/2026;C;P;Manual;3;Comercial;1", f"Y;01/2026;C;P;Manual;3;Comercial;{value}"
            ),
            ano=2026,
            frequencia="mensal",
            keep=lambda row: row.concessionaria == "X",
        )


def test_aggregate_overflow_fails():
    with pytest.raises(Exception) as caught:
        parser.parse_trafego_file(
            content(
                "X;01/2026;C;P;Manual;3;Comercial;9223372036854775807",
                "X;01/2026;C;P;Manual;3;Comercial;1",
            ),
            ano=2026,
            frequencia="mensal",
        )
    assert caught.type is ParseError, caught.value
    assert "agregado fora de Int64" in str(caught.value)


@pytest.mark.parametrize(
    "value,frequency",
    [
        ("99/01/2026", "diaria"),
        ("29/02/2026", "diaria"),
        ("01/2026", "diaria"),
        ("01/2025", "mensal"),
        ("01/2026 ", "mensal"),
    ],
)
def test_invalid_civil_reference_fails(value, frequency):
    with pytest.raises(ParseError, match="data"):
        parser.parse_trafego_file(
            content(f"X;{value};C;P;Manual;3;Comercial;1"), ano=2026, frequencia=frequency
        )


def test_registro_recusa_data_que_nao_e_texto():
    with pytest.raises(pydantic.ValidationError, match="data deve ser texto civil"):
        models.TrafegoRecord.model_validate(
            {
                "source_record": 1,
                "frequencia": "diaria",
                "data": datetime(2026, 1, 1).date(),
                "concessionaria": "X",
                "praca": "P",
                "categoria_eixo": "3",
                "tipo_veiculo": "Comercial",
                "tipo_cobranca": "C",
                "volume": "1",
            }
        )


@pytest.mark.parametrize(
    "header",
    [
        HEADER.replace("volume_total", "volume_total;quantidade"),
        HEADER.replace("volume_total", "volume_total;volume_total"),
        HEADER.replace("volume_total", "extra"),
        HEADER.replace("volume_total", "volume_total;extra"),
        HEADER.replace("praca;", ""),
    ],
)
def test_bad_header_fails_even_without_data(header):
    with pytest.raises(ParseError):
        parser.parse_trafego_file(io.BytesIO(header.encode()), ano=2026, frequencia="mensal")


@pytest.mark.parametrize(
    "row,message",
    [
        ("X;01/2026;C;P;Manual;3;Comercial", "largura 7, esperada 8"),
        ("X;01/2026;C;P;Manual;3;Comercial;1;extra", "largura 9, esperada 8"),
        ('X;01/2026;C;"P;Manual;3;Comercial;1', "CSV inválido"),
        ('X;01/2026;C;"P"x;Manual;3;Comercial;1', "CSV inválido"),
        ("X;01/2026; ;P;Manual;3;Comercial;1", "sentido"),
        ("X;01/2026;C;P;Manual;0 eixos;Comercial;1", "eixos"),
    ],
)
def test_malformed_record_is_not_skipped(row, message):
    try:
        parser.parse_trafego_file(
            content(row), ano=2026, frequencia="mensal", keep=lambda _row: False
        )
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ParseError) and re.search(message, str(caught)), caught


def test_keep_must_return_bool():
    with pytest.raises(TypeError, match="keep deve retornar bool"):
        parser.parse_trafego_file(
            content("X;01/2026;C;P;Manual;3;Comercial;1"),
            ano=2026,
            frequencia="mensal",
            keep=lambda _row: 1,
        )


def test_embedded_header_is_rejected():
    with pytest.raises(ParseError, match="cabeçalho repetido"):
        parser.parse_trafego_file(content(HEADER.rstrip()), ano=2026, frequencia="mensal")


def outcome(raw: bytes, **options):
    try:
        return parser.parse_trafego_file(io.BytesIO(raw), ano=2026, frequencia="mensal", **options)
    except ParseError as exc:
        return exc


def test_bom_multiline_and_blank_records():
    raw = "\ufeff" + HEADER + '\nX;01/2026;C;"P\ntexto";Manual;3;Comercial;1\n'
    result = outcome(raw.encode())
    assert not isinstance(result, ParseError), result
    assert result.frame["praca"].tolist() == ["P\ntexto"]
    assert result.diagnostics["blank_records"] == 1
    assert result.diagnostics["csv_records_read"] == 2
    assert result.diagnostics["physical_lines"] == 4


def test_repeated_literal_text_is_shared_without_changing_output_dtype():
    plaza = "P " + "á" * 4000
    try:
        result = parser.parse_trafego_file(
            content(*(f"X;01/2026;C;{plaza};C{i};3;Comercial;1" for i in range(500))),
            ano=2026,
            frequencia="mensal",
            max_memory_bytes=1024**2,
        )
    except ResourceLimitError as exc:
        result = exc
    assert not isinstance(result, ResourceLimitError), result
    values = result.frame["praca"].array
    assert len(result.frame) == 500
    assert all(value == plaza and value is values[0] for value in values)
    assert isinstance(result.frame["praca"].dtype, pd.StringDtype)
    assert result.frame["praca"].dtype.storage == "python"
    assert result.frame["tipo_cobranca"].tolist() == [f"C{i}" for i in range(500)]
    assert result.diagnostics["source_volume_sum"] == 500
    assert result.diagnostics["selected_volume_sum"] == 500
    assert result.diagnostics["retained_bytes_estimate"] <= 1024**2


def test_group_resize_reserves_old_and_new_tables_before_insertion():
    probe = {}
    boundary = None
    for index in range(10000):
        old_bytes = sys.getsizeof(probe)
        probe[index] = index
        if old_bytes >= 32768 and sys.getsizeof(probe) > old_bytes:
            boundary = index
            break
    assert boundary is not None
    state = _traffic.TrafficScan(10000, 16 * 1024**2)
    combinations = itertools.product(range(1, 21), range(16), range(8))
    for index, (day, plaza, billing) in enumerate(combinations):
        record = models.TrafegoRecord.model_validate(
            {
                "source_record": index + 1,
                "frequencia": "diaria",
                "data": f"{day:02d}/01/2026",
                "concessionaria": "X",
                "praca": f"P{plaza}",
                "categoria_eixo": "3",
                "tipo_veiculo": "Comercial",
                "tipo_cobranca": f"C{billing}",
                "volume": "1",
            }
        )
        if index == boundary:
            break
        state.append(record)
    assert len(state.groups) == boundary
    previous_size = sys.getsizeof(state.groups)
    state.max_memory_bytes = state.retained_bytes() + previous_size * 2 - 1
    with pytest.raises(ResourceLimitError, match="memória"):
        state.append(record)
    assert len(state.groups) == boundary
    assert sys.getsizeof(state.groups) == previous_size
    state.max_memory_bytes = 16 * 1024**2
    state.append(record)
    assert len(state.groups) == boundary + 1
    assert sys.getsizeof(state.groups) > previous_size


@pytest.mark.parametrize(
    "option,value",
    [
        ("max_rows", True),
        ("max_rows", -1),
        ("max_memory_bytes", 0),
        ("max_memory_bytes", True),
        ("ano", 1677),
        ("ano", 2262),
        ("ano", True),
        ("frequencia", "anual"),
        ("keep", 1),
    ],
)
def test_invalid_internal_budget_before_read(option, value):
    arguments = {"ano": 2026, "frequencia": "mensal", option: value}
    with pytest.raises(Exception) as caught:
        parser.parse_trafego_file(io.BytesIO(), **arguments)
    assert caught.type is InvalidParameterError, caught.value


@pytest.mark.parametrize(
    "digits,expected",
    [
        ("0", None),
        ("1", None),
        ("2", True),
        ("000", None),
        ("0001", None),
        ("0002", True),
        ("9" * 300, True),
        ("9" * 5000, True),
        ("0" * 5000 + "1", None),
        ("0" * 5000 + "2", True),
    ],
    ids=[
        "zero",
        "one",
        "two",
        "zero_padded",
        "one_padded",
        "two_padded",
        "300_digits",
        "5000_digits",
        "5000_zeros_one",
        "5000_zeros_two",
    ],
)
def test_heavy_bounds_exact_without_integer_conversion(digits, expected):
    category = f"Veículo comercial acima de {digits} eixos"
    observed = []

    def keep(record):
        state = parser.heavy_vehicle_status(record)
        observed.append(state)
        assert record.categoria_eixo == category
        assert record.n_eixos is None
        return state is True

    raw = f"X;01/2026;C;P;Manual;{category};Comercial;1"
    result = parser.parse_trafego_file(content(raw), ano=2026, frequencia="mensal", keep=keep)
    assert observed == [expected]
    assert len(result.frame) == int(expected is True)
    assert result.diagnostics["validated_rows"] == 1
    assert result.diagnostics["eof_reached"]
    complete = parser.parse_trafego_file(content(raw), ano=2026, frequencia="mensal").frame
    assert complete["categoria_eixo"].tolist() == [category]
    assert complete["n_eixos"].isna().all()


def test_missing_category_is_structural_not_zero():
    stream = io.BytesIO(b"concessionaria;praca;mes_ano;volume_total\nX;P;01/2026;1\n")
    result = parser.parse_trafego_file(stream, ano=2026, frequencia="mensal")
    assert result.frame["n_eixos"].isna().all()
    assert result.frame["tipo_veiculo"].isna().all()
    assert "categoria_eixo" in result.diagnostics["missing_columns"]


@pytest.mark.parametrize(
    "header,row",
    [
        ("concessionaria;praca_de_pedagio;lat", "X;P;inf"),
        ("concessionaria;praca_de_pedagio;uf", "X;P;XX"),
        ("concessionaria;praca_de_pedagio;lat", "X;P;-90"),
        ("concessionaria;praca_de_pedagio;lat", "X;P;5 "),
        ("concessionaria;praca_de_pedagio", "X;P;extra"),
        ("concessionaria;praca_de_pedagio", "X"),
        ("concessionaria;praca_de_pedagio", "concessionaria;praca_de_pedagio"),
    ],
)
def test_pracas_invalid_occurrence_is_not_skipped(header, row):
    try:
        parser.parse_pracas((header + "\n" + row + "\n").encode())
    except Exception as exc:
        caught = exc
    else:
        caught = None
    assert isinstance(caught, ParseError), caught


@pytest.mark.parametrize(
    "header",
    [
        "concessionaria;praca_de_pedagio;lat;latitude",
        "concessionaria;praca_de_pedagio;unknown",
        "concessionaria;concessionaria;praca_de_pedagio",
        "concessionaria;rodovia",
    ],
)
def test_pracas_ambiguous_or_unknown_header_fails(header):
    with pytest.raises(ParseError):
        parser.parse_pracas((header + "\n").encode())


def test_pracas_comma_separated_registry():
    try:
        frame = parser.parse_pracas(b"concessionaria,praca_de_pedagio,uf\nX,P,SP\n")
    except ParseError as exc:
        frame = exc
    assert not isinstance(frame, ParseError), frame
    assert frame[["concessionaria", "praca_de_pedagio", "uf"]].values.tolist() == [["X", "P", "SP"]]


def test_enrichment_null_versus_blank_is_conflict():
    frame = pd.DataFrame(
        {"concessionaria": ["X", "X"], "praca_de_pedagio": ["P", "P"], "municipio": [None, ""]}
    )
    mapping, diagnostics = parser.build_pracas_enrichment(frame)
    assert not mapping and diagnostics["conflicting_keys"] == 1


def test_enrichment_missing_keys_do_not_match():
    frame = pd.DataFrame(
        {"concessionaria": [None, " "], "praca_de_pedagio": ["P", "P"], "uf": ["SP", "PR"]}
    )
    mapping, diagnostics = parser.build_pracas_enrichment(frame)
    assert not mapping and diagnostics["missing_key_rows"] == 2


@pytest.mark.parametrize("name", ["mensal_2026", "diario_2026", "legado_2010"])
def test_official_integrity_sample_all_cells(name):
    metadata = json.loads((OFFICIAL / "metadata.json").read_bytes())
    sample = metadata["samples"][name]
    body = (OFFICIAL / sample["file"]).read_bytes()
    assert len(body) == sample["size_bytes"]
    assert hashlib.sha256(body).hexdigest() == sample["sha256"]
    rows = list(csv.DictReader(io.StringIO(body.decode("cp1252")), delimiter=";"))
    assert rows == [record["cells"] for record in sample["records"]]
    result = parser.parse_trafego_file(
        io.BytesIO(body), ano=sample["year"], frequencia=sample["frequency"]
    )
    frame = result.frame
    assert frame.columns.tolist() == [
        "data",
        "concessionaria",
        "praca",
        "sentido",
        "n_eixos",
        "tipo_veiculo",
        "volume",
        "rodovia",
        "uf",
        "municipio",
        "categoria_eixo",
        "tipo_cobranca",
        "frequencia",
    ]
    assert len(frame) == len(rows)
    for index, source in enumerate(rows):
        category = source.get("categoria_eixo", source.get("categoria"))
        axle = re.fullmatch(
            r"(?:Ve[íi]culo (?:Comercial|Passeio) )?([0-9]+) [Ee]ixos?", category, re.IGNORECASE
        )
        expected_date = datetime.strptime(
            source["mes_ano"], "%d/%m/%Y" if source["mes_ano"].count("/") == 2 else "%m/%Y"
        )
        expected = {
            "data": pd.Timestamp(expected_date),
            "concessionaria": source["concessionaria"],
            "praca": source["praca"],
            "sentido": source["sentido"],
            "n_eixos": int(axle[1]) if axle else None,
            "tipo_veiculo": source["tipo_de_veiculo"],
            "volume": int(Decimal(source["volume_total"].replace(",", "."))),
            "rodovia": None,
            "uf": None,
            "municipio": None,
            "categoria_eixo": category,
            "tipo_cobranca": source["tipo_cobranca"],
            "frequencia": sample["frequency"],
        }
        for column, value in expected.items():
            actual = frame.iloc[index][column]
            assert (None if pd.isna(actual) else actual) == value, column
    assert str(frame["data"].dtype) == "datetime64[ns]"
    for column in ("volume", "n_eixos"):
        assert str(frame[column].dtype) == "Int64"
    for column in set(frame) - {"data", "volume", "n_eixos"}:
        assert isinstance(frame[column].dtype, pd.StringDtype)
        assert frame[column].dtype.storage == "python"
    assert result.diagnostics["validated_rows"] == len(rows)
    assert result.diagnostics["selected_rows"] == len(rows)
    assert result.diagnostics["source_volume_sum"] == sum(
        int(Decimal(row["volume_total"].replace(",", "."))) for row in rows
    )
    assert result.diagnostics["eof_reached"]


def test_truncated_utf8_at_eof_falls_back_to_windows_1252():
    raw = "concessionaria;mes_ano;volume_total;praca\nX;01/2026;1;Conceição".encode() + b"\xc3"
    result = outcome(raw)
    assert not isinstance(result, ParseError), result
    assert result.frame["praca"].tolist() == ["ConceiÃ§Ã£oÃ"]
    assert result.diagnostics["encoding"] == "windows-1252"

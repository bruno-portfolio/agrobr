from __future__ import annotations

import csv
import io
from dataclasses import replace
from decimal import Decimal
from pathlib import Path

import pytest

from agrobr.comexstat import models, parser, query
from agrobr.exceptions import ParseError, ResourceLimitError


def test_dictionary_bad_late_row_not_ignored():
    with pytest.raises(ParseError, match="Registro 2"):
        parser.parse_dictionary(io.BytesIO(b"CO_VIA;NO_VIA\n00;ok\n001;bad\n"), tabela="vias")


def test_dictionary_limits_preserve_all_or_fail():
    body = b"CO_VIA;NO_VIA\n00;zero\n00;duplicate\n"
    with pytest.raises(ResourceLimitError, match="max_linhas"):
        parser.parse_dictionary(io.BytesIO(body), tabela="vias", max_linhas=1)
    assert len(parser.parse_dictionary(io.BytesIO(body), tabela="vias", max_linhas=None).frame) == 2


def test_memory_boundary_matches_reported_peak_with_unicode_repetition():
    body = ("CO_VIA;NO_VIA\n" + "00;東京 á🚜\n" * 20).encode()
    initial = parser.parse_dictionary(io.BytesIO(body), tabela="vias")
    peak = initial.details["retained_bytes_estimate"]
    assert parser.parse_dictionary(
        io.BytesIO(body), tabela="vias", max_memoria_bytes=peak
    ).frame.equals(initial.frame)
    with pytest.raises(ResourceLimitError):
        parser.parse_dictionary(io.BytesIO(body), tabela="vias", max_memoria_bytes=peak - 1)


def test_filters_preserve_literal_zero_codes_and_validate_rejected_rows():
    body = b"CO_ANO;CO_MES;CO_NCM;CO_UNID;CO_PAIS;SG_UF_NCM;CO_VIA;CO_URF;QT_ESTAT;KG_LIQUIDO;VL_FOB\n2026;01;12019000;10;000;EX;00;0000000;1;2;3\n2026;01;12019000;10;105;SP;01;0817800;4;5;6\n"
    selected = query.build_query(
        fluxo="exportacao", produto="soja", ano=2026, pais=0, via=0, urf=0, agregacao="detalhado"
    )
    result = parser.parse_resource(io.BytesIO(body), selected)
    assert len(result.frame) == 1
    assert result.details["validated_rows"] == 2
    assert result.frame.iloc[0]["uf"] == "EX"
    zero = parser.parse_resource(io.BytesIO(body), replace(selected, uf="ND"))
    assert len(zero.frame) == 0


def test_empty_bytes_rejected_distinct_from_header_only():
    with pytest.raises(ParseError, match="cabeçalho"):
        parser.parse_dictionary(io.BytesIO(), tabela="vias")


@pytest.mark.parametrize(
    "filename,flow,year",
    [
        ("EXP_2025.csv", "exportacao", 2025),
        ("EXP_2026.csv", "exportacao", 2026),
        ("IMP_2025.csv", "importacao", 2025),
        ("IMP_2026.csv", "importacao", 2026),
    ],
)
def test_official_records_every_cell_independent_of_models(filename, flow, year):
    path = (
        Path(__file__).parents[1] / "golden_data" / "comexstat" / "integridade20260908" / filename
    )
    with path.open(encoding="utf-8-sig", newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))[1:]
    selected = replace(
        query.build_query(fluxo=flow, produto="soja", ano=year, agregacao="detalhado"),
        ncm=models.SelecaoNcm(("",)),
    )
    result = parser.parse_resource(io.BytesIO(path.read_bytes()), selected)
    assert len(result.frame) == len(rows)
    expected_columns = [
        "ano",
        "mes",
        "ncm",
        "cod_unidade",
        "cod_pais",
        "uf",
        "cod_via",
        "cod_urf",
        "qtd_estatistica",
        "kg_liquido",
        "valor_fob_usd",
    ]
    if flow == "importacao":
        expected_columns += ["valor_frete_usd", "valor_seguro_usd"]
    assert list(result.frame.columns) == expected_columns
    for raw, observed in zip(rows, result.frame.itertuples(index=False, name=None), strict=True):
        for position, (token, value) in enumerate(zip(raw, observed, strict=True)):
            if position in (0, 1, 8, 9):
                assert value == int(token)
            elif position >= 10:
                assert value.hex() == float(Decimal(token)).hex()
            else:
                assert value == token


@pytest.mark.parametrize(
    "filename,table,columns",
    [
        ("NCM_UNIDADE.csv", "unidades", ["cod_unidade", "unidade", "sigla_unidade"]),
        (
            "PAIS.csv",
            "paises",
            [
                "cod_pais",
                "cod_pais_iso_numerico",
                "cod_pais_iso_alfa3",
                "pais",
                "pais_ingles",
                "pais_espanhol",
            ],
        ),
        ("VIA.csv", "vias", ["cod_via", "via"]),
        ("URF.csv", "urfs", ["cod_urf", "urf"]),
    ],
)
def test_official_dictionary_every_literal_cell(filename, table, columns):
    path = (
        Path(__file__).parents[1] / "golden_data" / "comexstat" / "integridade20260908" / filename
    )
    encoding = "cp1252" if table in ("paises", "urfs") else "utf-8-sig"
    with path.open(encoding=encoding, newline="") as stream:
        rows = list(csv.reader(stream, delimiter=";"))[1:]
    result = parser.parse_dictionary(io.BytesIO(path.read_bytes()), tabela=table)
    assert list(result.frame.columns) == columns
    assert list(result.frame.itertuples(index=False, name=None)) == [tuple(row) for row in rows]
    assert result.details["eof_reached"] is True

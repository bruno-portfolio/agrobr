from __future__ import annotations

import io
import json
from datetime import datetime
from pathlib import Path

import openpyxl
import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.mapbiomas import models, municipal_parser

GOLDEN = Path(__file__).parents[1] / "golden_data/mapbiomas/municipal11_official"
GOLDEN_10 = Path(__file__).parents[1] / "golden_data/mapbiomas/municipal10_official"
HEADERS = [
    "ID",
    "country",
    "biome",
    "region",
    "state",
    "geocode",
    "municipality",
    "municipality-state",
    "class",
    "class_level_0",
    "class_level_1",
    "class_level_2",
    "class_level_3",
    "class_level_4",
    *[f"y{year}" for year in range(1985, 2026)],
]


def _row(**changes: object) -> list[object]:
    values: dict[str, object] = {
        "ID": 1,
        "country": "Brasil",
        "biome": "Cerrado",
        "region": "Centro-oeste",
        "state": "Distrito Federal",
        "geocode": "5300108",
        "municipality": "Brasília",
        "municipality-state": "Brasília - Distrito Federal",
        "class": 3,
        "class_level_0": "Natural",
        "class_level_1": "1. Forest",
        "class_level_2": "1.1. Forest Formation",
        "class_level_3": "1.1. Forest Formation",
        "class_level_4": "1.1. Forest Formation",
        **{f"y{year}": float(year - 1984) for year in range(1985, 2026)},
    }
    values.update(changes)
    return [values[name] for name in HEADERS]


def _xlsx(
    rows: list[list[object]], headers: list[object] | None = None, collection: int = 11
) -> bytes:
    workbook = openpyxl.Workbook()
    sheet = workbook.active
    sheet.title = f"COVERAGE_{collection}"
    sheet.append(HEADERS if headers is None else headers)
    for row in rows:
        sheet.append(row)
    content = io.BytesIO()
    workbook.save(content)
    workbook.close()
    return content.getvalue()


def test_official_subset_all_areas_match_independent_xml_oracles():
    oracle = json.loads((GOLDEN / "raw_oracle.json").read_text(encoding="utf-8"))
    frame, details = municipal_parser.parse_cobertura_municipal(
        (GOLDEN / "municipal11_selected.xlsx").read_bytes(),
        colecao=11,
    )
    states = {
        "Distrito Federal": "DF",
        "Mato Grosso": "MT",
        "Pará": "PA",
        "Alagoas": "AL",
        "Pernambuco": "PE",
    }
    expected = []
    for year in range(1985, 2026):
        column = next(column for column, value in oracle["header"].items() if value == f"y{year}")
        for record in oracle["rows"]:
            cells = record["cells"]
            expected.append(
                (
                    cells["C"],
                    states[cells["E"]],
                    cells["G"],
                    int(cells["I"]),
                    cells["J"],
                    year,
                    float(cells[column]),
                    cells["F"],
                    int(cells["A"]),
                )
            )
    selected = frame.drop(columns="classe")
    assert list(selected.itertuples(index=False, name=None)) == expected
    assert len(frame) == 142 * 41
    assert details["coverage"]["validated_rows"] == 142
    assert details["coverage"]["annual_cells"] == 5822
    assert details["coverage"]["eof_reached"] is True
    assert details["geocodes_with_multiple_states"] == [
        {"geocodigo": "2703007", "estados": ["AL", "PE"]}
    ]
    assert details["warnings"] == []


def test_all_selectors_preserve_year_major_and_typed_output():
    content = _xlsx(
        [_row(), _row(ID=2, geocode="5107925", municipality="Sorriso", state="Mato Grosso")]
    )
    frame, details = municipal_parser.parse_cobertura_municipal(
        content,
        colecao=11,
        bioma="Cerrado",
        uf="MT",
        geocodigo="5107925",
        classe_id=3,
    )
    assert frame["ano"].tolist() == list(range(1985, 2026))
    assert frame["area_ha"].tolist() == [float(value) for value in range(1, 42)]
    assert frame.columns.tolist() == [
        *models.COLUNAS_SAIDA_COBERTURA_MUNICIPAL,
        "geocodigo",
        "id_registro",
    ]
    assert str(frame["ano"].dtype) == str(frame["classe_id"].dtype) == "Int64"
    assert str(frame["id_registro"].dtype) == "Int64"
    assert str(frame["area_ha"].dtype) == "float64"
    assert all(
        frame[column].dtype == pd.Series([""]).dtype
        for column in ["bioma", "uf", "municipio", "classe", "nivel_0", "geocodigo"]
    )
    assert details["coverage"]["validated_rows"] == 2
    assert details["annual_statistics"]["2025"] == {
        "count": 2,
        "null_count": 0,
        "zero_count": 0,
        "minimum_ha": 41.0,
        "maximum_ha": 41.0,
    }


@pytest.mark.parametrize(
    "invalid", [None, True, -1.0, "1.2", "invalid", "=1+1", datetime(2025, 1, 1)]
)
def test_invalid_area_outside_selected_year_and_state_aborts(invalid: object):
    rows = [
        _row(),
        _row(ID=2, geocode="5107925", municipality="Sorriso", state="Mato Grosso", y2025=invalid),
    ]
    with pytest.raises(ParseError, match="linha 3"):
        municipal_parser.parse_cobertura_municipal(_xlsx(rows), colecao=11, uf="DF", ano=1985)


@pytest.mark.parametrize(
    "field,invalid",
    [
        ("geocode", 5300108),
        ("geocode", "５３００１０８"),
        ("geocode", "530010"),
        ("geocode", "5300108 "),
        ("class", True),
        ("class", 3.5),
        ("municipality", " "),
        ("class_level_0", ""),
        ("state", "Unknown"),
        ("biome", "Unknown"),
        ("country", "Outro"),
    ],
)
def test_invalid_identity_outside_filter_aborts(field: str, invalid: object):
    content = _xlsx([_row(), _row(ID=2, **{field: invalid})])
    with pytest.raises(ParseError):
        municipal_parser.parse_cobertura_municipal(content, colecao=11, uf="AC")


def test_duplicate_published_id_outside_filter_aborts():
    with pytest.raises(ParseError, match="ID publicado duplicado"):
        municipal_parser.parse_cobertura_municipal(
            _xlsx([_row(), _row(geocode="5300109")]), colecao=11, uf="AC"
        )


def test_geocode_name_conflict_within_same_state_aborts():
    rows = [_row(), _row(ID=2, municipality="Outro nome", **{"class": 4})]
    with pytest.raises(ParseError, match="geocode/nome"):
        municipal_parser.parse_cobertura_municipal(_xlsx(rows), colecao=11)


def test_zero_is_observation_and_empty_physical_row_is_counted():
    content = _xlsx([_row(y1985=0), [None] * 55, _row(ID=2, geocode="5300109")])
    frame, details = municipal_parser.parse_cobertura_municipal(content, colecao=11, ano=1985)
    assert frame["area_ha"].tolist() == [0.0, 1.0]
    assert details["coverage"]["physical_rows"] == 3
    assert details["coverage"]["empty_rows"] == 1
    assert details["annual_statistics"]["1985"]["zero_count"] == 1


@pytest.mark.parametrize(
    "action", ["missing_year", "duplicate_year", "extra_year", "missing_geocode", "unknown_column"]
)
def test_incompatible_header_aborts(action: str):
    headers: list[object] = list(HEADERS)
    row = _row()
    if action == "missing_year":
        headers.pop()
        row.pop()
    elif action == "duplicate_year":
        headers[-1] = "1985"
    elif action == "extra_year":
        headers[-1] = "y2026"
    elif action == "missing_geocode":
        headers.pop(5)
        row.pop(5)
    else:
        headers[3] = "unrecognized_region"
    with pytest.raises(ParseError):
        municipal_parser.parse_cobertura_municipal(_xlsx([row], headers), colecao=11)


def test_official_collection10_preserves_ids_areas_and_repeated_territorial_keys():
    oracle = json.loads(
        (GOLDEN_10 / "municipal10_duplicates_minimal_oracle.json").read_text(encoding="utf-8")
    )
    frame, details = municipal_parser.parse_cobertura_municipal(
        (GOLDEN_10 / "municipal10_duplicates_minimal.xlsx").read_bytes(),
        colecao=10,
    )
    expected = []
    for year in range(1985, 2025):
        column = next(
            column for column, value in oracle["header"].items() if str(value) == str(year)
        )
        for record in oracle["rows"]:
            cells = record["cells"]
            expected.append(
                (
                    int(cells["A"]),
                    cells["G"],
                    cells["E"],
                    int(cells["I"]),
                    year,
                    float(cells[column]),
                )
            )
    actual = frame[["id_registro", "geocodigo", "municipio", "classe_id", "ano", "area_ha"]]
    assert list(actual.itertuples(index=False, name=None)) == expected
    assert len(frame) == 200
    assert details["territorial_keys"] == {
        "unique": 2,
        "repeated_groups": 2,
        "rows_in_repeated_groups": 5,
        "additional_rows": 3,
    }
    assert details["warnings"] == []


def test_official_collection10_classes_zero_and_thirteen_use_municipal_legend():
    oracle = json.loads(
        (GOLDEN_10 / "municipal10_classes_0_13_oracle.json").read_text(encoding="utf-8")
    )
    frame, _ = municipal_parser.parse_cobertura_municipal(
        (GOLDEN_10 / "municipal10_classes_0_13.xlsx").read_bytes(),
        colecao=10,
    )
    expected = []
    for year in range(1985, 2025):
        column = next(
            column for column, value in oracle["header"].items() if str(value) == str(year)
        )
        for record in oracle["rows"]:
            cells = record["cells"]
            expected.append(
                (int(cells["A"]), int(cells["I"]), cells["J"], year, float(cells[column]))
            )
    actual = frame[["id_registro", "classe_id", "nivel_0", "ano", "area_ha"]]
    assert list(actual.itertuples(index=False, name=None)) == expected
    assert frame["classe"].drop_duplicates().tolist() == [
        "Não observado",
        "Outras Formações não Florestais",
    ]
    assert models.classe_para_nome(0, 10) == "Não observado"
    assert models.classe_para_nome(13, 10) == "Outras Formações não Florestais"
    assert models.classe_para_nome(13, 11) == "Mosaico Herbáceo-Arbustivo"


def test_corrupt_archive_aborts():
    with pytest.raises(ParseError):
        municipal_parser.parse_cobertura_municipal(_xlsx([_row()])[:-30], colecao=11)

from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from agrobr.contracts.mapbiomas import MAPBIOMAS_COBERTURA_MUNICIPAL_V1
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente


@pytest.fixture
def municipal_frame():
    return pd.DataFrame(
        {
            "bioma": ["Mata Atlântica", "Mata Atlântica"],
            "uf": ["AL", "PE"],
            "municipio": ["Ibateguara", "Ibateguara"],
            "classe_id": pd.Series([15, 15], dtype="Int64"),
            "classe": ["Pastagem", "Pastagem"],
            "nivel_0": ["Antropic", "Antropic"],
            "ano": pd.Series([2025, 2025], dtype="Int64"),
            "area_ha": pd.Series([0, 1.25], dtype="float64"),
            "geocodigo": ["2703007", "2703007"],
            "id_registro": pd.Series([0, 1], dtype="Int64"),
        }
    )


def test_contract_empty_frame_has_identical_physical_types(municipal_frame):
    empty = MAPBIOMAS_COBERTURA_MUNICIPAL_V1.empty_frame()
    assert MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(empty) == (True, [])
    assert (
        empty.dtypes[["classe_id", "ano", "area_ha", "id_registro"]].to_dict()
        == municipal_frame.dtypes[["classe_id", "ano", "area_ha", "id_registro"]].to_dict()
    )
    assert all(
        empty[column].dtype == municipal_frame[column].dtype
        for column in ["bioma", "uf", "municipio", "classe", "nivel_0", "geocodigo"]
    )
    assert empty.empty


@pytest.mark.parametrize("geocodigo", ["4300001", "4300002", "0000001"])
def test_contract_geocode_does_not_claim_ibge_or_state_prefix(geocodigo, municipal_frame):
    municipal_frame["geocodigo"] = geocodigo
    assert MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame) == (True, [])


@pytest.mark.parametrize("field", ["uf", "bioma", "classe_id", "ano", "geocodigo", "id_registro"])
def test_primary_key_preserves_each_published_dimension(field, municipal_frame):
    municipal_frame.iloc[1] = municipal_frame.iloc[0]
    replacements = {
        "uf": "PE",
        "bioma": "Cerrado",
        "classe_id": 0,
        "ano": 1985,
        "geocodigo": "4300001",
        "id_registro": 1,
    }
    municipal_frame.loc[1, field] = replacements[field]
    assert MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame) == (True, [])


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (field, value)
        for field in ["bioma", "uf", "municipio", "classe", "nivel_0", "geocodigo"]
        for value in [None, "", "  ", 123]
        if (field, value) != ("classe", None)
    ],
)
def test_text_columns_reject_missing_blank_or_non_string(field, value, municipal_frame):
    municipal_frame[field] = municipal_frame[field].astype(object)
    municipal_frame.loc[0, field] = value
    valid, errors = MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame)
    assert not valid and errors


def test_classe_nula_e_aceita(municipal_frame):
    municipal_frame["classe"] = municipal_frame["classe"].astype(object)
    municipal_frame.loc[0, "classe"] = None
    assert MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame) == (True, [])


@pytest.mark.parametrize(
    "value",
    ["270300", "27030070", "２７０３００７", "270300A", " 2703007", "2703007 ", "2703007\n"],
)
def test_geocode_requires_exactly_seven_ascii_digits(value, municipal_frame):
    municipal_frame.loc[0, "geocodigo"] = value
    assert not MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame)[0]


@pytest.mark.parametrize(
    "field,dtype",
    [
        ("classe_id", "int64"),
        ("id_registro", "int64"),
        ("ano", "float64"),
        ("ano", "object"),
        ("area_ha", "Float64"),
        ("area_ha", "int64"),
        ("area_ha", "object"),
    ],
)
@pytest.mark.parametrize("empty", [False, True])
def test_contract_requires_declared_numeric_dtypes(field, dtype, empty, municipal_frame):
    municipal_frame[field] = pd.Series([1, 2], dtype=dtype)
    if empty:
        municipal_frame = municipal_frame.iloc[:0]
    valid, errors = MAPBIOMAS_COBERTURA_MUNICIPAL_V1.validate(municipal_frame)
    assert not valid and any("dtype" in error for error in errors)


@pytest.mark.parametrize(
    "damage,violation",
    [
        (
            "missing",
            "Missing required columns: {'geocodigo'}; Primary key columns missing: ['geocodigo']",
        ),
        ("duplicate", "Duplicate column labels: ['geocodigo']"),
    ],
)
def test_invalid_column_layout_returns_errors(damage, violation, municipal_frame):
    frame = (
        municipal_frame.drop(columns="geocodigo")
        if damage == "missing"
        else pd.concat([municipal_frame, municipal_frame[["geocodigo"]]], axis=1)
    )
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(frame, MAPBIOMAS_COBERTURA_MUNICIPAL_V1)
    assert erro.value.violation == violation

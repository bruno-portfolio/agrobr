from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr import constants, contracts
from agrobr.contracts import desmatamento
from agrobr.desmatamento import parser
from agrobr.exceptions import ContractViolationError
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data/desmatamento/selecao_20260907"
CONTRACTS = {
    "desmatamento_prodes_feicoes": desmatamento.PRODES_FEICOES_V2,
    "desmatamento_deter_feicoes": desmatamento.DETER_FEICOES_V2,
    "desmatamento_prodes": desmatamento.DESMATAMENTO_PRODES_V2,
    "desmatamento_deter": desmatamento.DESMATAMENTO_DETER_V2,
}
CAPTURES = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8-sig"))["captures"]


@pytest.fixture(params=CONTRACTS)
def contract(request):
    return CONTRACTS[request.param]


@pytest.fixture
def frame(contract):
    values = {
        "ano": 2024,
        "uf": "MT",
        "classe": "DESMATAMENTO",
        "area_km2": 0.0,
        "bioma": "Amazônia",
        "feature_id": "layer.001",
        "fid": 1,
        "scene_id": "9007199254740993",
        "municipio": "Município publicado",
        "municipio_id": "001",
        "cod_municipio": pd.NA,
    }
    result = contract.empty_frame()
    for column in contract.columns:
        value = values.get(column.name, "")
        if column.type == contracts.ColumnType.DATE:
            value = pd.Timestamp("2024-02-29")
        elif column.type == contracts.ColumnType.FLOAT:
            value = float(values.get(column.name, 0))
        result[column.name] = pd.Series([value], dtype=result[column.name].dtype)
    return result


@pytest.mark.parametrize("case", CAPTURES, ids=lambda case: case["file"])
def test_official_layout_satisfies_feature_contract(case):
    page = parser.parse_page(
        (GOLDEN / case["file"]).read_bytes(), product=case["product"], biome=case["biome"]
    )
    frame = parser.build_frame(page.records, product=case["product"], biome=case["biome"])
    contract = CONTRACTS[f"desmatamento_{case['product'].lower()}_feicoes"]
    expected = (
        constants.DESMATAMENTO_PRODES_COLUMNS
        if case["product"] == "PRODES"
        else constants.DESMATAMENTO_DETER_COLUMNS
    )
    assert list(frame) == list(expected) == contract.list_columns()
    assert contract.validate(frame) == (True, [])


@pytest.mark.parametrize("column,value", [("uf", "mt"), ("uf", "XX"), ("bioma", "amazonia")])
def test_normalized_geography_domain_is_enforced(contract, frame, column, value):
    frame.loc[0, column] = value
    valid, errors = contract.validate(frame)
    assert not valid and errors


def test_text_requires_default_pandas_dtype(contract, frame):
    assert frame["bioma"].dtype == pd.Series([""]).dtype
    frame["bioma"] = frame["bioma"].astype("string[python]")
    valid, errors = contract.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize("offset", [pd.Timedelta(1, unit="ns"), pd.Timedelta(hours=1)])
@pytest.mark.parametrize(
    "contract",
    [
        desmatamento.PRODES_FEICOES_V2,
        desmatamento.DETER_FEICOES_V2,
        desmatamento.DESMATAMENTO_DETER_V2,
    ],
)
def test_date_requires_civil_midnight(contract, frame, offset):
    date_column = next(
        column.name for column in contract.columns if column.type == contracts.ColumnType.DATE
    )
    frame.loc[0, date_column] += offset
    valid, errors = contract.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize("value", [None, "", " ", "\t"])
@pytest.mark.parametrize("name", ["desmatamento_prodes_feicoes", "desmatamento_deter_feicoes"])
def test_feature_identifier_requires_nonblank_published_text(name, value):
    contract = CONTRACTS[name]
    frame = contract.empty_frame()
    frame.loc[0, "bioma"] = "Amazônia"
    frame.loc[0, "feature_id"] = value
    valid, errors = contract.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize("value", ["", " ", "NaN", "null", "01", "+1", ".1", "1.", "1e", " 1"])
def test_scene_identifier_rejects_non_json_number_text(value):
    contract = desmatamento.PRODES_FEICOES_V2
    frame = contract.empty_frame()
    frame.loc[0, "bioma"] = "Amazônia"
    frame.loc[0, "feature_id"] = "layer.1"
    frame.loc[0, "scene_id"] = value
    valid, errors = contract.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize(
    "value,dtype", [(2024.5, "float64"), (2024.0, "float64"), (0, "Int64"), (10000, "Int64")]
)
def test_aggregate_year_requires_integral_calendar_year(value, dtype):
    contract = desmatamento.DESMATAMENTO_PRODES_V2
    frame = pd.DataFrame(
        {
            "ano": pd.Series([value], dtype=dtype),
            "uf": pd.Series(["MT"], dtype=desmatamento.TEXTO),
            "classe": pd.Series(["DESMATAMENTO"], dtype=desmatamento.TEXTO),
            "area_km2": pd.Series([0.0], dtype="float64"),
            "satelite": pd.Series([None], dtype=desmatamento.TEXTO),
            "sensor": pd.Series([None], dtype=desmatamento.TEXTO),
            "bioma": pd.Series(["Amazônia"], dtype=desmatamento.TEXTO),
        }
    )
    valid, errors = contract.validate(frame)
    assert not valid and errors


@pytest.mark.parametrize("value", ["", " "])
def test_blank_class_is_preserved_in_source_and_rejected_in_aggregate(contract, frame, value):
    frame.loc[0, "classe"] = value
    valid, errors = contract.validate(frame)
    assert valid is (not contract.primary_key)
    assert bool(errors) is bool(contract.primary_key)


def test_missing_column_is_reported(contract, frame):
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(frame.drop(columns="bioma"), contract)
    faltas = ["Missing required columns: {'bioma'}"]
    if "bioma" in contract.primary_key:
        faltas.append("Primary key columns missing: ['bioma']")
    assert erro.value.violation == "; ".join(faltas)


def test_duplicate_column_is_reported_without_exception(contract, frame):
    with levanta_exatamente(ContractViolationError) as erro:
        contracts.validate_dataset(pd.concat([frame, frame[["bioma"]]], axis=1), contract)
    assert erro.value.violation == "Duplicate column labels: ['bioma']"

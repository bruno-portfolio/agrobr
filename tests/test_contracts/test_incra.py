from __future__ import annotations

import pandas as pd
import pytest

from agrobr import contracts
from tests.helpers import INCRA_EXPECTED_ALIASES, incra_expected_frame


@pytest.fixture
def contract():
    return contracts.get_contract("incra_quilombolas")


@pytest.fixture
def frame():
    return incra_expected_frame()


def test_incra_contract_official_values_and_empty_schema(contract, frame):
    assert contract.name == "incra.quilombolas"
    assert contract.version == "2.0" and contract.primary_key == []
    assert contract.list_columns() == list(INCRA_EXPECTED_ALIASES)
    assert contract.validate(frame) == (True, [])
    assert frame["codigo"].tolist() == [0] * 6
    assert pd.isna(frame.loc[5, "area_ha"])
    empty = contract.empty_frame()
    assert empty.empty and empty.dtypes.to_dict() == frame.dtypes.to_dict()
    assert contract.validate(empty) == (True, [])


@pytest.mark.parametrize("value", [None, "", " ", "\t\n"])
def test_incra_contract_feature_id_requires_nonblank_text(contract, frame, value):
    frame.loc[0, "feature_id"] = value
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("name", ["codigo", "familias"])
@pytest.mark.parametrize(
    "value,valid",
    [(-(2**31) - 1, False), (-(2**31), True), (0, True), (2**31 - 1, True), (2**31, False)],
)
def test_incra_contract_signed32_domain(contract, frame, name, value, valid):
    frame.loc[0, name] = value
    assert contract.validate(frame)[0] is valid
    assert frame.loc[0, name] == value


@pytest.mark.parametrize(
    "name,dtype",
    [
        ("codigo", "float64"),
        ("familias", "int32"),
        ("data_titulo", "object"),
        ("area_ha", "float32"),
        ("area_ha", "Float64"),
    ],
)
def test_incra_contract_alternative_dtype_rejected(contract, frame, name, dtype):
    frame[name] = frame[name].astype(dtype)
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("mutation", ["missing", "extra", "reordered", "duplicate"])
def test_incra_contract_complete_ordered_projection(contract, frame, mutation):
    if mutation == "missing":
        frame = frame.drop(columns="data_cadastro")
    elif mutation == "extra":
        frame["unpublished"] = None
    elif mutation == "reordered":
        frame = frame.loc[:, list(frame)[::-1]]
    else:
        frame = pd.concat([frame, frame[["codigo"]]], axis=1)
    assert not contract.validate(frame)[0]


def test_incra_contract_geometry_is_optional_last_column(contract, frame):
    frame["geometry"] = None
    assert contract.validate(frame) == (True, [])


@pytest.fixture
def administrative():
    contract = contracts.get_contract("incra_andamento_quilombola")
    frame = contract.empty_frame()
    frame.loc[0] = [
        "SR(01)AC",
        1,
        "54330.000697/2006-18",
        "A",
        "X",
        "",
        "",
        "",
        "Em Elaboração",
        "",
        "",
        "",
        "",
        "",
        "Parcial",
    ]
    frame.loc[1] = [
        "SR(01)AC",
        2,
        "54330.000697/2006-18",
        "B",
        "Y",
        "1.000,0000",
        "Não consta",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]
    for name in frame:
        frame[name] = frame[name].astype(
            "Int64" if name == "numero_publicado" else pd.Series([""]).dtype
        )
    return contract, frame


@pytest.mark.parametrize("name", ["processo", "comunidade", "municipio"])
def test_incra_administrative_empty_text_remains_representable(administrative, name):
    contract, frame = administrative
    frame.loc[0, name] = ""
    assert contract.validate(frame) == (True, [])


@pytest.mark.parametrize(
    "name,value",
    [
        ("regional", " "),
        ("regional", None),
        ("numero_publicado", 0),
        ("numero_publicado", 3),
        ("processo", None),
    ],
)
def test_incra_administrative_structural_violation(administrative, name, value):
    contract, frame = administrative
    frame.loc[0, name] = value
    assert not contract.validate(frame)[0]


def test_incra_administrative_empty_frame_has_public_dtypes(administrative):
    contract, frame = administrative
    empty = contract.empty_frame()
    assert empty.dtypes.to_dict() == frame.dtypes.to_dict()
    assert contract.validate(empty) == (True, [])

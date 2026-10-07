from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from agrobr import constants, contracts
from agrobr.contracts import embrapa_solos
from agrobr.embrapa_solos import parser

GOLDEN = Path(__file__).parents[1] / "golden_data/embrapa_solos/official_20260907"
CONTRACTS = {
    "perfis": embrapa_solos.PERFIS_V3,
    "mapa": embrapa_solos.MAPA_V2,
}


@pytest.fixture(params=CONTRACTS)
def family(request):
    return request.param


@pytest.fixture
def contract(family):
    return CONTRACTS[family]


@pytest.fixture
def frame(contract):
    values = {
        "fid": 1,
        "codigo_pon": 2**53 + 1,
        "feature_id": "published.001",
        "uf": "SC",
        "uf_original": " sc ",
        "latitude": -27.19443726,
        "longitude": -49.47196232,
        "area_km2": 0.0,
        "fosforo": "<1",
        "ph_h2o": "4.400000095367432",
    }
    result = contract.empty_frame()
    for column in contract.columns:
        result[column.name] = pd.Series([values.get(column.name)], dtype=result[column.name].dtype)
    return result


def test_embrapa_contract_registry_version_and_valid_frame(family, contract, frame):
    assert contracts.get_contract(f"embrapa_solos_{family}") == contract
    assert contract.version == ("3.0" if family == "perfis" else "2.0")
    assert contract.primary_key == []
    assert contract.validate(frame) == (True, [])


@pytest.mark.parametrize("value", ["", " ", "\n\t"])
def test_embrapa_feature_id_requires_nonblank_text(contract, frame, value):
    frame.loc[0, "feature_id"] = value
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("dtype", ["object", "float64", "int32"])
def test_embrapa_integer_dtype_rejects_coercible_alternative(contract, frame, dtype):
    frame["fid"] = frame["fid"].astype(dtype)
    assert not contract.validate(frame)[0]


def test_embrapa_text_rejects_forced_nullable_storage(contract, frame):
    frame["feature_id"] = frame["feature_id"].astype("string[python]")
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("value", ["SP", "sc", "XX", ""])
def test_embrapa_normalized_uf_must_match_original(value):
    frame = embrapa_solos.PERFIS_V3.empty_frame()
    for column in frame:
        cell = {"fid": 1, "feature_id": "published.1", "uf_original": " sc ", "uf": value}.get(
            column
        )
        frame[column] = pd.Series([cell], dtype=frame[column].dtype)
    assert not embrapa_solos.PERFIS_V3.validate(frame)[0]


@pytest.mark.parametrize("mutation", ["missing", "extra", "order", "duplicate"])
def test_embrapa_complete_attribute_shape_is_required(contract, frame, mutation):
    if mutation == "missing":
        frame = frame.drop(columns="fid")
    elif mutation == "extra":
        frame["unpublished"] = 1
    elif mutation == "order":
        frame = frame.loc[:, list(frame)[::-1]]
    else:
        frame = pd.concat([frame, frame[["fid"]]], axis=1)
    assert not contract.validate(frame)[0]


@pytest.mark.parametrize("include_geometry", [False, True])
def test_embrapa_official_complete_projection_meets_contract(family, contract, include_geometry):
    filename = f"{family}_{'geo' if include_geometry else 'reference'}.json"
    body = (GOLDEN / filename).read_bytes()
    page = parser.parse_page(body, product=family, include_geometry=include_geometry)
    frame = parser.build_frame(page.records, product=family)
    if family == "perfis":
        parser.converter_datas(frame)
    assert contract.validate(frame) == (True, [])
    assert frame.dtypes.equals(contract.empty_frame().dtypes)
    assert list(frame) == list(
        constants.EMBRAPA_SOLOS_PERFIS_COLUMNS
        if family == "perfis"
        else constants.EMBRAPA_SOLOS_MAPA_COLUMNS
    )
    if family == "perfis":
        assert str(frame["ano"].dtype) == "Int64"
        assert str(frame["data_colet"].dtype) == "datetime64[ns]"
        assert frame.loc[0, "ano"] == 2006
        assert frame.loc[0, "data_colet"] == pd.Timestamp(2006, 2, 8)
        assert frame.loc[0, "fosforo"] == "8"
        assert frame.loc[0, "codigo_pon"] == 5762
        assert frame.loc[0, "ph_h2o"] == "4.400000095367432"
        if not include_geometry:
            assert pd.isna(frame.loc[1, "ano"])
            assert frame.loc[5, "fosforo"] == "<1"

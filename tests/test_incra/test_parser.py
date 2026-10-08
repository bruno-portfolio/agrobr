from __future__ import annotations

import copy
import json
import math
from pathlib import Path

import pandas as pd
import pytest

from agrobr.exceptions import ParseError
from agrobr.incra import parser
from agrobr.utils.result import ATRIBUTO_AVISOS
from tests.helpers import incra_expected_frame

GOLDEN = Path(__file__).parents[1] / "golden_data/incra/wfs_v2_20260908"


@pytest.fixture
def payload():
    return json.loads((GOLDEN / "reference.json").read_bytes())


def test_official_tabular_all_cells_and_dtypes():
    page = parser.parse_page((GOLDEN / "reference.json").read_bytes(), include_geometry=False)
    frame = parser.build_frame(page.records)
    parser.converter_datas(frame)
    pd.testing.assert_frame_equal(frame, incra_expected_frame())
    assert page.parser_version == parser.PARSER_VERSION
    assert page.source_rows == page.returned_count == 6
    assert page.reported_count == 444
    assert frame["codigo"].tolist() == [0] * 6
    assert frame["area_ha"].isna().tolist() == [False] * 5 + [True]
    assert page.geometries is None
    assert page.sort_keys[0] == (0, "54160.001672/2013-51", "MOTA")
    assert page.source_timestamp == "2026-09-08T01:36:34.091Z"


def test_default_4674_geometry_is_not_relabelled_4326():
    with pytest.raises(ParseError, match="CRS"):
        parser.parse_page((GOLDEN / "geo_default.json").read_bytes(), include_geometry=True)


@pytest.mark.parametrize(
    "name,value",
    [
        ("numberMatched", 5),
        ("numberReturned", 5),
        ("totalFeatures", 445),
        ("numberMatched", True),
        ("numberReturned", 6.0),
        ("totalFeatures", None),
    ],
)
def test_contradictory_or_noninteger_counts_fail(payload, name, value):
    payload[name] = value
    with pytest.raises(ParseError):
        parser.parse_page(json.dumps(payload).encode(), include_geometry=False)


def test_invalid_date_retains_layout_error_context(payload):
    payload["features"][0]["properties"]["dt_public1"] = "2023-02-29"
    with pytest.raises(ParseError, match="dt_public1"):
        parser.parse_page(json.dumps(payload).encode(), include_geometry=False)


def test_cadastro_com_ano_fora_de_1900_2099_vira_nat_com_aviso(payload):
    cadastros = ["0982-11-01T10:00:00Z", "1850-11-01T10:00:00Z", "2026-09-07T13:40:09-03:00"]
    for feature, cadastro in zip(payload["features"], cadastros):
        feature["properties"]["dt_cadastro"] = cadastro
    frame = parser.build_frame(
        parser.parse_page(json.dumps(payload).encode(), include_geometry=False).records
    )
    with pytest.warns(UserWarning) as capturados:
        parser.converter_datas(frame)
    aviso = (
        "incra: 2 valor(es) de data_cadastro viraram NaT (data ilegível ou com ano fora de "
        "1900–2099)."
    )
    assert aviso in [str(capturado.message) for capturado in capturados]
    assert aviso in frame.attrs[ATRIBUTO_AVISOS]
    assert str(frame["data_cadastro"].dtype) == "datetime64[ns, UTC]"
    assert frame["data_cadastro"].isna().tolist() == [True, True] + [False] * (len(frame) - 2)
    assert (frame["data_cadastro"].iloc[2:] == pd.Timestamp("2026-09-07T16:40:09Z")).all()


def test_integer_signed_zero_is_same_value_but_area_signed_zero_is_distinct(payload):
    first = copy.deepcopy(payload["features"][0])
    second = copy.deepcopy(first)
    payload["features"] = [first, second]
    payload["numberReturned"] = 2
    second["properties"]["cd_quilomb"] = "NEGATIVE_ZERO"
    body = json.dumps(payload).replace('"NEGATIVE_ZERO"', "-0.0").encode()
    page = parser.parse_page(body, include_geometry=False)
    assert page.signatures[0] == page.signatures[1]
    first["properties"]["nu_area_ha"] = 0
    second["properties"]["nu_area_ha"] = "NEGATIVE_ZERO"
    body = json.dumps(payload).replace('"NEGATIVE_ZERO"', "-0.0").encode()
    page = parser.parse_page(body, include_geometry=False)
    assert page.signatures[0] != page.signatures[1]
    assert math.copysign(1, parser.build_frame(page.records).loc[1, "area_ha"]) == -1


def test_diagnostics_keep_integral_counts_and_bounded_examples(payload):
    feature = copy.deepcopy(payload["features"][0])
    feature["properties"].update(
        sg_uf="XX", nu_area_ha=-1, nu_familia=-1, ds_descricao="", no_responsavel=" "
    )
    payload["features"] = [copy.deepcopy(feature) for _ in range(13)]
    payload["numberReturned"] = 13
    page = parser.parse_page(json.dumps(payload).encode(), include_geometry=False)
    assert page.statistics["ds_descricao"]["empty_count"] == 13
    assert page.statistics["no_responsavel"]["whitespace_count"] == 13
    assert page.statistics["dt_public1"]["null_count"] == 13
    for name in ("unknown_uf_token", "negative_area", "negative_families"):
        assert page.diagnostics.get(name) == {
            "count": 13,
            "examples": [{"row_index": index} for index in range(10)],
            "examples_omitted": 3,
        }
    assert len(page.warnings) == 3


def test_null_empty_and_unrequested_geometries_are_distinct():
    raw = json.loads((GOLDEN / "geo.json").read_bytes())
    raw["features"][0]["geometry"] = None
    null = parser.parse_page(json.dumps(raw).encode(), include_geometry=True)
    assert null.geometries == [None] and null.diagnostics["null_geometry"]["count"] == 1
    raw["features"][0]["geometry"] = {"type": "MultiPolygon", "coordinates": []}
    empty = parser.parse_page(json.dumps(raw).encode(), include_geometry=True)
    assert empty.geometries == [{"type": "MultiPolygon", "coordinates": []}]
    assert empty.diagnostics["empty_geometry"]["count"] == 1
    unrequested = parser.parse_page((GOLDEN / "geo.json").read_bytes(), include_geometry=False)
    assert unrequested.geometries is None and unrequested.records[0].geometry is None
    assert unrequested.diagnostics["unexpected_geometry"]["count"] == 1


def test_conflicting_next_links_fail(payload):
    payload["next"] = "https://example.invalid/different"
    with pytest.raises(ParseError, match="continuação"):
        parser.parse_page(json.dumps(payload).encode(), include_geometry=False)

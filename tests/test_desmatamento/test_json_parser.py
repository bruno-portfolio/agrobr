from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from agrobr.desmatamento import parser
from agrobr.exceptions import ParseError
from tests.helpers import levanta_exatamente

GOLDEN = Path(__file__).parents[1] / "golden_data/desmatamento/selecao_20260907"
CASES = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8-sig"))["captures"]
ALIASES = {
    "year": "ano",
    "state": "estado_original",
    "main_class": "classe",
    "area_km": "area_km2",
    "satellite": "satelite",
    "view_date": "data",
    "classname": "classe",
    "municipality": "municipio",
    "mun_geocod": "municipio_id",
    "areamunkm": "area_km2",
    "uf": "uf_original",
}
DATES = {"image_date", "publish_year", "view_date", "publish_month", "created_date"}


def source_payload(name: str = "prodes_caatinga") -> dict:
    return json.loads((GOLDEN / f"{name}.json").read_bytes())


def parse_payload(
    payload: dict, *, product: str = "PRODES", biome: str = "Caatinga", geo: bool = False
):
    return parser.parse_page(
        json.dumps(payload).encode(), product=product, biome=biome, include_geometry=geo
    )


@pytest.mark.parametrize("case", CASES, ids=lambda value: value["file"])
def test_official_page_all_properties_and_typed_frame(case):
    content = (GOLDEN / case["file"]).read_bytes()
    assert hashlib.sha256(content).hexdigest() == case["sha256"]
    raw = json.loads(content)
    page = parser.parse_page(content, product=case["product"], biome=case["biome"])
    frame = parser.build_frame(page.records, product=case["product"], biome=case["biome"])
    assert len(frame) == page.source_rows == 6
    assert frame["bioma"].tolist() == [case["biome"]] * 6
    assert frame["feature_id"].tolist() == [feature["id"] for feature in raw["features"]]
    assert len(frame.columns) == (20 if case["product"] == "PRODES" else 19)
    for index, feature in enumerate(raw["features"]):
        for name, expected in feature["properties"].items():
            observed = frame.iloc[index][ALIASES.get(name, name)]
            if expected is None:
                assert pd.isna(observed), (index, name)
            elif name in DATES:
                assert observed == pd.Timestamp(expected), (index, name)
            elif name == "scene_id":
                assert observed == str(expected), (index, name)
            else:
                assert observed == expected, (index, name)
    assert frame["area_km2"].dtype == "float64"
    if case["product"] == "PRODES":
        assert str(frame["ano"].dtype) == "Int64"
    for name in frame.select_dtypes(include="string").columns:
        assert frame[name].dtype.storage == "python"


@pytest.mark.parametrize("value", [None, "", " ", 1, 1.2, True])
def test_feature_id_requires_nonblank_json_string(value):
    payload = source_payload()
    payload["features"][0]["id"] = value
    with pytest.raises(ParseError):
        parse_payload(payload)


@pytest.mark.parametrize(
    "value",
    [
        "",
        "2023-02-29",
        "2024-01-01Z",
        "2024-01-01T00:00:00",
        "1500-01-01",
        "2263-01-01",
        20240101,
        True,
    ],
)
def test_civil_date_rejects_invalid_or_unsupported_value(value):
    payload = source_payload()
    payload["features"][0]["properties"]["image_date"] = value
    with pytest.raises(ParseError):
        parse_payload(payload)


@pytest.mark.parametrize("tokens", [("1502", "1502.0"), ("-0", "0"), ("1e2", "100")])
def test_signature_preserves_scene_identifier_lexical_difference(tokens):
    payload = source_payload()
    payload["features"][0]["properties"]["scene_id"] = "TOKEN"
    signatures = []
    values = []
    for token in tokens:
        page = parser.parse_page(
            json.dumps(payload).replace('"TOKEN"', token).encode(),
            product="PRODES",
            biome="Caatinga",
        )
        signatures.append(page.signatures[0])
        values.append(
            parser.build_frame(page.records, product="PRODES", biome="Caatinga").loc[0, "scene_id"]
        )
    assert signatures[0] != signatures[1]
    assert tuple(values) == tokens


def test_prodes_ano_decimal_recusado():
    payload = source_payload("prodes_caatinga")
    payload["features"][0]["properties"]["year"] = 2019.5
    page = parse_payload(payload)

    with levanta_exatamente(ParseError, match="Ano do PRODES não inteiro"):
        parser.build_frame(page.records, product="PRODES", biome="Caatinga")


def test_tabular_pampa_discards_geometry_without_retention():
    page = parser.parse_page(
        (GOLDEN / "prodes_pampa.json").read_bytes(), product="PRODES", biome="Pampa"
    )
    assert page.geometries is None and page.details["geometry_received_count"] == 6
    assert page.warnings and page.details["geometry_serialized_bytes_estimate"] > 0
    assert all("geometry" not in record.model_dump() for record in page.records)


@pytest.mark.parametrize("crs", [None, "urn:ogc:def:crs:EPSG::4674"])
def test_geo_rejects_missing_or_unrequested_crs(crs):
    payload = source_payload("prodes_pampa")
    payload["crs"] = None if crs is None else {"type": "name", "properties": {"name": crs}}
    with pytest.raises(ParseError):
        parse_payload(payload, biome="Pampa", geo=True)


@pytest.mark.parametrize(
    "counts",
    [
        {"numberMatched": 1},
        {"numberReturned": 1},
        {"numberMatched": True},
        {"numberMatched": "6"},
        {"numberReturned": -1},
    ],
)
def test_invalid_counts_rejected(counts):
    payload = source_payload()
    payload.update(counts)
    with pytest.raises(ParseError):
        parse_payload(payload)


@pytest.mark.parametrize("case", CASES, ids=lambda value: value["file"])
def test_polars_nullable_text_dtype_is_stable(case):
    pl = pytest.importorskip("polars")
    frame = parser.build_frame([], product=case["product"], biome=case["biome"])
    result = pl.from_pandas(frame)
    assert result.schema["feature_id"] == pl.Utf8
    if case["product"] == "PRODES":
        assert result.schema["scene_id"] == pl.Utf8
    else:
        assert result.schema["municipio_id"] == pl.Utf8


def test_unknown_uf_preserved_with_diagnostic():
    payload = source_payload()
    payload["features"][0]["properties"]["state"] = "unknown state"
    page = parse_payload(payload)
    frame = parser.build_frame(page.records, product="PRODES", biome="Caatinga")
    assert pd.isna(frame.loc[0, "uf"]) and frame.loc[0, "estado_original"] == "unknown state"
    assert page.details["unknown_uf_count"] == 1 and page.warnings


def test_frame_rejects_related_but_different_layout_model():
    page = parser.parse_page(
        (GOLDEN / "prodes_pampa.json").read_bytes(), product="PRODES", biome="Pampa"
    )
    with pytest.raises(ParseError):
        parser.build_frame(page.records, product="PRODES", biome="Cerrado")


def test_tabular_signature_ignores_unrequested_geometry_and_bbox():
    payload = source_payload("prodes_pampa")
    first = parse_payload(payload, biome="Pampa")
    payload["features"][0]["geometry"] = None
    payload["features"][0]["bbox"] = None
    payload["timeStamp"] = "changed"
    second = parse_payload(payload, biome="Pampa")
    assert first.signatures == second.signatures


def test_conflicting_next_links_rejected():
    payload = source_payload()
    payload["links"] = [{"rel": "next", "href": "one"}, {"rel": "next", "href": "two"}]
    with pytest.raises(ParseError):
        parse_payload(payload)


def test_geometric_name_mismatch_rejected():
    payload = source_payload("prodes_pampa")
    payload["features"][0]["geometry_name"] = "not_geom"
    with pytest.raises(ParseError):
        parse_payload(payload, biome="Pampa")


def test_scene_extreme_exponent_error_is_parse_error():
    payload = source_payload()
    payload["features"][0]["properties"]["scene_id"] = "TOKEN"
    literal = "1e" + "9" * 5000
    with levanta_exatamente(ParseError):
        parser.parse_page(
            json.dumps(payload).replace('"TOKEN"', literal).encode(),
            product="PRODES",
            biome="Caatinga",
        )

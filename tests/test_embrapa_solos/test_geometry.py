from __future__ import annotations

import json
from pathlib import Path

import pytest

from agrobr.embrapa_solos import parser
from agrobr.exceptions import ParseError
from tests.helpers import embrapa_solos_features

GOLDEN = Path(__file__).parents[1] / "golden_data/embrapa_solos/official_20260907"


@pytest.fixture
def page():
    return {
        "type": "FeatureCollection",
        "features": embrapa_solos_features(include_geometry=True),
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
    }


@pytest.fixture
def parse():
    return lambda value, product="perfis", geo=True: parser.parse_page(
        json.dumps(value).encode(), product=product, include_geometry=geo
    )


@pytest.mark.parametrize(
    "crs",
    [
        None,
        {"type": "name", "properties": {"name": "EPSG:4674"}},
        {"type": "link", "properties": {"name": "EPSG:4326"}},
        {"type": "name", "properties": {"name": 4326}},
    ],
)
def test_nonempty_geo_rejects_unproven_crs(page, parse, crs):
    page["crs"] = crs
    with pytest.raises(ParseError):
        parse(page)


@pytest.mark.parametrize(
    "coordinates", [[], [1], [1, 2, 3, 4], [[1, 2]], [True, 2], ["1", 2], [1e309, 2], [None, 2]]
)
def test_point_structure_rejected(page, parse, coordinates):
    page["features"][0]["geometry"]["coordinates"] = coordinates
    with pytest.raises(ParseError):
        parse(page)


@pytest.mark.parametrize(
    "coordinates",
    [
        [],
        [[[[0, 0], [1, 0], [1, 1], [0, 1]]]],
        [[[[0, 0], [1, 0], [0, 0]]]],
        [[[[0, 0], [1, 0, 1], [1, 1], [0, 0]]]],
    ],
)
def test_multipolygon_structural_errors(parse, coordinates):
    page = {
        "type": "FeatureCollection",
        "features": embrapa_solos_features("mapa", include_geometry=True),
        "crs": {"type": "name", "properties": {"name": "EPSG:4326"}},
    }
    page["features"][0]["geometry"]["coordinates"] = coordinates
    with pytest.raises(ParseError):
        parse(page, "mapa")


def test_tabular_validates_and_releases_unrequested_geometry(page, parse):
    result = parse(page, geo=False)
    assert result.geometries is None
    assert result.records[0].geometry is None
    assert result.diagnostics["unexpected_geometry"]["count"] == 1
    page["features"][0]["geometry"] = None
    assert result.signatures == parse(page, geo=False).signatures


@pytest.mark.parametrize("bbox", [[2, 0, 1, 1], [0, 1, 2], [0, 0, True, 1]])
def test_invalid_envelope_bbox_rejected(page, parse, bbox):
    page["bbox"] = bbox
    with pytest.raises(ParseError):
        parse(page)

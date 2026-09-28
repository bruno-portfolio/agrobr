from __future__ import annotations

import pytest

from agrobr.incra import _geometry, _json
from agrobr.utils import spatial


def test_ring_closure_rejects_values_that_only_match_after_float_rounding():
    value = _json.decode(
        b'{"type":"Polygon","coordinates":[[[0.1,0],[1,0],[1,1],[0.10000000000000001,0]]]}'
    )
    assert float("0.1") == float("0.10000000000000001")
    with pytest.raises(ValueError, match="antes"):
        _geometry.validate(value)


@pytest.mark.parametrize(
    "value",
    [
        {},
        {"type": [], "coordinates": []},
        {"type": {}, "coordinates": []},
        {"type": "Point", "coordinates": [0, 0]},
        {"type": "GeometryCollection", "geometries": []},
        {"type": "Polygon"},
        {"type": "Polygon", "coordinates": None},
        {"type": "Polygon", "coordinates": [[]]},
        {"type": "MultiPolygon", "coordinates": [[]]},
        {"type": "Polygon", "coordinates": [], "crs": None},
        {"type": "Polygon", "coordinates": [[[0], [1], [2], [0]]]},
    ],
)
def test_unapproved_or_incomplete_geometry_fails(value):
    with pytest.raises(ValueError):
        _geometry.validate(value)


@pytest.mark.parametrize("value", [True, "0", None, _json.Number("1e309"), _json.Number("1e-999")])
def test_bad_coordinate_fails(value):
    ring = [[0, 0], [value, 0], [1, 1], [0, 0]]
    with pytest.raises(ValueError):
        _geometry.validate({"type": "Polygon", "coordinates": [ring]})


def test_mixed_dimensions_across_polygons_fail():
    ring = [[0, 0], [1, 0], [1, 1], [0, 0]]
    with pytest.raises(ValueError, match="Dimensões"):
        _geometry.validate(
            {"type": "MultiPolygon", "coordinates": [[ring], [[point + [0] for point in ring]]]}
        )


@pytest.mark.parametrize("bbox", [[0, 0, 1], [True, 0, 1, 1], [2, 0, 1, 1]])
def test_bad_published_bbox_fails(bbox):
    with pytest.raises(ValueError):
        _geometry.bbox_values(bbox)


def test_validated_polygon_uses_shared_intersection_without_repair():
    outer = [[0, 0], [5, 0], [5, 5], [0, 5], [0, 0]]
    hole = [[1, 1], [4, 1], [4, 4], [1, 4], [1, 1]]
    value = _geometry.validate({"type": "Polygon", "coordinates": [outer, hole]})
    assert value == {"type": "Polygon", "coordinates": [outer, hole]}
    assert not spatial.geometry_intersects_bbox(value, (2, 2, 3, 3))
    assert spatial.geometry_intersects_bbox(value, (0, 0, 1, 1))

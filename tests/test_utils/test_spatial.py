from __future__ import annotations

import math

import pytest

from agrobr.utils import spatial


@pytest.mark.parametrize(
    "a,b,c,d,expected",
    [
        ([0, 0], [1, 1], [1, 1], [2, 0], True),
        ([0, 0], [2, 0], [1, 0], [3, 0], True),
        ([0, 0], [1, 0], [2, 0], [3, 0], False),
        ([0, 0], [1, 0], [math.nextafter(1.0, math.inf), 0], [2, 0], False),
        ([0, 0], [2, 0], [1, 1], [1, 0], True),
        ([0, 0], [2, 0], [1, 1], [1, math.nextafter(0.0, 1.0)], False),
        ([0, 0], [2, 2], [0, 2], [2, 0], True),
        ([0, 0], [2, 2], [0, 1], [1, 2], False),
        ([1, 1], [1, 1], [0, 0], [2, 2], True),
        ([-0.0, 0], [0.0, 1], [-1, 0], [1, 0], True),
        ([0, 0, 20], [2, 2, 30], [0, 2, -20], [2, 0, -30], True),
    ],
)
def test_segment_envelope_preserves_exact_boundary_semantics(a, b, c, d, expected):
    assert spatial._intersects(a, b, c, d) is expected
    assert spatial._intersects(c, d, b, a) is expected


def test_disjoint_segment_envelopes_avoid_exact_arithmetic(monkeypatch):
    def unexpected(*_args):
        raise AssertionError("Disjoint envelopes do not require orientation")

    monkeypatch.setattr(spatial, "_orientation", unexpected)
    assert not spatial._intersects([0, 0], [1, 1], [2, 0], [3, 1])


@pytest.mark.parametrize(
    "bbox,expected",
    [
        ((2, 2, 3, 3), False),
        ((0.2, 0.2, 0.8, 0.8), True),
        ((9, 9, 11, 11), True),
        ((-1, -1, 11, 11), True),
        ((20, 20, 21, 21), False),
        ((1, 2, 1, 3), True),
        ((-1, 4, 11, 5), True),
    ],
)
def test_polygon_holes_edges_and_containment(bbox, expected):
    geometry = {
        "type": "Polygon",
        "coordinates": [
            [[0, 0], [10, 0], [10, 10], [0, 10], [0, 0]],
            [[1, 1], [1, 9], [9, 9], [9, 1], [1, 1]],
        ],
    }
    assert spatial.geometry_intersects_bbox(geometry, bbox) is expected


@pytest.mark.parametrize("kind", ["Point", "Polygon", "MultiPolygon"])
def test_empty_geometry_has_no_intersection(kind):
    assert not spatial.geometry_intersects_bbox({"type": kind, "coordinates": []}, (0, 0, 1, 1))


def test_unsupported_geometry_is_not_treated_as_a_surface():
    with pytest.raises(ValueError, match="Point, Polygon or MultiPolygon"):
        spatial.geometry_intersects_bbox(
            {"type": "LineString", "coordinates": [[0, 0], [1, 1]]}, (0, 0, 1, 1)
        )

from __future__ import annotations

from fractions import Fraction
from typing import Any


def _orientation(a: list[float], b: list[float], c: list[float]) -> Fraction:
    ax, ay, bx, by, cx, cy = [Fraction(value) for value in (*a[:2], *b[:2], *c[:2])]
    return (bx - ax) * (cy - ay) - (by - ay) * (cx - ax)


def _on_segment(point: list[float], a: list[float], b: list[float]) -> bool:
    return (
        min(a[0], b[0]) <= point[0] <= max(a[0], b[0])
        and min(a[1], b[1]) <= point[1] <= max(a[1], b[1])
        and _orientation(a, b, point) == 0
    )


def _intersects(a: list[float], b: list[float], c: list[float], d: list[float]) -> bool:
    if (
        max(a[0], b[0]) < min(c[0], d[0])
        or max(c[0], d[0]) < min(a[0], b[0])
        or max(a[1], b[1]) < min(c[1], d[1])
        or max(c[1], d[1]) < min(a[1], b[1])
    ):
        return False
    oa, ob, oc, od = (
        _orientation(a, b, c),
        _orientation(a, b, d),
        _orientation(c, d, a),
        _orientation(c, d, b),
    )
    if any(
        (value == 0 and _on_segment(point, start, end))
        for value, point, start, end in ((oa, c, a, b), (ob, d, a, b), (oc, a, c, d), (od, b, c, d))
    ):
        return True
    return (oa > 0) != (ob > 0) and (oc > 0) != (od > 0)


def _inside(point: list[float], ring: list[list[float]]) -> bool:
    inside = False
    for a, b in zip(ring, ring[1:]):
        if _on_segment(point, a, b):
            return True
        if (a[1] > point[1]) != (b[1] > point[1]):
            cross = Fraction(a[0]) + (Fraction(point[1]) - Fraction(a[1])) * (
                Fraction(b[0]) - Fraction(a[0])
            ) / (Fraction(b[1]) - Fraction(a[1]))
            if Fraction(point[0]) < cross:
                inside = not inside
    return inside


def geometry_intersects_bbox(
    geometry: dict[str, Any] | None, bbox: tuple[float, float, float, float]
) -> bool:
    """Test XY intersection using previously validated geometry and bounds."""
    if geometry is None:
        return False
    left, bottom, right, top = bbox
    if geometry["type"] == "Point":
        if not geometry["coordinates"]:
            return False
        x, y = geometry["coordinates"][:2]
        return bool(left <= x <= right and bottom <= y <= top)
    if geometry["type"] == "Polygon":
        polygons = [geometry["coordinates"]] if geometry["coordinates"] else []
    elif geometry["type"] == "MultiPolygon":
        polygons = geometry["coordinates"]
    else:
        raise ValueError("Intersects requires Point, Polygon or MultiPolygon geometry")
    corners = [[left, bottom], [right, bottom], [right, top], [left, top], [left, bottom]]
    edges = list(zip(corners, corners[1:]))
    for polygon in polygons:
        for ring in polygon:
            if any(left <= point[0] <= right and bottom <= point[1] <= top for point in ring):
                return True
            if any(_intersects(a, b, c, d) for a, b in zip(ring, ring[1:]) for c, d in edges):
                return True
        if any(
            _inside(point, polygon[0]) and not any(_inside(point, hole) for hole in polygon[1:])
            for point in corners[:-1]
        ):
            return True
    return False

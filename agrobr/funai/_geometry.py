from __future__ import annotations

from typing import Any

from . import _json


def bbox_values(value: Any) -> list[float] | None:
    if value is None:
        return None
    if not isinstance(value, list) or len(value) not in (4, 6):
        raise ValueError("BBox publicado inválido")
    values = [_json.floating(item) for item in value]
    dimensions = len(values) // 2
    if any(values[index] > values[index + dimensions] for index in range(dimensions)):
        raise ValueError("BBox publicado invertido")
    return values


def ring(value: Any) -> tuple[list[list[float]], int]:
    if not isinstance(value, list) or len(value) < 4:
        raise ValueError("Anel incompleto")
    positions = []
    dimensions: set[int] = set()
    for point in value:
        if not isinstance(point, list) or len(point) not in (2, 3):
            raise ValueError("Posição exige duas ou três coordenadas")
        positions.append([_json.floating(item) for item in point])
        dimensions.add(len(point))
    if len(dimensions) != 1:
        raise ValueError("Dimensão inconsistente no anel")
    if [_json.decimal(item) for item in value[0]] != [_json.decimal(item) for item in value[-1]]:
        raise ValueError("Anel aberto antes da conversão float64")
    return positions, dimensions.pop()


def validate(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict) or set(value) - {"type", "coordinates", "bbox"}:
        raise ValueError("Geometria inválida ou membro desconhecido")
    if value.get("type") not in {"Polygon", "MultiPolygon"}:
        raise ValueError("Geometria FUNAI exige Polygon ou MultiPolygon")
    raw = value.get("coordinates")
    if not isinstance(raw, list):
        raise ValueError("Coordenadas ausentes")
    polygons = [raw] if value["type"] == "Polygon" and raw else raw
    converted = []
    dimensions: set[int] = set()
    for polygon in polygons:
        if not isinstance(polygon, list) or not polygon:
            raise ValueError("Polígono interno sem anéis")
        rings = []
        for item in polygon:
            positions, dimension = ring(item)
            rings.append(positions)
            dimensions.add(dimension)
        converted.append(rings)
    if len(dimensions) > 1:
        raise ValueError("Dimensões inconsistentes na geometria")
    result: dict[str, Any] = {
        "type": value["type"],
        "coordinates": converted[0] if value["type"] == "Polygon" and converted else converted,
    }
    if "bbox" in value:
        result["bbox"] = bbox_values(value["bbox"])
    return result

from __future__ import annotations

from typing import Any

from . import _json


def bbox_values(value: Any) -> list[float] | None:
    if value is None:
        return None
    if not isinstance(value, list) or len(value) not in (4, 6):
        raise ValueError("BBox publicado inválido")
    result = [_json.floating(item) for item in value]
    dimensions = len(result) // 2
    if any(result[index] > result[index + dimensions] for index in range(dimensions)):
        raise ValueError("BBox publicado invertido")
    return result


def coordinates(value: Any, depth: int) -> tuple[list[Any], int]:
    if not isinstance(value, list) or not value:
        raise ValueError("Coordenadas geométricas ausentes")
    if depth == 0:
        if len(value) not in (2, 3):
            raise ValueError("Posição geométrica deve ser 2D ou 3D")
        return [_json.floating(item) for item in value], len(value)
    converted = [coordinates(item, depth - 1) for item in value]
    dimensions = {dimension for _, dimension in converted}
    if len(dimensions) != 1:
        raise ValueError("Dimensões geométricas inconsistentes")
    if depth == 1 and (
        len(value) < 4
        or [_json.decimal(item) for item in value[0]] != [_json.decimal(item) for item in value[-1]]
    ):
        raise ValueError("Anel geométrico aberto ou incompleto")
    return [item for item, _ in converted], dimensions.pop()


def validate(value: Any, product: str) -> dict[str, Any] | None:
    if value is None:
        return None
    wanted = "Point" if product == "perfis" else "MultiPolygon"
    if not isinstance(value, dict) or value.get("type") != wanted:
        raise ValueError("Geometria incompatível com layout")
    if set(value) - {"type", "coordinates", "bbox"}:
        raise ValueError("Membro geométrico desconhecido")
    converted, _ = coordinates(value.get("coordinates"), 0 if wanted == "Point" else 3)
    result: dict[str, Any] = {"type": wanted, "coordinates": converted}
    if "bbox" in value:
        result["bbox"] = bbox_values(value["bbox"])
    return result

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data/ana/oficial_20260923"

SCHEMA: dict[str, list[tuple[str, str, bool]]] = {
    "hidrografia": [
        ("OBJECTID", "OBJECTID", False),
        ("codigo_curso", "COCURSODAG", False),
        ("codigo_bacia", "COBACIA", False),
        ("nome_rio", "NORIOCOMP", False),
        ("dominio", "DEDOMINIAL", False),
    ],
    "pivos_irrigacao": [
        ("OBJECTID", "OBJECTID", False),
        ("codigo_municipio", "CD_GEOCMU", False),
        ("municipio", "NM_MUNICIP", False),
        ("estado", "NM_ESTADO", False),
        ("regiao_hidro", "REGIAO_HID", False),
        ("area_ha", "HECTARES", True),
    ],
    "demanda_irrigacao": [
        ("OBJECTID", "OBJECTID", False),
        ("ID", "ID", False),
        ("codigo_bacia", "COBACIA", False),
        ("versao", "DSVERSAO", False),
        ("vazao_max_mensal", "VZMAXMEN", True),
        ("vazao_mes_seco", "VZMESSEC", True),
        ("vazao_mes_irrigacao", "VZMESIRR", True),
        ("vazao_media_anual", "VZMEDANO", True),
    ],
    "disponibilidade_hidrica": [
        ("OBJECTID", "OBJECTID", False),
        ("ID", "ID", False),
        ("area_montante_km2", "NUAREAMONT", True),
        ("disponibilidade_m3_s", "DISPQ95", True),
        ("nome_rio", "NMRIO", False),
        ("dominio", "DEDOMINIAL", False),
        ("versao", "DSVERSAO", False),
    ],
}


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_text(encoding="utf-8"))
    for entry in data["requests"]:
        body = (GOLDEN / entry["file"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == entry["sha256"], entry["file"]
    return data


def body_for(data: dict[str, Any], path: str, query: bytes) -> bytes | None:
    params = dict(parse_qsl(query.decode("ascii")))
    for entry in data["requests"]:
        if path.endswith(entry["path"] + "/query") and entry["params"] == params:
            return (GOLDEN / entry["file"]).read_bytes()
    return None


def official_ids(case: str) -> list[int]:
    return sorted(json.loads((GOLDEN / case / "ids.json").read_bytes())["objectIds"] or [])


def features(
    data: dict[str, Any], case: str, fmt: str, prefix: str = "chave_"
) -> list[dict[str, Any]]:
    found: dict[int, dict[str, Any]] = {}
    names = sorted(
        {
            entry["file"]
            for entry in data["requests"]
            if entry["case"] == case
            and entry["params"]["f"] == fmt
            and "orderByFields" in entry["params"]
            and entry["file"].split("/")[-1].startswith(prefix)
        }
    )
    for name in names:
        for feature in json.loads((GOLDEN / name).read_bytes())["features"]:
            attributes = feature.get("attributes") or feature.get("properties")
            found[attributes["OBJECTID"]] = feature
    return [found[oid] for oid in sorted(found)]


def row(layer: str, feature: dict[str, Any]) -> dict[str, Any]:
    attributes = feature.get("attributes") or feature.get("properties") or {}
    cells = {}
    for column, field, numeric in SCHEMA[layer]:
        value = attributes.get(field)
        cells[column] = None if value is None else float(value) if numeric else value
    return cells


def published(frame: Any) -> list[dict[str, Any]]:
    rows = []
    for record in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        cells = {}
        for column, value in record.items():
            if pd.isna(value):
                cells[column] = None
            else:
                cells[column] = value.item() if hasattr(value, "item") else value
        rows.append(cells)
    return rows


def coordinates(geometry: Any) -> Any:
    if geometry is None:
        return None
    mapping = geometry.__geo_interface__
    return json.loads(json.dumps(mapping["coordinates"]))


def source_coordinates(feature: dict[str, Any]) -> Any:
    geometry = feature.get("geometry")
    return None if geometry is None else geometry["coordinates"]

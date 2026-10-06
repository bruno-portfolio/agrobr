from __future__ import annotations

import argparse
import asyncio
import json
import warnings
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd

from agrobr import exceptions
from agrobr.http import responses

BASE = "https://portal1.snirh.gov.br/server/rest/services/dados_abertos"
PATHS = {
    "hidrografia": "Hidrografia/MapServer/0",
    "pivos_irrigacao": "Pivos_Mapeados/MapServer/0",
    "demanda_irrigacao": "Demanda_de_Irrigacao_Vazao_de_Retirada_para_Irrigacao/MapServer/0",
    "disponibilidade_hidrica": "Disponibilidade_Hidrica_Superficial/MapServer/0",
}
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
ESTADOS = {"PI": "PIAUÍ", "GO": "GOIÁS", "DF": "DISTRITO FEDERAL"}
CASES: list[tuple[str, str, dict[str, Any]]] = [
    ("hidrografia_paginas", "hidrografia", {"bbox": (-48.8, -16.8, -46.8, -14.8)}),
    ("hidrografia_df", "hidrografia", {"bbox": (-48.1, -16.1, -47.9, -15.9)}),
    ("demanda_df", "demanda_irrigacao", {"bbox": (-48.1, -16.1, -47.9, -15.9)}),
    ("pivos_piaui", "pivos_irrigacao", {"uf": "PI"}),
    ("pivos_goias", "pivos_irrigacao", {"uf": "GO"}),
    ("disponibilidade_brasilia", "disponibilidade_hidrica", {"bbox": (-47.5, -16.0, -47.0, -15.5)}),
    ("hidrografia_oceano", "hidrografia", {"bbox": (-30.0, -20.0, -29.9, -19.9)}),
]
BATCH = 200


def spatial(options: dict[str, Any]) -> dict[str, str]:
    bbox = options.get("bbox")
    if bbox is None:
        return {}
    return {
        "geometry": ",".join(str(value) for value in bbox),
        "geometryType": "esriGeometryEnvelope",
        "inSR": "4326",
        "spatialRel": "esriSpatialRelIntersects",
    }


def where(options: dict[str, Any]) -> str:
    uf = options.get("uf")
    return "1=1" if uf is None else f"NM_ESTADO='{ESTADOS[uf]}'"


def _official_json(response: httpx.Response) -> dict[str, Any]:
    responses.raise_for_status(response, source="ana")
    url = str(response.url)
    content_type = response.headers.get("content-type", "").partition(";")[0].strip().lower()
    if content_type not in {"application/json", "application/geo+json", "text/plain"}:
        raise exceptions.SourceUnavailableError(
            source="ana", url=url, last_error=f"Tipo de resposta inesperado: {content_type!r}"
        )
    payload = responses.parse_json_response(response, source="ana", url=url)
    if not isinstance(payload, dict):
        raise exceptions.SourceUnavailableError(
            source="ana", url=url, last_error="Resposta oficial não é um objeto JSON"
        )
    error = responses.arcgis_error_message(payload)
    if error:
        raise exceptions.SourceUnavailableError(source="ana", url=url, last_error=error)
    return payload


def official(
    client: httpx.Client, layer: str, options: dict[str, Any], fmt: str
) -> tuple[list[int], dict[int, dict[str, Any]]]:
    url = f"{BASE}/{PATHS[layer]}/query"
    ids_response = client.get(
        url,
        params={"where": where(options), "f": "json", "returnIdsOnly": "true", **spatial(options)},
    )
    ids: list[int] = sorted(_official_json(ids_response).get("objectIds") or [])
    features: dict[int, dict[str, Any]] = {}
    for start in range(0, len(ids), BATCH):
        chunk = ids[start : start + BATCH]
        params = {
            "objectIds": ",".join(str(value) for value in chunk),
            "outFields": "*",
            "outSR": "4326",
            "f": fmt,
            "returnGeometry": "true" if fmt == "geojson" else "false",
        }
        payload = _official_json(client.get(url, params=params))
        for feature in payload.get("features", []):
            attributes = feature.get("attributes") or feature.get("properties") or {}
            features[int(attributes["OBJECTID"])] = feature
    return ids, features


def expected_row(layer: str, feature: dict[str, Any]) -> dict[str, Any]:
    attributes = feature.get("attributes") or feature.get("properties") or {}
    row: dict[str, Any] = {}
    for column, field, numeric in SCHEMA[layer]:
        value = attributes.get(field)
        row[column] = None if value is None else float(value) if numeric else value
    return row


def published(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for record in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        row: dict[str, Any] = {}
        for column, value in record.items():
            if pd.isna(value):
                row[str(column)] = None
            else:
                row[str(column)] = value.item() if hasattr(value, "item") else value
        rows.append(row)
    return rows


def coordinates(geometry: Any) -> Any:
    if geometry is None:
        return None
    return json.loads(json.dumps(geometry.__geo_interface__["coordinates"]))


async def agrobr_outputs(layer: str, options: dict[str, Any]) -> tuple[Any, Any, list[str]]:
    from agrobr import ana

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame = await getattr(ana, layer)(**options)
        geo = await getattr(ana, f"{layer}_geo")(**options)
    return frame, geo, [str(item.message) for item in caught]


def compare(client: httpx.Client, layer: str, options: dict[str, Any]) -> dict[str, Any]:
    frame, geo, messages = asyncio.run(agrobr_outputs(layer, options))
    ids, table = official(client, layer, options, "json")
    _, shapes = official(client, layer, options, "geojson")
    problems: list[str] = []
    if sorted(table) != ids:
        problems.append(
            f"lotes por objectIds não cobriram os IDs oficiais: {len(table)} de {len(ids)}"
        )
    expected = [expected_row(layer, table[oid]) for oid in ids if oid in table]
    observed = sorted(published(frame), key=lambda row: row["OBJECTID"])
    if len(observed) != len(ids):
        problems.append(f"linhas: agrobr {len(observed)} × oficiais {len(ids)}")
    if observed != expected:
        differing = sum(1 for want, got in zip(expected, observed, strict=False) if want != got)
        problems.append(f"tabular: {differing} linhas divergentes")
    geo_rows = sorted(
        zip(published(geo), geo.geometry, strict=True), key=lambda item: item[0]["OBJECTID"]
    )
    if [row for row, _ in geo_rows] != [
        expected_row(layer, shapes[oid]) for oid in ids if oid in shapes
    ]:
        problems.append("geo: atributos divergentes")
    wrong = sum(
        1
        for (row, geometry) in geo_rows
        if row["OBJECTID"] not in shapes
        or coordinates(geometry)
        != (shapes[row["OBJECTID"]].get("geometry") or {}).get("coordinates")
    )
    if wrong or len(geo_rows) != len(ids):
        problems.append(
            f"geo: {wrong} geometrias divergentes; {len(geo_rows)} de {len(ids)} feições"
        )
    if geo.crs is None or geo.crs.to_epsg() != 4326:
        problems.append(f"CRS: {geo.crs}")
    return {
        "status": "ok" if not problems else "mismatch",
        "problems": problems,
        "ids_oficiais": len(ids),
        "celulas": len(expected) * len(SCHEMA[layer]),
        "avisos_agrobr": messages,
    }


def run(output: Path) -> int:
    checks: dict[str, Any] = {}
    headers = {
        "User-Agent": "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
    }
    with httpx.Client(headers=headers, timeout=httpx.Timeout(60, read=300)) as client:
        for name, layer, options in CASES:
            checks[name] = compare(client, layer, options)
            print(name, checks[name]["status"], checks[name]["problems"][:3], flush=True)
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "IDs oficiais (returnIdsOnly) e feições por lotes de objectIds, lidos com json × saída pública do agrobr",
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    failed = sum(check["status"] != "ok" for check in checks.values())
    print(f"{len(checks) - failed} ok / {failed} mismatch")
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo as camadas ArcGIS da ANA contra leitura independente por objectIds"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())

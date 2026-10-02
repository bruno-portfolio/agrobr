from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import pandas as pd

GOLDEN = Path(__file__).parents[1] / "golden_data/ana/oficial_20260923"
MASSAS = Path(__file__).parents[1] / "golden_data/ana/massas_dagua_20261001"

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


def manifest(golden: Path = GOLDEN) -> dict[str, Any]:
    data: dict[str, Any] = json.loads((golden / "manifest.json").read_text(encoding="utf-8"))
    for entry in data["requests"]:
        body = (golden / entry["file"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == entry["sha256"], entry["file"]
    return data


def body_for(data: dict[str, Any], path: str, query: bytes, golden: Path = GOLDEN) -> bytes | None:
    params = dict(parse_qsl(query.decode("ascii")))
    for entry in data["requests"]:
        if path.endswith(entry["path"] + "/query") and entry["params"] == params:
            return (golden / entry["file"]).read_bytes()
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


UF_POR_NOME = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPÁ": "AP", "AMAZONAS": "AM", "BAHIA": "BA", "CEARÁ": "CE",
    "DISTRITO FEDERAL": "DF", "ESPÍRITO SANTO": "ES", "GOIÁS": "GO", "MARANHÃO": "MA",
    "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG", "PARÁ": "PA",
    "PARAÍBA": "PB", "PARANÁ": "PR", "PERNAMBUCO": "PE", "PIAUÍ": "PI", "RIO DE JANEIRO": "RJ",
    "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS", "RONDÔNIA": "RO", "RORAIMA": "RR",
    "SANTA CATARINA": "SC", "SÃO PAULO": "SP", "SERGIPE": "SE", "TOCANTINS": "TO",
}  # fmt: skip
MASSAS_SCHEMA: list[tuple[str, str, str]] = [
    ("codigo", "esp_cd", "inteiro"),
    ("nome", "nmoriginal", "texto"),
    ("nome_alternativo", "nmalternat", "texto"),
    ("tipo", "detipomass", "texto"),
    ("dominio", "dedominial", "texto"),
    ("entidade_fiscalizadora", "defiscaliz", "texto"),
    ("uso_principal", "usoprinc", "texto"),
    ("volume_hm3", "nuvolumhm3", "medida"),
    ("area_km2", "nuareakm2", "medida"),
    ("area_ha", "nuareaha", "medida"),
    ("perimetro_km", "nuperimkm", "medida"),
    ("data_construcao", "dtreserv", "data"),
    ("nome_rio", "nmriocomp", "texto"),
    ("codigo_snisb", "cod_snisb", "inteiro"),
    ("codigo_trecho", "cotrecho", "inteiro"),
    ("uf", "nmufe", "uf"),
    ("municipios", "nmmun", "texto"),
    ("fonte_geometria", "deversao", "texto"),
]


def massa(feature: dict[str, Any]) -> dict[str, Any]:
    attributes = feature.get("attributes") or feature.get("properties") or {}
    cells: dict[str, Any] = {}
    for column, field, kind in MASSAS_SCHEMA:
        value = attributes.get(field)
        if isinstance(value, str) and not value.strip():
            value = None
        if value is None:
            cells[column] = None
        elif kind == "inteiro":
            cells[column] = int(value)
        elif kind == "medida":
            cells[column] = float(value)
        elif kind == "data":
            day, month, year = (int(piece) for piece in value.split("/"))
            cells[column] = pd.Timestamp(year, month, day)
        elif kind == "uf":
            cells[column] = "/".join(sorted(UF_POR_NOME[nome.strip()] for nome in value.split(",")))
        else:
            cells[column] = value
    return cells


def massas_features(data: dict[str, Any], case: str, fmt: str) -> list[dict[str, Any]]:
    found: dict[int, dict[str, Any]] = {}
    for entry in data["requests"]:
        if entry["case"] == case and entry["file"].split("/")[-1] == f"faixa_0.{fmt}":
            for feature in json.loads((MASSAS / entry["file"]).read_bytes())["features"]:
                attributes = feature.get("attributes") or feature.get("properties")
                found[attributes["FID"]] = feature
    return [found[fid] for fid in sorted(found)]


def massas_ids(case: str) -> list[int]:
    return sorted(json.loads((MASSAS / case / "ids.json").read_bytes()).get("objectIds") or [])

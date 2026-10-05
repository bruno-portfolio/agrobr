from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import pandas as pd
import pytest

GOLDEN = Path(__file__).parents[1] / "golden_data" / "funai" / "oficial_20260923"
PRINCIPAIS = [
    ("codigo", "terrai_codigo"),
    ("nome", "terrai_nome"),
    ("etnia", "etnia_nome"),
    ("municipio", "municipio_nome"),
    ("uf", "uf_sigla"),
    ("area_ha", "superficie_perimetro_ha"),
    ("fase", "fase_ti"),
    ("modalidade", "modalidade_ti"),
    ("data_atualizacao", "data_atualizacao"),
]


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    for entry in [*data["requests"], *data["schema"]]:
        body = (GOLDEN / entry["file"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == entry["sha256"], entry["file"]
    return data


def entrada(data: dict[str, Any], query: str) -> dict[str, Any] | None:
    params = dict(parse_qsl(query, keep_blank_values=True))
    return next((entry for entry in data["requests"] if entry["params"] == params), None)


def atributos() -> list[str]:
    xsd = (GOLDEN / "schema" / "describe_tis_poligonais.xsd").read_text(encoding="utf-8")
    return [
        nome
        for nome, tipo in re.findall(r'<xsd:element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', xsd)
        if not tipo.startswith("gml:") and nome != "tis_poligonais"
    ]


def colunas() -> list[tuple[str, str | None]]:
    usados = {fonte for _, fonte in PRINCIPAIS}
    resto: list[tuple[str, str | None]] = [
        (nome, nome) for nome in atributos() if nome not in usados
    ]
    return [*PRINCIPAIS, ("feature_id", None), *resto]


def feicoes(data: dict[str, Any], caso: str) -> list[dict[str, Any]]:
    paginas = sorted(
        (e for e in data["requests"] if e["case"] == caso and e["role"] == "page"),
        key=lambda entry: int(entry["params"]["startIndex"]),
    )
    aceitas: list[dict[str, Any]] = []
    for indice, entry in enumerate(paginas):
        features = json.loads((GOLDEN / entry["file"]).read_bytes())["features"]
        if indice:
            assert features[0]["properties"]["gid"] == aceitas[-1]["properties"]["gid"]
            features = features[1:]
        aceitas.extend(features)
    return aceitas


def linha(feature: dict[str, Any]) -> dict[str, Any]:
    propriedades = feature["properties"]
    saida = {
        coluna: feature["id"] if fonte is None else propriedades[fonte]
        for coluna, fonte in colunas()
    }
    data = saida["data_atualizacao"]
    if data is not None:
        dia, mes, ano = (int(parte) for parte in data.split("/"))
        saida["data_atualizacao"] = pd.Timestamp(ano, mes, dia)
    return saida


def ufs(texto: str | None) -> set[str]:
    return {parte.strip().upper() for parte in texto.split(",")} if texto else set()


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        linhas.append(
            {
                str(coluna): None
                if pd.isna(valor)
                else valor.item()
                if hasattr(valor, "item")
                else valor
                for coluna, valor in registro.items()
            }
        )
    return linhas


def intersecta(geometria: dict[str, Any], bbox: tuple[float, float, float, float]) -> bool:
    geometria_shapely = pytest.importorskip("shapely.geometry")
    return bool(geometria_shapely.shape(geometria).intersects(geometria_shapely.box(*bbox)))


def coordenadas(geometria: Any) -> Any:
    return json.loads(json.dumps(geometria.__geo_interface__["coordinates"]))

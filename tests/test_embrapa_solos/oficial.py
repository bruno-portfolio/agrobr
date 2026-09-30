from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl

import pandas as pd
from shapely.geometry import box, shape

GOLDEN = Path(__file__).parents[1] / "golden_data" / "embrapa_solos" / "oficial_20260923"
UFS = frozenset(
    [
        "AC",
        "AL",
        "AM",
        "AP",
        "BA",
        "CE",
        "DF",
        "ES",
        "GO",
        "MA",
        "MG",
        "MS",
        "MT",
        "PA",
        "PB",
        "PE",
        "PI",
        "PR",
        "RJ",
        "RN",
        "RO",
        "RR",
        "RS",
        "SC",
        "SE",
        "SP",
        "TO",
    ]
)
CAMADAS = {"perfis": "perfis_pronasolos_2020", "mapa": "brasil_solos_5m_20201104"}
DUPLA_CODIFICACAO = re.compile("[ÃÂ][\x80-\xbf]")
PRINCIPAIS = {
    "perfis": [
        ("fid", "fid"),
        ("uf", "uf"),
        ("municipio", "municipio"),
        ("latitude", "gcs_latitu"),
        ("longitude", "gcs_longit"),
        ("horizonte", "simbolo_ho"),
        ("profundidade", "profundida"),
        ("areia_total", "areia_tota"),
        ("silte", "silte"),
        ("argila", "argila"),
        ("ph_h2o", "ph_h2o"),
        ("carbono_organico", "carbono_or"),
        ("ctc", "valor_t"),
        ("saturacao_bases", "valor_v"),
        ("aluminio", "aluminio_t"),
        ("fosforo", "fosforo_as"),
        ("classe_textural", "classe_tex"),
        ("nivel_levantamento", "nivel_leva"),
        ("uso_atual", "uso_atual"),
    ],
    "mapa": [
        ("fid", "ogc_fid"),
        ("simbolos", "simbolos"),
        ("comp1", "comp1"),
        ("comp2", "comp2"),
        ("comp3", "comp3"),
        ("legenda", "leg_desc"),
        ("area_km2", "area_km2"),
        ("ordem1", "ordem1"),
        ("subordem1", "subordem1"),
        ("gdegrupo1", "gdegrupo1"),
        ("ordem2", "ordem2"),
        ("subordem2", "subordem2"),
        ("gdegrupo2", "gdegrupo2"),
        ("legenda_sinotica", "leg_sinot"),
        ("classe_dom", "classe_dom"),
    ],
}
MEDIDAS = (
    "areia_total",
    "silte",
    "argila",
    "ph_h2o",
    "carbono_organico",
    "ctc",
    "saturacao_bases",
    "aluminio",
    "fosforo",
)


def manifest() -> dict[str, Any]:
    data: dict[str, Any] = json.loads((GOLDEN / "manifest.json").read_bytes())
    for entry in [*data["requests"], *data["schema"]]:
        body = (GOLDEN / entry["file"]).read_bytes()
        assert hashlib.sha256(body).hexdigest() == entry["sha256"], entry["file"]
    return data


def entrada(data: dict[str, Any], query: str) -> dict[str, Any] | None:
    params = dict(parse_qsl(query, keep_blank_values=True))
    return next((entry for entry in data["requests"] if entry["params"] == params), None)


def atributos(produto: str) -> list[str]:
    xsd = (GOLDEN / "schema" / f"describe_{CAMADAS[produto]}.xsd").read_text(encoding="utf-8")
    return [
        nome
        for nome, tipo in re.findall(r'<xsd:element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', xsd)
        if not tipo.startswith("gml:") and nome != CAMADAS[produto]
    ]


def colunas(produto: str) -> list[tuple[str, str | None]]:
    principais: list[tuple[str, str | None]] = list(PRINCIPAIS[produto])
    usados = {fonte for _, fonte in principais}
    resto: list[tuple[str, str | None]] = [
        (nome, nome) for nome in atributos(produto) if nome not in usados
    ]
    final: list[tuple[str, str | None]] = [("feature_id", None)]
    if produto == "perfis":
        final.insert(0, ("uf_original", "uf"))
    return [*principais, *resto, *final]


def feicoes(data: dict[str, Any], caso: str) -> list[dict[str, Any]]:
    paginas = sorted(
        (e for e in data["requests"] if e["case"] == caso and e["role"] == "page"),
        key=lambda entry: int(entry["params"]["startIndex"]),
    )
    aceitas: list[dict[str, Any]] = []
    for indice, entry in enumerate(paginas):
        features = json.loads((GOLDEN / entry["file"]).read_bytes())["features"]
        if indice:
            assert features[0]["id"] == aceitas[-1]["id"], entry["file"]
            features = features[1:]
        aceitas.extend(features)
    return aceitas


def linha(produto: str, feature: dict[str, Any]) -> dict[str, Any]:
    propriedades = feature["properties"]
    saida: dict[str, Any] = {}
    for coluna, fonte in colunas(produto):
        if fonte is None:
            saida[coluna] = feature["id"]
        elif produto == "perfis" and coluna == "uf":
            bruto = propriedades["uf"]
            normal = bruto.strip().upper() if isinstance(bruto, str) else None
            saida[coluna] = normal if normal in UFS else None
        elif produto == "perfis" and coluna in {"ano", "data_colet"}:
            bruto = propriedades[fonte]
            saida[coluna] = (
                None
                if bruto is None or bruto == "NULL"
                else int(bruto)
                if coluna == "ano"
                else datetime.strptime(bruto, "%Y-%m-%d")
            )
        else:
            saida[coluna] = texto(propriedades[fonte])
    return saida


def texto(valor: Any) -> Any:
    if not isinstance(valor, str) or not DUPLA_CODIFICACAO.search(valor):
        return valor
    try:
        return valor.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return valor


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
    return bool(shape(geometria).intersects(box(*bbox)))


def coordenadas(geometria: Any) -> Any:
    return json.loads(json.dumps(geometria.__geo_interface__["coordinates"]))

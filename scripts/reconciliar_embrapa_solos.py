from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import math
import re
import warnings
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
from shapely import wkt
from shapely.geometry import box

URL = "https://geoinfo.dados.embrapa.br/geoserver/ows"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
CAMADAS = {"perfis": "perfis_pronasolos_2020", "mapa": "brasil_solos_5m_20201104"}
CHAVES = {"perfis": "fid", "mapa": "ogc_fid"}
RENOMES = {
    "perfis": {
        "gcs_latitu": "latitude",
        "gcs_longit": "longitude",
        "simbolo_ho": "horizonte",
        "profundida": "profundidade",
        "areia_tota": "areia_total",
        "carbono_or": "carbono_organico",
        "valor_t": "ctc",
        "valor_v": "saturacao_bases",
        "aluminio_t": "aluminio",
        "fosforo_as": "fosforo",
        "classe_tex": "classe_textural",
        "nivel_leva": "nivel_levantamento",
    },
    "mapa": {"ogc_fid": "fid", "leg_desc": "legenda", "leg_sinot": "legenda_sinotica"},
}
PRINCIPAIS = {
    "perfis": [
        "fid",
        "uf",
        "municipio",
        "gcs_latitu",
        "gcs_longit",
        "simbolo_ho",
        "profundida",
        "areia_tota",
        "silte",
        "argila",
        "ph_h2o",
        "carbono_or",
        "valor_t",
        "valor_v",
        "aluminio_t",
        "fosforo_as",
        "classe_tex",
        "nivel_leva",
        "uso_atual",
    ],
    "mapa": [
        "ogc_fid",
        "simbolos",
        "comp1",
        "comp2",
        "comp3",
        "leg_desc",
        "area_km2",
        "ordem1",
        "subordem1",
        "gdegrupo1",
        "ordem2",
        "subordem2",
        "gdegrupo2",
        "leg_sinot",
        "classe_dom",
    ],
}
INTEIROS = {"fid", "ogc_fid", "codigo_pon"}
REAIS = {"gcs_latitu", "gcs_longit", "area_km2"}
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
PAGINA = 5000
TOLERANCIA = 1e-8
Caixa = tuple[float, float, float, float]
CASOS: list[tuple[str, str, dict[str, Any]]] = [
    ("perfis_prefixo_500", "perfis", {"max_registros": 500}),
    ("perfis_uf_df", "perfis", {"uf": "DF", "max_registros": None, "tamanho_pagina": 1000}),
    ("perfis_bbox_brasilia", "perfis", {"bbox": (-47.95, -15.85, -47.75, -15.65)}),
    ("mapa_completo", "mapa", {"max_registros": None, "tamanho_pagina": 1000}),
    ("mapa_ordem_latossolo", "mapa", {"ordem": "latossolo", "max_registros": None}),
    ("mapa_bbox_florianopolis", "mapa", {"bbox": (-48.52, -27.62, -48.5, -27.6)}),
]


def atributos(cliente: httpx.Client, produto: str) -> tuple[list[str], str]:
    resposta = cliente.get(
        URL,
        params={
            "service": "WFS",
            "version": "2.0.0",
            "request": "DescribeFeatureType",
            "typeNames": f"geonode:{CAMADAS[produto]}",
        },
    )
    resposta.raise_for_status()
    elementos = re.findall(r'<xsd:element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', resposta.text)
    nomes = [n for n, t in elementos if not t.startswith("gml:") and n != CAMADAS[produto]]
    geometria = next(n for n, t in elementos if t.startswith("gml:"))
    return nomes, geometria


def colunas(produto: str, nomes: list[str]) -> list[str]:
    principais = PRINCIPAIS[produto]
    renomes = RENOMES[produto]
    saida = [renomes.get(nome, nome) for nome in principais]
    saida += [nome for nome in nomes if nome not in principais]
    if produto == "perfis":
        saida.append("uf_original")
    return [*saida, "feature_id"]


def linhas_oficiais(
    cliente: httpx.Client, produto: str, nomes: list[str], geometria: str, opcoes: dict[str, Any]
) -> tuple[list[dict[str, str]], int]:
    bbox: Caixa | None = opcoes.get("bbox")
    limite = opcoes.get(
        "max_registros", 50000 if bbox is None else 5000 if produto == "perfis" else 3000
    )
    parametros: dict[str, str] = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"geonode:{CAMADAS[produto]}",
        "outputFormat": "csv",
        "propertyName": ",".join([*nomes, geometria] if bbox else nomes),
        "sortBy": f"{CHAVES[produto]} A",
    }
    if bbox is not None:
        parametros["BBOX"] = ",".join(str(valor) for valor in bbox) + ",EPSG:4326"
        parametros["srsName"] = "EPSG:4326"
    linhas: list[dict[str, str]] = []
    while limite is None or len(linhas) < limite:
        quantidade = PAGINA if limite is None else min(PAGINA, limite - len(linhas))
        resposta = cliente.get(
            URL, params={**parametros, "count": str(quantidade), "startIndex": str(len(linhas))}
        )
        resposta.raise_for_status()
        pagina = list(csv.DictReader(io.StringIO(resposta.content.decode("utf-8"))))
        linhas += pagina
        if len(pagina) < quantidade:
            break
    return linhas, len(linhas)


def selecionar(
    linhas: list[dict[str, str]], geometria: str, opcoes: dict[str, Any]
) -> list[dict[str, str]]:
    selecionadas = []
    for linha in linhas:
        if opcoes.get("uf") is not None and uf_normal(linha["uf"]) != opcoes["uf"]:
            continue
        if (
            opcoes.get("ordem") is not None
            and opcoes["ordem"].upper() not in linha["ordem1"].upper()
        ):
            continue
        bbox: Caixa | None = opcoes.get("bbox")
        if bbox is not None and not wkt.loads(linha[geometria]).intersects(box(*bbox)):
            continue
        selecionadas.append(linha)
    return selecionadas


def texto(valor: str) -> str:
    if not re.search("[ÃÂ][\x80-\xbf]", valor):
        return valor
    try:
        return valor.encode("latin-1").decode("utf-8")
    except UnicodeError:
        return valor


def uf_normal(valor: str) -> str | None:
    normal = valor.strip().upper()
    return normal if normal in UFS else None


def esperado(produto: str, linha: dict[str, str], nomes: list[str]) -> dict[str, Any]:
    renomes = RENOMES[produto]
    saida: dict[str, Any] = {}
    for nome in nomes:
        bruto = linha[nome]
        coluna = "uf_original" if produto == "perfis" and nome == "uf" else renomes.get(nome, nome)
        if bruto == "" or (
            produto == "perfis" and nome in {"ano", "data_colet"} and bruto == "NULL"
        ):
            saida[coluna] = None
        elif produto == "perfis" and nome == "ano":
            saida[coluna] = int(bruto)
        elif produto == "perfis" and nome == "data_colet":
            saida[coluna] = datetime.strptime(bruto, "%Y-%m-%d")
        elif nome in INTEIROS:
            saida[coluna] = int(bruto)
        elif nome in REAIS:
            saida[coluna] = float(bruto)
        else:
            saida[coluna] = texto(bruto)
    if produto == "perfis":
        saida["uf"] = uf_normal(linha["uf"])
    saida["feature_id"] = linha["FID"]
    return saida


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        linha: dict[str, Any] = {}
        for coluna, valor in registro.items():
            if pd.isna(valor) or valor == "":
                linha[str(coluna)] = None
            else:
                linha[str(coluna)] = valor.item() if hasattr(valor, "item") else valor
        linhas.append(linha)
    return linhas


def iguais(obtido: Any, oficial: Any) -> bool:
    if isinstance(obtido, float) and isinstance(oficial, float):
        return math.isclose(obtido, oficial, rel_tol=0, abs_tol=TOLERANCIA)
    if isinstance(obtido, dict) and isinstance(oficial, dict):
        return obtido.keys() == oficial.keys() and all(
            iguais(obtido[k], oficial[k]) for k in obtido
        )
    if isinstance(obtido, list) and isinstance(oficial, list):
        return len(obtido) == len(oficial) and all(
            iguais(a, b) for a, b in zip(obtido, oficial, strict=True)
        )
    return bool(obtido == oficial)


def coordenadas(geometria: Any) -> Any:
    return json.loads(json.dumps(geometria.__geo_interface__["coordinates"]))


async def saida_agrobr(produto: str, opcoes: dict[str, Any]) -> tuple[Any, list[str]]:
    from agrobr import embrapa_solos

    funcao: Callable[..., Awaitable[Any]]
    if opcoes.get("bbox") is not None:
        funcao = embrapa_solos.perfis_geo if produto == "perfis" else embrapa_solos.mapa_solos_geo
    else:
        funcao = embrapa_solos.perfis if produto == "perfis" else embrapa_solos.mapa_solos
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame = await funcao(**opcoes)
    return frame, [str(item.message) for item in capturados]


def comparar(cliente: httpx.Client, produto: str, opcoes: dict[str, Any]) -> dict[str, Any]:
    nomes, geometria = atributos(cliente, produto)
    frame, avisos = asyncio.run(saida_agrobr(produto, opcoes))
    lidas, varridas = linhas_oficiais(cliente, produto, nomes, geometria, opcoes)
    selecionadas = selecionar(lidas, geometria, opcoes)
    ordem = colunas(produto, nomes)
    problemas: list[str] = []
    if list(frame.columns) != [*ordem, *(["geometry"] if opcoes.get("bbox") else [])]:
        problemas.append("colunas divergentes")
    oficiais = [
        {coluna: esperado(produto, linha, nomes)[coluna] for coluna in ordem}
        for linha in selecionadas
    ]
    observadas = publicado(frame)
    if len(observadas) != len(oficiais):
        problemas.append(f"linhas: agrobr {len(observadas)} × oficiais {len(oficiais)}")
    divergentes = sum(1 for a, b in zip(observadas, oficiais, strict=False) if not iguais(a, b))
    if divergentes:
        problemas.append(f"{divergentes} linhas divergentes")
    if opcoes.get("bbox") is not None:
        erradas = sum(
            1
            for obtida, linha in zip(frame.geometry, selecionadas, strict=False)
            if not iguais(coordenadas(obtida), coordenadas(wkt.loads(linha[geometria])))
        )
        if erradas:
            problemas.append(f"{erradas} geometrias divergentes")
        if frame.crs is None or frame.crs.to_epsg() != 4326:
            problemas.append(f"CRS {frame.crs}")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas_oficiais_lidas": varridas,
        "linhas_publicadas": len(observadas),
        "celulas": len(oficiais) * len(ordem),
        "avisos_agrobr": [aviso for aviso in avisos if "prefixo remoto" in aviso],
    }


def run(saida: Path) -> int:
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=300)
    ) as cliente:
        for nome, produto, opcoes in CASOS:
            resultados[nome] = comparar(cliente, produto, opcoes)
            print(nome, resultados[nome]["status"], resultados[nome]["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "WFS 2.0 em CSV (lido com csv) × saída pública do agrobr (JSON); nulo do CSV = campo vazio; o CSV do GeoServer arredonda doubles a 8 casas, então números e coordenadas comparam com tolerância absoluta de 1e-8",
        "checks": resultados,
    }
    saida.parent.mkdir(parents=True, exist_ok=True)
    saida.write_text(
        json.dumps(relatorio, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    falhas = sum(check["status"] != "ok" for check in resultados.values())
    print(f"{len(resultados) - falhas} ok / {falhas} mismatch")
    return int(falhas > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo perfis e mapa da Embrapa Solos contra o WFS em CSV"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())

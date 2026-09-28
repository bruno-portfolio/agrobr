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

URL = "https://geoserver.funai.gov.br/geoserver/Funai/ows"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
CAMADA = "tis_poligonais"
PRINCIPAIS = {
    "terrai_codigo": "codigo",
    "terrai_nome": "nome",
    "etnia_nome": "etnia",
    "municipio_nome": "municipio",
    "uf_sigla": "uf",
    "superficie_perimetro_ha": "area_ha",
    "fase_ti": "fase",
    "modalidade_ti": "modalidade",
    "data_atualizacao": "data_atualizacao",
}
INTEIROS = {"gid", "terrai_codigo", "undadm_codigo", "epsg"}
REAIS = {"superficie_perimetro_ha"}
TOLERANCIA = 1e-8
Caixa = tuple[float, float, float, float]
CASOS: list[tuple[str, dict[str, Any]]] = [
    ("todas", {"max_registros": None, "tamanho_pagina": 1000}),
    ("uf_pa", {"uf": "PA", "max_registros": None, "tamanho_pagina": 1000}),
    ("fase_declarada", {"fase": "Declarada", "max_registros": None, "tamanho_pagina": 1000}),
    ("bbox_dentro_ti_101", {"bbox": (-66.852, -2.627, -66.812, -2.587)}),
    ("bbox_canto_ti_101", {"bbox": (-66.9038, -2.6766, -66.8998, -2.6726)}),
    ("oceano", {"bbox": (-30.0, -20.0, -29.9, -19.9)}),
]


def atributos(cliente: httpx.Client) -> tuple[list[str], str]:
    resposta = cliente.get(
        URL,
        params={
            "service": "WFS",
            "version": "2.0.0",
            "request": "DescribeFeatureType",
            "typeNames": f"Funai:{CAMADA}",
        },
    )
    resposta.raise_for_status()
    elementos = re.findall(r'<xsd:element[^>]*name="([^"]+)"[^>]*type="([^"]+)"', resposta.text)
    nomes = [n for n, t in elementos if not t.startswith("gml:") and n != CAMADA]
    geometria = next(n for n, t in elementos if t.startswith("gml:"))
    return nomes, geometria


def colunas(nomes: list[str]) -> list[str]:
    resto = [nome for nome in nomes if nome not in PRINCIPAIS]
    return [*PRINCIPAIS.values(), "feature_id", *resto]


def linhas_oficiais(
    cliente: httpx.Client, nomes: list[str], geometria: str, opcoes: dict[str, Any]
) -> list[dict[str, str]]:
    bbox: Caixa | None = opcoes.get("bbox")
    parametros: dict[str, str] = {
        "service": "WFS",
        "version": "2.0.0",
        "request": "GetFeature",
        "typeNames": f"Funai:{CAMADA}",
        "outputFormat": "csv",
        "propertyName": ",".join([*nomes, geometria] if bbox else nomes),
        "sortBy": "terrai_codigo A,gid A",
        "count": "5000",
    }
    if bbox is not None:
        parametros["BBOX"] = ",".join(str(valor) for valor in bbox) + ",EPSG:4326"
        parametros["srsName"] = "EPSG:4326"
    resposta = cliente.get(URL, params=parametros)
    resposta.raise_for_status()
    return list(csv.DictReader(io.StringIO(resposta.content.decode("utf-8"))))


def ufs(texto: str) -> set[str]:
    return {parte.strip().upper() for parte in texto.split(",")} if texto else set()


def selecionar(
    linhas: list[dict[str, str]], geometria: str, opcoes: dict[str, Any]
) -> list[dict[str, str]]:
    selecionadas = []
    for linha in linhas:
        if opcoes.get("uf") is not None and opcoes["uf"] not in ufs(linha["uf_sigla"]):
            continue
        if opcoes.get("fase") is not None and linha["fase_ti"] != opcoes["fase"]:
            continue
        bbox: Caixa | None = opcoes.get("bbox")
        if bbox is not None and not wkt.loads(linha[geometria]).intersects(box(*bbox)):
            continue
        selecionadas.append(linha)
    return selecionadas


def esperado(linha: dict[str, str], nomes: list[str]) -> dict[str, Any]:
    saida: dict[str, Any] = {}
    for nome in nomes:
        bruto = linha[nome]
        coluna = PRINCIPAIS.get(nome, nome)
        if bruto == "":
            saida[coluna] = None
        elif nome in INTEIROS:
            saida[coluna] = int(bruto)
        elif nome in REAIS:
            saida[coluna] = float(bruto)
        else:
            saida[coluna] = bruto
    return saida


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns=["geometry", "feature_id"], errors="ignore").to_dict(
        "records"
    ):
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


async def saida_agrobr(opcoes: dict[str, Any]) -> tuple[Any, list[str]]:
    from agrobr import funai

    funcao: Callable[..., Awaitable[Any]] = (
        funai.terras_indigenas_geo if opcoes.get("bbox") is not None else funai.terras_indigenas
    )
    with warnings.catch_warnings(record=True) as capturados:
        warnings.simplefilter("always")
        frame = await funcao(**opcoes)
    return frame, [str(item.message) for item in capturados]


def comparar(cliente: httpx.Client, opcoes: dict[str, Any]) -> dict[str, Any]:
    nomes, geometria = atributos(cliente)
    frame, avisos = asyncio.run(saida_agrobr(opcoes))
    lidas = linhas_oficiais(cliente, nomes, geometria, opcoes)
    selecionadas = selecionar(lidas, geometria, opcoes)
    ordem = colunas(nomes)
    problemas: list[str] = []
    if list(frame.columns) != [*ordem, *(["geometry"] if opcoes.get("bbox") else [])]:
        problemas.append("colunas divergentes")
    sem_id = [coluna for coluna in ordem if coluna != "feature_id"]
    oficiais = [
        {coluna: esperado(linha, nomes)[coluna] for coluna in sem_id} for linha in selecionadas
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
        "linhas_oficiais_lidas": len(lidas),
        "linhas_publicadas": len(observadas),
        "celulas": len(oficiais) * len(sem_id),
        "avisos_agrobr": [aviso for aviso in avisos if "prefixo remoto" in aviso],
    }


def run(saida: Path) -> int:
    resultados: dict[str, Any] = {}
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=300)
    ) as cliente:
        for nome, opcoes in CASOS:
            resultados[nome] = comparar(cliente, opcoes)
            print(nome, resultados[nome]["status"], resultados[nome]["problems"][:3], flush=True)
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "WFS 2.0 em CSV (lido com csv) × saída pública do agrobr (JSON); nulo do CSV = campo vazio; feature_id fica fora porque a FUNAI o gera a cada pedido; números e coordenadas com tolerância absoluta de 1e-8",
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
        description="Confere ao vivo as Terras Indígenas da FUNAI contra o WFS em CSV"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())

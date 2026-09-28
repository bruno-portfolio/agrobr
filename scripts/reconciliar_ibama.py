from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import re
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import httpx
import pandas as pd
import shapely

CSV_URL = "https://stibamadadosabertosprd.blob.core.windows.net/dados-abertos/dados/TERMOS_DE_EMBARGO/TERMO_EMBARGO/termo_de_embargo.csv"
CKAN_URL = "https://dadosabertos.ibama.gov.br/api/3/action/package_show"
CONJUNTO = "fiscalizacao-termo-de-embargo"
AGENT = "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
BRASILIA = timedelta(hours=-3)
IDADE_MAXIMA = timedelta(days=7)
BBOX_DOC = (-56.0, -16.0, -54.0, -14.0)
COLUNAS = [
    ("seq_tad", "SEQ_TAD", "texto"),
    ("numero_tad", "NUM_TAD", "texto"),
    ("data_embargo", "DAT_EMBARGO", "data"),
    ("num_processo", "NUM_PROCESSO", "texto"),
    ("descricao", "DES_TAD", "texto"),
    ("codigo_municipio", "COD_MUNICIPIO", "texto"),
    ("municipio", "MUNICIPIO", "texto"),
    ("uf", "UF", "texto"),
    ("latitude", "NUM_LATITUDE_TAD", "numero"),
    ("longitude", "NUM_LONGITUDE_TAD", "numero"),
    ("area_embargada_ha", "QTD_AREA_EMBARGADA", "area"),
    ("nome_imovel", "NOME_IMOVEL", "texto"),
    ("status", "DES_STATUS_FORMULARIO", "texto"),
    ("cancelado", "SIT_CANCELADO", "sim_nao"),
    ("data_desembargo", "DAT_DESEMBARGO", "data"),
]
CASOS: list[tuple[str, bool, dict[str, Any]]] = [
    ("todos", False, {}),
    ("uf_df", False, {"uf": "DF"}),
    ("bbox_doc_ponto", False, {"bbox": BBOX_DOC}),
    ("geo_uf_rr", True, {"uf": "RR"}),
    ("geo_bbox_doc_intersecao", True, {"bbox": BBOX_DOC}),
]


def valor(bruto: str, tipo: str) -> Any:
    if bruto == "":
        return False if tipo == "sim_nao" else None
    if tipo == "data":
        return datetime.strptime(bruto, "%Y-%m-%d %H:%M:%S")
    if tipo == "area":
        inteiro, _, fracao = bruto.partition(",")
        return float(Decimal(f"{inteiro}.{fracao or '0'}"))
    if tipo == "numero":
        return float(Decimal(bruto))
    if tipo == "sim_nao":
        return bruto == "S"
    return bruto


def esperado(linha: dict[str, str]) -> dict[str, Any]:
    return {saida: valor(linha[origem], tipo) for saida, origem, tipo in COLUNAS}


def publicado(frame: pd.DataFrame) -> list[dict[str, Any]]:
    linhas = []
    for registro in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        linha: dict[str, Any] = {}
        for coluna, bruto in registro.items():
            if bruto is None or bruto is pd.NaT or (not isinstance(bruto, str) and pd.isna(bruto)):
                linha[str(coluna)] = None
            elif isinstance(bruto, pd.Timestamp):
                linha[str(coluna)] = bruto.to_pydatetime()
            else:
                linha[str(coluna)] = bruto.item() if hasattr(bruto, "item") else bruto
        linhas.append(linha)
    return linhas


def ponto_no_bbox(linha: dict[str, str], bbox: tuple[float, float, float, float]) -> bool:
    try:
        lon, lat = float(linha["NUM_LONGITUDE_TAD"]), float(linha["NUM_LATITUDE_TAD"])
    except ValueError:
        return False
    return bbox[0] <= lon <= bbox[2] and bbox[1] <= lat <= bbox[3]


def selecionar(
    linhas: list[dict[str, str]], geo: bool, opcoes: dict[str, Any]
) -> list[dict[str, str]]:
    uf: str | None = opcoes.get("uf")
    bbox: tuple[float, float, float, float] | None = opcoes.get("bbox")
    caixa = shapely.box(*bbox) if bbox is not None else None
    selecionadas = []
    for linha in linhas:
        if uf is not None and linha["UF"].strip().upper() != uf:
            continue
        if geo:
            wkt = linha["GEOM_AREA_EMBARGADA"]
            geometria = shapely.from_wkt(wkt, on_invalid="ignore") if wkt else None
            if geometria is None or (
                caixa is not None and not shapely.intersects(geometria, caixa)
            ):
                continue
        elif bbox is not None and not ponto_no_bbox(linha, bbox):
            continue
        selecionadas.append(linha)
    return selecionadas


def numeros_do_wkt(wkt: str) -> list[float]:
    return [float(n) for n in re.findall(r"-?\d+(?:\.\d+)?(?:[eE][-+]?\d+)?", wkt)]


async def saida_agrobr(geo: bool, opcoes: dict[str, Any]) -> tuple[Any, Any]:
    from agrobr import ibama

    if geo:
        return await ibama.embargos_geo(return_meta=True, **opcoes)
    return await ibama.embargos(return_meta=True, **opcoes)


def comparar(linhas: list[dict[str, str]], geo: bool, opcoes: dict[str, Any]) -> dict[str, Any]:
    frame, meta = asyncio.run(saida_agrobr(geo, opcoes))
    selecionadas = selecionar(linhas, geo, opcoes)
    problemas: list[str] = []
    esperadas = [saida for saida, _, _ in COLUNAS] + (["geometry"] if geo else [])
    if list(frame.columns) != esperadas:
        problemas.append(f"colunas: {list(frame.columns)}")
    if meta.source_url != CSV_URL:
        problemas.append(f"source_url {meta.source_url}")
    oficiais = [esperado(linha) for linha in selecionadas]
    observadas = publicado(frame)
    if len(observadas) != len(oficiais):
        problemas.append(f"linhas: agrobr {len(observadas)} × oficiais {len(oficiais)}")
    divergentes = sum(1 for a, b in zip(observadas, oficiais, strict=False) if a != b)
    if divergentes:
        problemas.append(f"{divergentes} linhas divergentes")
    numeros = 0
    if geo:
        erradas = 0
        for geometria, linha in zip(frame.geometry, selecionadas, strict=False):
            oficiais_wkt = numeros_do_wkt(linha["GEOM_AREA_EMBARGADA"])
            numeros += len(oficiais_wkt)
            if shapely.get_coordinates(geometria).ravel().tolist() != oficiais_wkt:
                erradas += 1
        if erradas:
            problemas.append(f"{erradas} geometrias divergentes")
        if frame.crs is None or frame.crs.to_epsg() != 4326:
            problemas.append(f"CRS {frame.crs}")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "linhas_oficiais": len(oficiais),
        "linhas_publicadas": len(observadas),
        "celulas": len(oficiais) * len(COLUNAS),
        "numeros_de_coordenada": numeros,
        "edicao_servida": meta.source_details.get("ultima_atualizacao_relatorio"),
    }


def edicao(
    linhas: list[dict[str, str]],
    servida: str | None,
    last_modified: str | None,
    catalogo: dict[str, Any],
) -> dict[str, Any]:
    valores = sorted({linha["ULTIMA_ATUALIZACAO_RELATORIO"] for linha in linhas})
    problemas: list[str] = []
    if len(valores) != 1:
        problemas.append(f"edições no arquivo: {valores}")
    oficial = valores[-1] if valores else None
    if servida != oficial:
        problemas.append(f"edição servida pelo agrobr {servida} × arquivo oficial {oficial}")
    agora = datetime.now(UTC)
    idade = None
    if oficial is not None:
        emitida = datetime.strptime(oficial, "%Y-%m-%d %H:%M:%S").replace(tzinfo=UTC) - BRASILIA
        idade = agora - emitida
        if idade > IDADE_MAXIMA:
            problemas.append(
                f"edição parada há {idade.days} dias (a fonte declara atualização diária)"
            )
        if last_modified is not None:
            gravado = datetime.strptime(last_modified, "%a, %d %b %Y %H:%M:%S GMT").replace(
                tzinfo=UTC
            )
            if not timedelta(0) <= gravado - emitida <= timedelta(hours=12):
                problemas.append(f"Last-Modified {last_modified} longe da edição {oficial}")
    recurso = next(
        (
            r
            for r in catalogo.get("resources", [])
            if str(r.get("format", "")).upper() == "CSV"
            and str(r.get("name", "")).strip() == "Termos de embargo"
        ),
        None,
    )
    url_catalogo = None if recurso is None else str(recurso.get("url", ""))
    if url_catalogo is None or not url_catalogo.endswith("/termo_de_embargo.csv"):
        problemas.append(f"catálogo sem o recurso CSV 'Termos de embargo' ({url_catalogo})")
    return {
        "status": "ok" if not problemas else "mismatch",
        "problems": problemas,
        "edicao_oficial": oficial,
        "edicao_servida": servida,
        "idade_horas": None if idade is None else round(idade.total_seconds() / 3600, 1),
        "blob_last_modified": last_modified,
        "ckan_metadata_modified": catalogo.get("metadata_modified"),
        "ckan_url_do_recurso": url_catalogo,
        "ckan_url_confere_com_o_agrobr": url_catalogo == CSV_URL,
    }


def run(saida: Path) -> int:
    with httpx.Client(
        headers={"User-Agent": AGENT}, timeout=httpx.Timeout(60, read=600), follow_redirects=True
    ) as cliente:
        resposta = cliente.get(CSV_URL)
        resposta.raise_for_status()
        catalogo = cliente.get(CKAN_URL, params={"id": CONJUNTO}).json()["result"]
    texto = resposta.content.decode("utf-8-sig")
    csv.field_size_limit(1 << 30)
    linhas = list(csv.DictReader(io.StringIO(texto, newline=""), delimiter=";"))
    resultados: dict[str, Any] = {}
    for nome, geo, opcoes in CASOS:
        resultados[nome] = comparar(linhas, geo, opcoes)
        print(nome, resultados[nome]["status"], resultados[nome]["problems"][:3], flush=True)
    resultados["edicao"] = edicao(
        linhas,
        resultados["todos"]["edicao_servida"],
        resposta.headers.get("last-modified"),
        catalogo,
    )
    print(
        "edicao", resultados["edicao"]["status"], resultados["edicao"]["problems"][:3], flush=True
    )
    relatorio = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": (
            "CSV oficial do conjunto fiscalizacao-termo-de-embargo lido com csv × saída pública do agrobr; "
            "área e coordenadas por Decimal, datas por strptime, bbox da _geo por shapely.intersects direto; "
            "geometria pelos números do WKT; edição servida × arquivo × Last-Modified × catálogo"
        ),
        "csv_bytes": len(resposta.content),
        "registros_oficiais": len(linhas),
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
        description="Confere ao vivo os embargos do IBAMA contra o CSV oficial e a edição publicada"
    )
    parser.add_argument("--output", required=True, type=Path)
    return run(parser.parse_args().output)


if __name__ == "__main__":
    raise SystemExit(main())

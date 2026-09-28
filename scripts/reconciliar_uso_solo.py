from __future__ import annotations

import argparse
import asyncio
import json
import sys
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import httpx

from agrobr import constants
from agrobr.desmatamento import models as desmatamento_models
from agrobr.http.user_agents import UserAgentRotator
from agrobr.queimadas import models as queimadas_models

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / "tests/golden_data/reconciliacao_r12_20260918/manifest.json"
GEOSERVER = constants.URLS[constants.Fonte.DESMATAMENTO]["geoserver"]
FOCOS_BASE = constants.URLS[constants.Fonte.QUEIMADAS]["dados_abertos"]
MAPBIOMAS_WORKBOOKS = {
    "mapbiomas_colecao_11_estadual": (
        constants.URLS[constants.Fonte.MAPBIOMAS]["biome_state_collection_11"],
        "../mapbiomas/collection11_official/response.xlsx",
    ),
    "mapbiomas_colecao_10_estadual": (
        f"{constants.URLS[constants.Fonte.MAPBIOMAS]['dataverse']}/"
        f"{constants.URLS[constants.Fonte.MAPBIOMAS]['biome_state_file_id']}?format=original",
        "mapbiomas/mapbiomas_col10_biome_state_recorte.xlsx",
    ),
}

TIPOS_GEOMETRIA = frozenset({"MultiPolygon"})
TIPOS_PUBLICADOS: dict[str, dict[str, frozenset[str]]] = {
    "PRODES": {
        "fid": frozenset({"int"}),
        "state": frozenset({"string"}),
        "path_row": frozenset({"string"}),
        "main_class": frozenset({"string"}),
        "class_name": frozenset({"string"}),
        "def_cloud": frozenset({"int", "number"}),
        "julian_day": frozenset({"number"}),
        "image_date": frozenset({"date"}),
        "year": frozenset({"int", "number"}),
        "area_km": frozenset({"number"}),
        "scene_id": frozenset({"number"}),
        "publish_year": frozenset({"date"}),
        "source": frozenset({"string"}),
        "satellite": frozenset({"string"}),
        "sensor": frozenset({"string"}),
        "uuid": frozenset({"string"}),
        "pub_date": frozenset({"string"}),
    },
    "DETER": {
        "gid": frozenset({"string"}),
        "classname": frozenset({"string"}),
        "quadrant": frozenset({"string"}),
        "path_row": frozenset({"string"}),
        "view_date": frozenset({"date"}),
        "sensor": frozenset({"string"}),
        "satellite": frozenset({"string"}),
        "areauckm": frozenset({"number"}),
        "uc": frozenset({"string"}),
        "areamunkm": frozenset({"number"}),
        "municipality": frozenset({"string"}),
        "uf": frozenset({"string"}),
        "publish_month": frozenset({"date"}),
        "mun_geocod": frozenset({"string"}),
        "created_date": frozenset({"date"}),
        "areatotalkm": frozenset({"number"}),
    },
}


def describe_url(workspace: str, layer: str) -> str:
    return (
        f"{GEOSERVER}/{workspace}/ows?service=WFS&version=2.0.0&request=DescribeFeatureType"
        f"&typeNames={workspace}:{layer}&outputFormat=application%2Fjson"
    )


def compare_wfs_layer(
    product: str, biome: str, layer: str, described: Any, status: int
) -> dict[str, Any]:
    problems: list[str] = []
    declared = desmatamento_models.layout_properties(product, biome)
    geometry = desmatamento_models.layout_geometry_column(product, biome)
    published: dict[str, str] = {}
    if status != 200 or not isinstance(described, dict):
        problems.append(f"DescribeFeatureType respondeu {status} sem descrição utilizável")
    else:
        types = [
            entry for entry in described.get("featureTypes", []) if entry.get("typeName") == layer
        ]
        if not types:
            problems.append(f"camada {layer} ausente da descrição publicada")
        else:
            published = {
                str(item["name"]): str(item.get("localType"))
                for item in types[0].get("properties", [])
            }
            if not published:
                problems.append(f"camada {layer} publicada sem propriedades")
    if published:
        if geometry not in published:
            problems.append(f"coluna de geometria {geometry} ausente da camada")
        elif published[geometry] not in TIPOS_GEOMETRIA:
            problems.append(
                f"tipo da geometria {geometry}: publicado {published[geometry]!r}, "
                f"esperado um de {sorted(TIPOS_GEOMETRIA)}"
            )
        atributos = set(published) - {geometry}
        for name in sorted(set(declared) - atributos):
            problems.append(f"propriedade declarada ausente na fonte: {name}")
        for name in sorted(atributos - set(declared)):
            problems.append(f"propriedade nova sem decisão: {name}={published[name]!r}")
        for name in sorted(atributos & set(declared)):
            aceitos = TIPOS_PUBLICADOS[product].get(name, frozenset())
            if published[name] not in aceitos:
                problems.append(
                    f"tipo de {name}: publicado {published[name]!r}, esperado um de {sorted(aceitos)}"
                )
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "propriedades_publicadas": len(published),
        "propriedades_declaradas": len(declared),
    }


def compare_focos_header(header: list[str]) -> dict[str, Any]:
    declared = list(queimadas_models.COLUNAS_CSV)
    problems: list[str] = []
    if not header:
        problems.append("cabeçalho vazio ou não lido")
    else:
        for name in sorted(set(declared) - set(header)):
            problems.append(f"coluna declarada ausente na publicação: {name}")
        for name in sorted(set(header) - set(declared)):
            problems.append(f"coluna nova sem decisão: {name}")
        if header != declared and not problems:
            problems.append(f"ordem publicada {header} != declarada {declared}")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "colunas_publicadas": len(header),
    }


def compare_workbook_identity(
    headers: dict[str, str], status: int, recorded: dict[str, Any]
) -> dict[str, Any]:
    problems: list[str] = []
    if status != 200:
        problems.append(f"HEAD respondeu {status}")
    tamanho = headers.get("content-length")
    esperado = recorded.get("corpo_original_bytes")
    if tamanho is not None and esperado is not None and int(tamanho) != int(esperado):
        problems.append(f"content-length {tamanho} != {esperado} bytes da captura")
    if tamanho is None:
        problems.append("resposta sem content-length")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "etag": headers.get("etag"),
        "last_modified": headers.get("last-modified"),
        "content_length": tamanho,
        "nota": "disponibilidade e identidade HTTP do workbook; não revalida abas nem células",
    }


def recorded_files() -> dict[str, dict[str, Any]]:
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))
    return {entry["file"]: entry for entry in manifest["files"]}


async def focos_header(http: httpx.AsyncClient) -> tuple[str, list[str], int]:
    hoje = date.today()
    for recuo in range(0, 4):
        ano, mes = divmod(hoje.year * 12 + hoje.month - 1 - recuo, 12)
        url = f"{FOCOS_BASE}/mensal/Brasil/focos_mensal_br_{ano:04d}{mes + 1:02d}.csv"
        response = await http.get(url, headers={"range": "bytes=0-2047"})
        if response.status_code in (200, 206):
            linha = response.content.split(b"\n", 1)[0].decode("utf-8-sig").strip()
            return url, linha.split(","), response.status_code
    return url, [], response.status_code


async def run(output: Path) -> int:
    recorded = recorded_files()
    report: dict[str, Any] = {"fetched_at": datetime.now(UTC).isoformat(), "structure": []}
    headers = UserAgentRotator.get_bot_headers()
    async with httpx.AsyncClient(timeout=180, follow_redirects=True, headers=headers) as http:
        for product in ("PRODES", "DETER"):
            workspaces = (
                desmatamento_models.PRODES_WORKSPACES
                if product == "PRODES"
                else desmatamento_models.DETER_WORKSPACES
            )
            layers = (
                desmatamento_models.PRODES_LAYERS
                if product == "PRODES"
                else desmatamento_models.DETER_LAYERS
            )
            for biome, workspace in workspaces.items():
                response = await http.get(describe_url(workspace, layers[biome]))
                try:
                    described = response.json()
                except ValueError:
                    described = None
                report["structure"].append(
                    {
                        "case": f"{product.lower()}_{workspace}_{layers[biome]}",
                        "http": response.status_code,
                        **compare_wfs_layer(
                            product, biome, layers[biome], described, response.status_code
                        ),
                    }
                )
        url, header, status = await focos_header(http)
        report["structure"].append(
            {
                "case": "queimadas_focos_mensal_cabecalho",
                "http": status,
                "url": url,
                **compare_focos_header(header),
            }
        )
        for case, (workbook_url, golden) in MAPBIOMAS_WORKBOOKS.items():
            response = await http.head(workbook_url)
            report["structure"].append(
                {
                    "case": case,
                    "http": response.status_code,
                    "url": workbook_url,
                    **compare_workbook_identity(
                        dict(response.headers), response.status_code, recorded[golden]
                    ),
                }
            )
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(report, indent=1, ensure_ascii=False), encoding="utf-8")
    mismatches = [entry for entry in report["structure"] if entry["status"] != "ok"]
    for entry in report["structure"]:
        print(entry["status"], entry["case"], "; ".join(entry["problems"]))
    print(
        f"{len(report['structure']) - len(mismatches)} ok / {len(mismatches)} mismatch -> {output}"
    )
    return 1 if mismatches else 0


def main() -> int:
    parser = argparse.ArgumentParser(
        description=(
            "Inventário N1 live de uso do solo, desmatamento e queimadas "
            "(DescribeFeatureType do TerraBrasilis, cabeçalho dos focos, identidade dos XLSX MapBiomas)"
        )
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / f"reports/reconciliacao_uso_solo_{datetime.now(UTC):%Y%m%d}.json",
    )
    arguments = parser.parse_args()
    return asyncio.run(run(arguments.output))


if __name__ == "__main__":
    sys.exit(main())

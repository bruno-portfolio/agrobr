from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import struct
import tempfile
import warnings
import zipfile
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import quote

import httpx
import pandas as pd

from agrobr import exceptions

BASE = "https://certificacao.incra.gov.br/csv_shp/zip/"
FILES = {
    "sigef_publico": "Sigef Público_{uf}.zip",
    "sigef_privado": "Sigef Privado_{uf}.zip",
    "snci": "Imóvel certificado SNCI Brasil_{uf}.zip",
    "assentamentos": "Assentamento Brasil.zip",
}
IBGE_UF = {
    11: "RO", 12: "AC", 13: "AM", 14: "RR", 15: "PA", 16: "AP", 17: "TO",
    21: "MA", 22: "PI", 23: "CE", 24: "RN", 25: "PB", 26: "PE", 27: "AL", 28: "SE", 29: "BA",
    31: "MG", 32: "ES", 33: "RJ", 35: "SP", 41: "PR", 42: "SC", 43: "RS",
    50: "MS", 51: "MT", 52: "GO", 53: "DF",
}  # fmt: skip
UFS = sorted(IBGE_UF.values())
SIRGAS_2000 = (
    'GEOGCS["SIRGAS 2000",DATUM["D_SIRGAS_2000",SPHEROID["GRS_1980",6378137,298.257222101]],'
    'PRIMEM["Greenwich",0],UNIT["Degree",0.017453292519943295]]'
)
Rings = list[list[tuple[float, ...]]]


def member(archive: bytes, suffix: str) -> bytes:
    with zipfile.ZipFile(BytesIO(archive)) as handle:
        (name,) = [name for name in handle.namelist() if name.lower().endswith(suffix)]
        return handle.read(name)


def dbf(archive: bytes) -> list[dict[str, str]]:
    raw = member(archive, ".dbf")
    count, header, width = struct.unpack_from("<IHH", raw, 4)
    fields = []
    offset = 32
    while raw[offset] != 0x0D:
        fields.append((raw[offset : offset + 11].split(b"\0")[0].decode("ascii"), raw[offset + 16]))
        offset += 32
    records = []
    for index in range(count):
        position = header + index * width + 1
        record = {}
        for name, length in fields:
            record[name] = raw[position : position + length].decode("latin-1")
            position += length
        records.append(record)
    return records


def shapes(archive: bytes) -> list[Rings | None]:
    raw = member(archive, ".shp")
    result: list[Rings | None] = []
    offset = 100
    while offset < len(raw):
        length = struct.unpack_from(">i", raw, offset + 4)[0]
        content = offset + 8
        shape_type = struct.unpack_from("<i", raw, content)[0]
        if shape_type == 0:
            result.append(None)
        else:
            parts_count, points_count = struct.unpack_from("<ii", raw, content + 36)
            parts = list(struct.unpack_from(f"<{parts_count}i", raw, content + 44))
            start = content + 44 + 4 * parts_count
            coords = struct.unpack_from(f"<{2 * points_count}d", raw, start)
            columns = [coords[0::2], coords[1::2]]
            if shape_type == 15:
                z_start = start + 16 * points_count + 16
                columns.append(struct.unpack_from(f"<{points_count}d", raw, z_start))
            points = list(zip(*columns, strict=True))
            bounds = zip(parts, [*parts[1:], points_count], strict=True)
            result.append(sorted(points[first:last] for first, last in bounds))
        offset = content + 2 * length
    return result


def text(value: str) -> str | None:
    return value.strip(" ") or None


def dbf_date(value: str) -> pd.Timestamp | None:
    value = value.strip()
    if value in ("", "00000000"):
        return None
    try:
        parsed = datetime.strptime(value, "%Y%m%d")
    except ValueError:
        return None
    return pd.Timestamp(parsed) if 1900 <= parsed.year <= 2099 else None


def day_month_year(value: str) -> pd.Timestamp | None:
    value = value.strip()
    if not value:
        return None
    try:
        parsed = datetime.strptime(value, "%d/%m/%Y")
    except ValueError:
        return None
    return pd.Timestamp(parsed) if 1900 <= parsed.year <= 2099 else None


def integer(value: str) -> int | None:
    value = value.strip()
    return int(value) if value else None


def decimal(value: str) -> float | None:
    value = value.strip()
    return float(Decimal(value)) if value and set(value) != {"*"} else None


def sigef(record: dict[str, str], natureza: str) -> dict[str, Any]:
    return {
        "codigo_parcela": text(record["parcela_co"]),
        "rt": text(record["rt"]),
        "art": text(record["art"]),
        "situacao": text(record["situacao_i"]),
        "codigo_imovel": text(record["codigo_imo"]),
        "data_submissao": dbf_date(record["data_submi"]),
        "data_aprovacao": dbf_date(record["data_aprov"]),
        "status": text(record["status"]),
        "nome_area": text(record["nome_area"]),
        "registro_matricula": text(record["registro_m"]),
        "registro_data": dbf_date(record["registro_d"]),
        "cod_municipio": integer(record["municipio_"]),
        "uf": IBGE_UF[int(record["uf_id"])],
        "natureza": natureza,
    }


def snci(record: dict[str, str]) -> dict[str, Any]:
    return {
        "num_processo": text(record["num_proces"]),
        "sr": text(record["sr"]),
        "num_certificacao": text(record["num_certif"]),
        "data_certificacao": dbf_date(record["data_certi"]),
        "area_peca_tecnica": decimal(record["qtd_area_p"]),
        "cod_profissional": text(record["cod_profis"]),
        "cod_imovel_rural": text(record["cod_imovel"]),
        "nome_imovel": text(record["nome_imove"]),
        "uf": text(record["uf_municip"]),
    }


def assentamento(record: dict[str, str]) -> dict[str, Any]:
    return {
        "codigo_sipra": text(record["cd_sipra"]),
        "nome_projeto": text(record["nome_proje"]),
        "municipio": text(record["municipio"]),
        "uf": text(record["uf"]),
        "area_ha": decimal(record["area_hecta"]),
        "capacidade": integer(record["capacidade"]),
        "num_familias": integer(record["num_famili"]),
        "fase": integer(record["fase"]),
        "data_criacao": day_month_year(record["data_de_cr"]),
        "forma_obtencao": text(record["forma_obte"]),
        "data_obtencao": day_month_year(record["data_obten"]),
        "area_calc_ha": decimal(record["area_calc_"]),
        "sr": text(record["sr"]),
        "descricao_fase": text(record["descricao_"]),
    }


BUILDERS: dict[str, Callable[[dict[str, str]], dict[str, Any]]] = {
    "sigef_publico": lambda record: sigef(record, "publico"),
    "sigef_privado": lambda record: sigef(record, "privado"),
    "snci": snci,
    "assentamentos": assentamento,
}


def published(frame: pd.DataFrame) -> list[dict[str, Any]]:
    rows = []
    for row in frame.drop(columns="geometry", errors="ignore").to_dict("records"):
        cells: dict[str, Any] = {}
        for column, value in row.items():
            if pd.isna(value):
                cells[str(column)] = None
            else:
                cells[str(column)] = value.item() if hasattr(value, "item") else value
        rows.append(cells)
    return rows


def rings(geometry: Any) -> Rings | None:
    if geometry is None:
        return None
    polygons = list(geometry.geoms) if geometry.geom_type == "MultiPolygon" else [geometry]
    found: Rings = []
    for polygon in polygons:
        found.append([tuple(point) for point in polygon.exterior.coords])
        found.extend([tuple(point) for point in ring.coords] for ring in polygon.interiors)
    return sorted(found)


def url_of(tema: str, uf: str | None) -> str:
    return BASE + quote(FILES[tema].format(uf=uf) if uf else FILES[tema])


def fetch(client: httpx.Client, url: str, path: Path) -> dict[str, Any]:
    requested = datetime.now(UTC).isoformat()
    digest = hashlib.sha256()
    size = 0
    with client.stream("GET", url) as response, path.open("wb") as handle:
        for chunk in response.iter_bytes(1 << 20):
            digest.update(chunk)
            size += len(chunk)
            handle.write(chunk)
        status = response.status_code
        modified = response.headers.get("Last-Modified")
    return {
        "url": url,
        "status": status,
        "bytes": size,
        "sha256": digest.hexdigest(),
        "last_modified": modified,
        "requested_at": requested,
        "received_at": datetime.now(UTC).isoformat(),
    }


async def agrobr_outputs(tema: str, uf: str | None) -> tuple[pd.DataFrame, Any, Any, list[str]]:
    from agrobr import acervo_fundiario

    funcao, opcoes = (
        ("sigef", {"natureza": tema.removeprefix("sigef_")})
        if tema.startswith("sigef_")
        else (tema, {})
    )
    tabular_function = getattr(acervo_fundiario, funcao)
    geo_function = getattr(acervo_fundiario, f"{funcao}_geo")
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        if uf is None:
            frame, meta = await tabular_function(return_meta=True)
            geo = await geo_function()
        else:
            frame, meta = await tabular_function(uf, return_meta=True, **opcoes)
            geo = await geo_function(uf, **opcoes)
    return frame, meta, geo, [str(item.message) for item in caught]


def cached_sha(tema: str, uf: str | None) -> str | None:
    from agrobr.acervo_fundiario import client

    metadata = client._load_meta(client._meta_path(tema, uf))
    return None if metadata is None else str(metadata.get("sha256"))


def compare(
    tema: str, uf: str | None, archive: bytes, outputs: tuple[pd.DataFrame, Any, Any, list[str]]
) -> dict[str, Any]:
    frame, meta, geo, messages = outputs
    expected = [BUILDERS[tema](record) for record in dbf(archive)]
    observed = published(frame)
    problems = []
    if len(observed) != len(expected):
        problems.append(f"linhas: agrobr {len(observed)} × fonte {len(expected)}")
    divergent: dict[str, int] = {}
    examples: list[list[Any]] = []
    for index, (want, got) in enumerate(zip(expected, observed, strict=False)):
        for column, value in want.items():
            if got.get(column) != value:
                divergent[column] = divergent.get(column, 0) + 1
                if len(examples) < 10:
                    examples.append([index, column, repr(value), repr(got.get(column))])
    problems.extend(f"{column}: {count} células divergentes" for column, count in divergent.items())
    source = shapes(archive)
    geometries = [rings(item) for item in geo.geometry]
    different = sum(1 for want, got in zip(source, geometries, strict=False) if want != got)
    if len(geometries) != len(source) or different:
        problems.append(f"geometrias divergentes: {different} de {len(source)}")
    if member(archive, ".prj").decode("ascii") != SIRGAS_2000 or geo.crs.to_epsg() != 4674:
        problems.append(f"CRS: PRJ da fonte × agrobr EPSG:{geo.crs.to_epsg()}")
    if meta.source_url != url_of(tema, uf):
        problems.append(f"source_url: {meta.source_url}")
    return {
        "status": "ok" if not problems else "mismatch",
        "problems": problems,
        "registros": len(expected),
        "celulas": len(expected) * len(expected[0]) if expected else 0,
        "geometrias_nulas": source.count(None),
        "exemplos": examples,
        "avisos_agrobr": messages,
    }


def coverage(client: httpx.Client) -> dict[str, Any]:
    missing = []
    unavailable = []
    for tema in ("sigef_publico", "sigef_privado", "snci"):
        for uf in UFS:
            response = client.head(url_of(tema, uf))
            if tema == "snci" and response.status_code == 404:
                unavailable.append(f"{tema} {uf}: HTTP 404")
            elif response.status_code != 200:
                missing.append(f"{tema} {uf}: HTTP {response.status_code}")
    return {
        "status": "mismatch" if missing else "indisponivel" if unavailable else "ok",
        "problems": missing,
        "indisponiveis": unavailable,
        "ufs": len(UFS),
    }


def run(directory: Path, output: Path, sigef_ufs: list[str], snci_ufs: list[str]) -> int:
    directory.mkdir(parents=True, exist_ok=True)
    os.environ.pop("AGROBR_CACHE_CACHE_DIR", None)
    os.environ["AGROBR_CACHE_DIR"] = tempfile.mkdtemp(prefix="agrobr_reconciliar_acervo_")
    targets: list[tuple[str, str | None]] = [
        *((tema, uf) for uf in sigef_ufs for tema in ("sigef_publico", "sigef_privado")),
        *(("snci", uf) for uf in snci_ufs),
        ("assentamentos", None),
    ]
    receipts: dict[str, Any] = {}
    checks: dict[str, Any] = {}
    headers = {
        "User-Agent": "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)"
    }
    with httpx.Client(headers=headers, timeout=httpx.Timeout(60, read=600)) as client:
        checks["cobertura"] = coverage(client)
        for tema, uf in targets:
            name = f"{tema}_{uf}" if uf else tema
            try:
                outputs = asyncio.run(agrobr_outputs(tema, uf))
            except exceptions.SourceUnavailableError as exc:
                if tema != "snci" or not exc.last_error.startswith("HTTP 404"):
                    raise
                checks[name] = {"status": "indisponivel", "problems": [str(exc)]}
                print(name, checks[name]["status"], checks[name]["problems"], flush=True)
                continue
            receipts[name] = fetch(client, url_of(tema, uf), directory / f"{name}.zip")
            if tema == "snci" and receipts[name]["status"] == 404:
                checks[name] = {"status": "indisponivel", "problems": [f"{name}: HTTP 404"]}
                print(name, checks[name]["status"], checks[name]["problems"], flush=True)
                continue
            archive = (directory / f"{name}.zip").read_bytes()
            checks[name] = compare(tema, uf, archive, outputs)
            if cached_sha(tema, uf) != receipts[name]["sha256"]:
                checks[name]["problems"].append("revisão do arquivo mudou entre agrobr e captura")
                checks[name]["status"] = "mismatch"
            print(name, checks[name]["status"], checks[name]["problems"][:3], flush=True)
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "captura ao vivo independente (httpx direto, DBF/SHP lidos com struct, latin-1) × saída pública do agrobr",
        "receipts": receipts,
        "checks": checks,
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(
        json.dumps(result, indent=2, ensure_ascii=False, default=str), encoding="utf-8"
    )
    failed = sum(check["status"] == "mismatch" for check in checks.values())
    unavailable = sum(check["status"] == "indisponivel" for check in checks.values())
    print(
        f"{len(checks) - failed - unavailable} ok / {failed} mismatch / {unavailable} indisponíveis"
    )
    return int(failed > 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Confere ao vivo SIGEF, SNCI e assentamentos do INCRA contra leitura independente dos shapefiles"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--sigef", nargs="*", default=["AC", "AP"])
    parser.add_argument("--snci", nargs="*", default=["SC", "RR", "AL"])
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output, arguments.sigef, arguments.snci)


if __name__ == "__main__":
    raise SystemExit(main())

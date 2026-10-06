from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import math
import sys
import zipfile
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path
from typing import Any

import httpx

from agrobr import constants
from agrobr.http.user_agents import UserAgentRotator
from agrobr.nasa_power import client as nasa_client
from agrobr.nasa_power import models as nasa_models

ROOT = Path(__file__).resolve().parents[1]
GOLDEN = ROOT / "tests/golden_data"
INMET_MANIFEST = GOLDEN / "inmet/selecao_20260906/manifest.json"
NASA_GOLDEN = GOLDEN / "reconciliacao_clima_20260918/nasa_power/nasa_power_mt_2025.json"
INMET_CATALOG_URL = f"{constants.URLS[constants.Fonte.INMET]['estacoes']}/T"
NASA_UNITS = {
    "T2M": "C",
    "T2M_MAX": "C",
    "T2M_MIN": "C",
    "PRECTOTCORR": "mm/day",
    "RH2M": "%",
    "ALLSKY_SFC_SW_DWN": "MJ/m^2/day",
    "WS2M": "m/s",
}
NASA_TOP_LEVEL = {"type", "geometry", "properties", "header", "messages", "parameters", "times"}
COORDINATE_TOLERANCE = 1e-6


def compare_inmet_zip_identity(
    head_headers: dict[str, str], recorded: dict[str, Any]
) -> dict[str, Any]:
    problems = []
    for key in ("last-modified", "etag", "content-length"):
        published = head_headers.get(key)
        expected = recorded.get("headers", {}).get(key)
        if expected and published != expected:
            problems.append(f"{key}: publicado {published!r} != capturado {expected!r}")
    if not head_headers:
        problems.append("sem cabeçalhos na resposta HEAD")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "nota": "identidade HTTP do ZIP anual (HEAD); não revalida membros nem cabeçalhos do CSV",
    }


def _coordinate(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = float(str(value).replace(",", "."))
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def compare_inmet_station(
    catalog: list[dict[str, Any]], metadata: dict[str, str]
) -> dict[str, Any]:
    problems = []
    entry = next((e for e in catalog if e.get("CD_ESTACAO") == metadata.get("codigo")), None)
    if entry is None:
        problems.append(f"estação {metadata.get('codigo')} ausente do catálogo /estacoes/T")
    else:
        if entry.get("SG_ESTADO") != metadata.get("uf"):
            problems.append(
                f"UF do catálogo {entry.get('SG_ESTADO')!r} != {metadata.get('uf')!r} do CSV"
            )
        for key, field in (("VL_LATITUDE", "latitude"), ("VL_LONGITUDE", "longitude")):
            published = _coordinate(entry.get(key))
            recorded = _coordinate(metadata.get(field))
            if published is None or recorded is None:
                problems.append(
                    f"{field}: coordenada ausente ou não finita (catálogo {entry.get(key)!r}, CSV {metadata.get(field)!r})"
                )
            elif abs(published - recorded) > COORDINATE_TOLERANCE:
                problems.append(f"{field}: catálogo {published} != CSV {recorded}")
    return {"status": "mismatch" if problems else "ok", "problems": problems}


def compare_nasa_publication(live: dict[str, Any], golden: dict[str, Any]) -> dict[str, Any]:
    problems = []
    unknown_top = sorted(set(live) - NASA_TOP_LEVEL)
    if unknown_top:
        problems.append(f"chaves de topo não classificadas: {unknown_top}")
    for code, unit in NASA_UNITS.items():
        published = live.get("parameters", {}).get(code, {}).get("units")
        if published != unit:
            problems.append(f"{code}: unidade publicada {published!r} != {unit!r}")
    live_params = live.get("properties", {}).get("parameter", {})
    golden_params = golden.get("properties", {}).get("parameter", {})
    extra = sorted(set(live_params) - set(NASA_UNITS))
    if extra:
        problems.append(f"parâmetros publicados sem decisão: {extra}")
    definitions = set(live.get("parameters", {}))
    if definitions != set(live_params):
        problems.append(
            f"definições em parameters ({sorted(definitions)}) diferem das séries publicadas "
            f"({sorted(live_params)})"
        )
    for code, values in golden_params.items():
        current = live_params.get(code, {})
        if set(current) != set(values):
            problems.append(f"{code}: conjunto de dias mudou ({len(current)} vs {len(values)})")
            continue
        changed = [d for d in values if current[d] != values[d]]
        if changed:
            problems.append(
                f"{code}: {len(changed)} dias com valor diferente da captura (ex.: {changed[:3]})"
            )
    live_header = live.get("header", {})
    golden_header = golden.get("header", {})
    for key in ("fill_value", "time_standard"):
        if live_header.get(key) != golden_header.get(key):
            problems.append(f"header.{key}: {live_header.get(key)!r} != {golden_header.get(key)!r}")
    live_coordinates = live.get("geometry", {}).get("coordinates", [])
    golden_coordinates = golden.get("geometry", {}).get("coordinates", [])
    if len(live_coordinates) < 2 or len(golden_coordinates) < 2:
        problems.append("geometria sem longitude/latitude")
    else:
        for index, name in ((0, "longitude"), (1, "latitude")):
            published, recorded = (
                _coordinate(live_coordinates[index]),
                _coordinate(golden_coordinates[index]),
            )
            if published is None or recorded is None:
                problems.append(f"{name}: coordenada ausente ou não finita")
            elif abs(published - recorded) > COORDINATE_TOLERANCE:
                problems.append(f"{name}: {published} != {recorded} da captura")
    return {
        "status": "mismatch" if problems else "ok",
        "problems": problems,
        "dias": len(golden_params.get("T2M", {})),
    }


def inmet_member_metadata(zip_bytes: bytes, member: str) -> dict[str, str]:
    with zipfile.ZipFile(BytesIO(zip_bytes)) as archive:
        lines = archive.read(member).decode("latin-1").splitlines()[:8]
    metadata: dict[str, str] = {}
    for line in lines:
        key, _, value = line.partition(";")
        metadata[key.rstrip(":").strip().upper()] = value.strip()
    return {
        "codigo": metadata.get("CODIGO (WMO)", ""),
        "uf": metadata.get("UF", ""),
        "latitude": metadata.get("LATITUDE", ""),
        "longitude": metadata.get("LONGITUDE", ""),
    }


async def run(output: Path) -> int:
    manifest = json.loads(INMET_MANIFEST.read_text(encoding="utf-8"))
    recorded = next(r for r in manifest["requests"] if r["url"].endswith("/2001.zip"))
    zip_path = INMET_MANIFEST.parent / recorded["body_file"]
    zip_bytes = zip_path.read_bytes()
    report: dict[str, Any] = {"fetched_at": datetime.now(UTC).isoformat(), "structure": []}
    async with httpx.AsyncClient(
        timeout=120, follow_redirects=True, headers=UserAgentRotator.get_headers(source="inmet")
    ) as http:
        head = await http.head(recorded["url"])
        report["structure"].append(
            {
                "case": "inmet_zip_2001_identidade",
                "http": head.status_code,
                **compare_inmet_zip_identity(dict(head.headers), recorded),
                "sha256_local": hashlib.sha256(zip_bytes).hexdigest(),
            }
        )
        catalog = (await http.get(INMET_CATALOG_URL)).json()
        member = next(m["member"] for m in recorded["members"] if m["estacao"] == "A001")
        report["structure"].append(
            {
                "case": "inmet_catalogo_a001",
                **compare_inmet_station(catalog, inmet_member_metadata(zip_bytes, member)),
                "estacoes_no_catalogo": len(catalog),
            }
        )
    lat, lon = nasa_models.UF_COORDS["MT"]
    params: dict[str, str | float] = {
        "parameters": ",".join(nasa_models.validate_parameters(None)),
        "community": "AG",
        "longitude": lon,
        "latitude": lat,
        "start": "20250101",
        "end": "20251231",
        "format": "JSON",
        "time-standard": "LST",
    }
    async with httpx.AsyncClient(
        timeout=180,
        follow_redirects=True,
        headers=UserAgentRotator.get_headers(source="nasa_power"),
    ) as http:
        live = (await http.get(nasa_client.BASE_URL, params=params)).json()
    golden = json.loads(NASA_GOLDEN.read_text(encoding="utf-8"))
    report["structure"].append(
        {"case": "nasa_power_mt_2025_publicacao", **compare_nasa_publication(live, golden)}
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
        description="Inventário N1 live do clima (INMET dadoshistoricos/catálogo, NASA POWER)"
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=ROOT / f"reports/reconciliacao_clima_{datetime.now(UTC):%Y%m%d}.json",
    )
    arguments = parser.parse_args()
    return asyncio.run(run(arguments.output))


if __name__ == "__main__":
    sys.exit(main())

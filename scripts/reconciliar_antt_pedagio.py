from __future__ import annotations

import argparse
import asyncio
import collections
import csv
import hashlib
import io
import json
import re
import warnings
from datetime import UTC, date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any, cast

import httpx
import pandas as pd

CATALOG = "https://dados.antt.gov.br/api/3/action/package_show?id={}"
TRAFFIC = "volume-trafego-praca-pedagio"
PLAZAS = "praca-de-pedagio"
RESOURCE = re.compile(r"volume-trafego-praca-pedagio-(\d{4})(_mensal_consolidado|_diario)?\.csv")
COUNT = re.compile(r"[+]?[0-9]+(?:[,.]0+)?")
AXLES = re.compile(r"(?:ve[ií]culo\s+(?:comercial|passeio)\s+)?([0-9]+)\s+eixos?", re.IGNORECASE)
ENCODING = "cp1252"
Key = tuple[date, str, str, str, str, str, str]


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
    return {
        "url": url,
        "status": status,
        "bytes": size,
        "sha256": digest.hexdigest(),
        "requested_at": requested,
        "received_at": datetime.now(UTC).isoformat(),
    }


def resources(package: dict[str, Any], years: list[int], daily: bool) -> dict[str, str]:
    selected = {}
    for item in package["resources"]:
        match = RESOURCE.fullmatch(item["url"].rsplit("/", 1)[-1])
        if match is None or item["format"].strip().upper() != "CSV":
            continue
        year, suffix = int(match[1]), match[2] or ""
        if year not in years or (suffix == "" and year >= 2024):
            continue
        if suffix == "_diario" and not daily:
            continue
        selected[f"{'diario' if suffix == '_diario' else 'mensal'}_{year}.csv"] = item["url"]
    return selected


def client() -> httpx.Client:
    headers = {
        "User-Agent": "agrobr-reconciliacao/1.0 (+https://github.com/bruno-portfolio/agrobr)",
        "Accept-Encoding": "identity",
    }
    return httpx.Client(
        timeout=httpx.Timeout(60, read=300), headers=headers, follow_redirects=False
    )


def catalogs(http: httpx.Client, directory: Path, receipts: dict[str, Any]) -> dict[str, Any]:
    packages = {}
    for slug in (TRAFFIC, PLAZAS):
        name = f"ckan_{slug}.json"
        receipts[name] = fetch(http, CATALOG.format(slug), directory / name)
        packages[slug] = json.loads((directory / name).read_bytes())["result"]
    plazas = [item for item in packages[PLAZAS]["resources"] if item["format"].upper() == "CSV"]
    receipts["links_pracas"] = [item["url"] for item in plazas]
    if len(plazas) == 1:
        receipts["pracas.csv"] = fetch(http, plazas[0]["url"], directory / "pracas.csv")
    return packages


def decode(body: bytes) -> tuple[str, list[str]]:
    problems = []
    try:
        if not body.isascii():
            body.decode("utf-8")
            problems.append("corpo é UTF-8 válido com bytes não ASCII: encoding da fonte mudou")
    except UnicodeDecodeError:
        pass
    return body.decode(ENCODING), problems


def month_of(reference: str) -> date:
    pieces = [int(piece) for piece in reference.split("/")]
    return date(pieces[-1], pieces[-2], 1)


def day_of(reference: str) -> date:
    day, month, year = (int(piece) for piece in reference.split("/"))
    return date(year, month, day)


def oracle(body: bytes, frequency: str) -> dict[str, Any]:
    text, problems = decode(body)
    reader = csv.reader(io.StringIO(text, newline=""), delimiter=";", strict=True)
    header = next(reader)
    position = {name: index for index, name in enumerate(header)}
    category = position.get("categoria_eixo", position.get("categoria"))
    blocks: dict[tuple[str, date], collections.Counter[tuple[str, ...]]] = collections.defaultdict(
        collections.Counter
    )
    anomalies = {"references_outside_day_one": 0, "uncountable_volumes": 0}
    rows = 0
    for row in reader:
        if not row:
            continue
        rows += 1
        reference = row[position["mes_ano"]]
        if not COUNT.fullmatch(row[position["volume_total"]]):
            anomalies["uncountable_volumes"] += 1
            continue
        if frequency == "mensal" and reference.count("/") == 2 and reference[:3] != "01/":
            anomalies["references_outside_day_one"] += 1
        blocks[row[position["concessionaria"]], month_of(reference)][tuple(row)] += 1
    totals: dict[Key, int] = collections.defaultdict(int)
    duplicated = []
    for (concession, month), occurrences in blocks.items():
        copies = 2 if all(count % 2 == 0 for count in occurrences.values()) else 1
        if copies == 2:
            duplicated.append([concession, month.strftime("%Y-%m")])
        for values, count in occurrences.items():
            reference = values[position["mes_ano"]]
            key = (
                month_of(reference) if frequency == "mensal" else day_of(reference),
                values[position["concessionaria"]],
                values[position["praca"]],
                values[position["sentido"]],
                values[category] if category is not None else "",
                values[position["tipo_cobranca"]],
                values[position["tipo_de_veiculo"]],
            )
            volume = int(Decimal(values[position["volume_total"]].replace(",", ".")))
            totals[key] += volume * count // copies
    return {
        "rows": rows,
        "totals": totals,
        "duplicated_blocks": duplicated,
        "anomalies": anomalies,
        "problems": problems,
    }


def compare_traffic(frame: pd.DataFrame, meta: Any, expected: dict[str, Any]) -> dict[str, Any]:
    observed: dict[Key, int] = {}
    axles_problems = 0
    row: Any
    for row in frame.itertuples(index=False):
        key = (
            row.data.date(),
            row.concessionaria,
            row.praca,
            row.sentido,
            "" if pd.isna(row.categoria_eixo) else row.categoria_eixo,
            row.tipo_cobranca,
            row.tipo_veiculo,
        )
        observed[key] = int(row.volume)
        match = AXLES.fullmatch(key[4].strip()) if key[4] else None
        axles = int(match[1]) if match else None
        if (None if pd.isna(row.n_eixos) else int(row.n_eixos)) != axles:
            axles_problems += 1
    totals = expected["totals"]
    only_agrobr = [key for key in observed if key not in totals]
    only_source = [key for key in totals if key not in observed]
    different = [key for key in observed if key in totals and observed[key] != totals[key]]
    parsing = meta.source_details["parsing"][0]
    problems = list(expected["problems"])
    for name, value in (
        ("chaves só no agrobr", len(only_agrobr)),
        ("chaves só na fonte", len(only_source)),
        ("volumes divergentes", len(different)),
        ("n_eixos divergente da contagem textual", axles_problems),
    ):
        if value:
            problems.append(f"{name}: {value}")
    counters = {
        "normalized_references": expected["anomalies"]["references_outside_day_one"],
        "excluded_volumes": expected["anomalies"]["uncountable_volumes"],
    }
    for name, value in counters.items():
        if parsing[name] != value:
            problems.append(f"{name}: agrobr {parsing[name]} × fonte {value}")
    blocks = [[item["concessionaria"], item["mes"]] for item in parsing["duplicated_blocks"]]
    if sorted(blocks) != sorted(expected["duplicated_blocks"]):
        problems.append(
            f"blocos duplicados: agrobr {blocks} × fonte {expected['duplicated_blocks']}"
        )
    return {
        "status": "ok" if not problems else "mismatch",
        "problems": problems,
        "source_rows": expected["rows"],
        "keys": len(totals),
        "volume": sum(totals.values()),
        "agrobr_rows": len(frame),
        "agrobr_volume": int(frame["volume"].sum()),
        "anomalies": {**expected["anomalies"], "duplicated_blocks": expected["duplicated_blocks"]},
        "examples": {
            "only_agrobr": [list(map(str, key)) for key in only_agrobr[:5]],
            "only_source": [list(map(str, key)) for key in only_source[:5]],
            "different": [
                [list(map(str, key)), observed[key], totals[key]] for key in different[:5]
            ],
        },
        "resource_sha256": meta.source_details["acquisitions"][-1]["acquisition"]["files"][0][
            "sha256"
        ],
    }


def plaza_rows(body: bytes) -> list[dict[str, Any]]:
    text, _ = decode(body)
    rows = []
    aliases = {"latitude": "lat", "longitude": "lon", "praca": "praca_de_pedagio"}
    for raw in csv.DictReader(io.StringIO(text, newline=""), delimiter=";"):
        row: dict[str, Any] = {aliases.get(name, name): value for name, value in raw.items()}
        row["uf"] = row["uf"].strip().upper() or None
        for name in ("lat", "lon"):
            row[name] = float(Decimal(row[name].replace(",", "."))) if row[name] else None
        row.setdefault("municipio", row.get("municipal"))
        row["km_m"] = float(Decimal(row["km_m"])) if row["km_m"] else None
        row["ano_do_pnv_snv"] = int(row["ano_do_pnv_snv"]) if row["ano_do_pnv_snv"] else None
        row["data_da_inativacao"] = (
            datetime.strptime(row["data_da_inativacao"], "%d/%m/%Y")
            if row["data_da_inativacao"]
            else None
        )
        rows.append(row)
    return rows


def compare_plazas(frame: pd.DataFrame, expected: list[dict[str, Any]]) -> dict[str, Any]:
    problems = []
    if len(frame) != len(expected):
        problems.append(f"linhas: agrobr {len(frame)} × fonte {len(expected)}")
    for index, (observed, source) in enumerate(
        zip(frame.to_dict("records"), expected, strict=False)
    ):
        for name, value in source.items():
            actual = observed.get(name)
            actual = (
                None
                if actual is None or (not isinstance(actual, str) and pd.isna(actual))
                else actual
            )
            if actual != value:
                problems.append(f"linha {index + 2}, {name}: agrobr {actual!r} × fonte {value!r}")
    return {
        "status": "ok" if not problems else "mismatch",
        "problems": problems[:20],
        "rows": len(expected),
    }


def compare_enrichment(
    frame: pd.DataFrame, plazas: list[dict[str, Any]], state: str | None = None
) -> dict[str, Any]:
    groups: dict[tuple[str, str], set[tuple[Any, ...]]] = collections.defaultdict(set)
    for row in plazas:
        groups[row["concessionaria"], row["praca_de_pedagio"]].add(
            (row["rodovia"], row["uf"], row["municipio"])
        )
    unique = {key: next(iter(values)) for key, values in groups.items() if len(values) == 1}
    problems = 0
    record: Any
    for record in frame.itertuples(index=False):
        expected = unique.get((record.concessionaria, record.praca), (None, None, None))
        actual = tuple(
            None if pd.isna(value) else value
            for value in (record.rodovia, record.uf, record.municipio)
        )
        if actual != expected:
            problems += 1
    outside = 0 if state is None else int(frame["uf"].ne(state).sum())
    return {
        "status": "ok" if not problems and not outside else "mismatch",
        "problems": [f"enriquecimento divergente em {problems} linhas"] * bool(problems)
        + [f"{outside} linhas fora de {state}"] * bool(outside),
        "rows": len(frame),
        "linked_rows": int(frame["uf"].notna().sum()),
    }


async def agrobr_traffic(name: str) -> tuple[pd.DataFrame, Any, list[str]]:
    from agrobr.alt import antt_pedagio

    kind, year = name.removesuffix(".csv").split("_")
    budget: dict[str, Any] = (
        {"max_linhas": 20_000_000, "max_memoria_bytes": 64 * 1024**3} if kind == "diario" else {}
    )
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        frame, meta = cast(
            "tuple[pd.DataFrame, Any]",
            await antt_pedagio.fluxo_pedagio(
                ano=int(year),
                frequencia="diaria" if kind == "diario" else "mensal",
                enriquecer=False,
                return_meta=True,
                **budget,
            ),
        )
    return frame, meta, [str(item.message) for item in caught]


async def agrobr_registry(year: int) -> dict[str, Any]:
    from agrobr.alt import antt_pedagio

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        outputs: dict[str, Any] = {
            "pracas": (await antt_pedagio.pracas_pedagio(return_meta=True))[0],
            "enriquecido": (await antt_pedagio.fluxo_pedagio(ano=year, return_meta=True))[0],
            "rs": (await antt_pedagio.fluxo_pedagio(ano=year, uf="RS", return_meta=True))[0],
        }
    outputs["warnings"] = [str(item.message) for item in caught]
    return outputs


def run(directory: Path, output: Path, years: list[int], daily: bool) -> int:
    directory.mkdir(parents=True, exist_ok=True)
    receipts: dict[str, Any] = {}
    checks: dict[str, Any] = {}
    messages: list[str] = []
    with client() as http:
        packages = catalogs(http, directory, receipts)
        for name, url in sorted(resources(packages[TRAFFIC], years, daily).items()):
            frame, meta, caught = asyncio.run(agrobr_traffic(name))
            messages.extend(caught)
            receipts[name] = fetch(http, url, directory / name)
            frequency = "diaria" if name.startswith("diario") else "mensal"
            checks[name] = compare_traffic(
                frame, meta, oracle((directory / name).read_bytes(), frequency)
            )
            if checks[name]["resource_sha256"] != receipts[name]["sha256"]:
                checks[name]["problems"].append("revisão do recurso mudou entre agrobr e captura")
                checks[name]["status"] = "mismatch"
            del frame, meta
            print(name, checks[name]["status"], flush=True)
    registry = asyncio.run(agrobr_registry(max(years)))
    messages.extend(registry["warnings"])
    plazas = plaza_rows((directory / "pracas.csv").read_bytes())
    checks["pracas"] = compare_plazas(registry["pracas"], plazas)
    checks["enriquecimento"] = compare_enrichment(registry["enriquecido"], plazas)
    checks["filtro_rs"] = compare_enrichment(registry["rs"], plazas, "RS")
    result = {
        "checked_at": datetime.now(UTC).isoformat(),
        "scope": "captura ao vivo independente (httpx direto, csv da biblioteca padrão, cp1252 declarado) × saída pública do agrobr",
        "receipts": receipts,
        "agrobr_warnings": messages,
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
        description="Confere ao vivo o fluxo e o cadastro da ANTT contra leitura independente dos CSVs"
    )
    parser.add_argument("--capture-dir", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument(
        "--anos", nargs="*", type=int, default=list(range(2010, date.today().year + 1))
    )
    parser.add_argument("--diario", action="store_true")
    arguments = parser.parse_args()
    return run(arguments.capture_dir, arguments.output, arguments.anos, arguments.diario)


if __name__ == "__main__":
    raise SystemExit(main())
